package ingestion

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go-assignment/packages/models"
)

// maxConcurrentFetches caps how many API sources are fetched at once.
const maxConcurrentFetches = 3

// Stream delivers a job's input records one at a time. Files are parsed
// incrementally from disk, so memory use does not grow with file size.
// API responses are already capped at maxAPIResponseSize, so they are
// fetched up front (concurrently) and replayed in source order.
type Stream struct {
	parts []streamPart
}

type streamPart struct {
	// sourceName is added to every record as "source" for mixed jobs.
	sourceName string
	// filePath is streamed from disk when set.
	filePath string
	// records holds the parsed records of an API source.
	records []RawRecord
}

// OpenFile returns a stream over a single uploaded CSV or JSON file.
func OpenFile(filePath string) *Stream {
	return &Stream{parts: []streamPart{{filePath: filePath}}}
}

// OpenSources validates the configured sources, resolves file paths
// inside the uploads directory and fetches API sources. Records keep the
// configured source order and are tagged with the source name.
func OpenSources(
	ctx context.Context,
	sources []models.SourceDefinition,
	client *http.Client,
) (*Stream, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no sources configured")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	parts := make([]streamPart, len(sources))
	var apiIndexes []int

	for i, source := range sources {
		parts[i].sourceName = strings.TrimSpace(source.Name)

		switch strings.ToLower(strings.TrimSpace(source.Type)) {
		case "csv", "json":
			if strings.TrimSpace(source.FilePath) == "" {
				return nil, sourceError(source, fmt.Errorf("file_path is required"))
			}

			filePath, err := resolveSourceFile(source.FilePath)
			if err != nil {
				return nil, sourceError(source, err)
			}
			parts[i].filePath = filePath

		case "api":
			if strings.TrimSpace(source.URL) == "" {
				return nil, sourceError(source, fmt.Errorf("URL is required"))
			}
			apiIndexes = append(apiIndexes, i)

		default:
			return nil, sourceError(source, fmt.Errorf("unsupported source type %q", source.Type))
		}
	}

	if err := fetchAPISources(ctx, sources, apiIndexes, parts, client); err != nil {
		return nil, err
	}

	return &Stream{parts: parts}, nil
}

// fetchAPISources fetches the given API sources with bounded concurrency
// and stores their records in parts.
func fetchAPISources(
	ctx context.Context,
	sources []models.SourceDefinition,
	indexes []int,
	parts []streamPart,
	client *http.Client,
) error {
	if len(indexes) == 0 {
		return nil
	}

	errs := make([]error, len(sources))
	limit := make(chan struct{}, maxConcurrentFetches)
	var wg sync.WaitGroup

	for _, index := range indexes {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-ctx.Done():
				errs[index] = ctx.Err()
				return
			}

			parts[index].records, errs[index] = fetchAPISource(ctx, sources[index], client)
		}(index)
	}

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}

	// Report the first failure in configured order.
	for _, index := range indexes {
		if errs[index] != nil {
			return sourceError(sources[index], errs[index])
		}
	}

	return nil
}

// fetchAPISource downloads one API source and parses it with the
// provider-specific adapter named in source.Options["provider"].
func fetchAPISource(
	ctx context.Context,
	source models.SourceDefinition,
	client *http.Client,
) ([]RawRecord, error) {
	body, err := readAPIResponse(ctx, client, source.URL)
	if err != nil {
		return nil, err
	}

	provider := ""
	if source.Options != nil {
		provider = strings.ToLower(strings.TrimSpace(source.Options["provider"]))
	}

	switch provider {
	case "open-meteo":
		return ParseOpenMeteo(body)
	case "coingecko":
		return ParseCoinGecko(body)
	default:
		return parseAPIRecords(body)
	}
}

// Count returns the number of records without keeping them in memory,
// so progress can be reported as a percentage of the total.
func (s *Stream) Count(ctx context.Context) (int, error) {
	count := 0

	err := s.Each(ctx, func(RawRecord) error {
		count++
		return nil
	})

	return count, err
}

// Each calls emit for every record in source order. It stops at the
// first error, including context cancellation.
func (s *Stream) Each(ctx context.Context, emit EmitFunc) error {
	for _, part := range s.parts {
		tagged := func(record RawRecord) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if record == nil {
				record = make(RawRecord)
			}
			if part.sourceName != "" {
				record["source"] = part.sourceName
			}
			return emit(record)
		}

		var err error
		if part.filePath != "" {
			err = StreamFile(part.filePath, tagged)
		} else {
			for _, record := range part.records {
				if err = tagged(copyRecord(record)); err != nil {
					break
				}
			}
		}

		if err != nil {
			if part.sourceName != "" && ctx.Err() == nil {
				return fmt.Errorf("read source %q: %w", part.sourceName, err)
			}
			return err
		}
	}

	return nil
}

// copyRecord prevents callers from mutating the stored API records,
// since Each may be called more than once (Count, then processing).
func copyRecord(record RawRecord) RawRecord {
	copied := make(RawRecord, len(record)+1)
	for key, value := range record {
		copied[key] = value
	}
	return copied
}

func sourceError(source models.SourceDefinition, err error) error {
	return fmt.Errorf("read source %q: %w", source.Name, err)
}
