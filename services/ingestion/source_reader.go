package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go-assignment/packages/models"
)

const (
	maxAPIResponseSize = 10 * 1024 * 1024
	maxAPIErrorBody    = 4096
)

// ReadSources concurrently reads and combines records from configured sources.
// At most three sources are processed simultaneously.
// API sources can use the Open-Meteo adapter by setting
// source.Options["provider"] to "open-meteo".
func ReadSources(
	ctx context.Context,
	sources []models.SourceDefinition,
	client *http.Client,
) ([]RawRecord, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no sources configured")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	type sourceResult struct {
		index   int
		records []RawRecord
		err     error
	}

	workerCount := 3
	if len(sources) < workerCount {
		workerCount = len(sources)
	}

	jobs := make(chan int, len(sources))
	results := make(chan sourceResult, len(sources))

	for i := range sources {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for index := range jobs {
				if err := ctx.Err(); err != nil {
					results <- sourceResult{
						index: index,
						err:   err,
					}
					continue
				}

				records, err := readOneSource(
					ctx,
					sources[index],
					client,
				)

				results <- sourceResult{
					index:   index,
					records: records,
					err:     err,
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Store results by source index to preserve configured source order.
	recordsBySource := make([][]RawRecord, len(sources))
	errorsBySource := make([]error, len(sources))

	for result := range results {
		recordsBySource[result.index] = result.records
		errorsBySource[result.index] = result.err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var allRecords []RawRecord

	for i, source := range sources {
		if errorsBySource[i] != nil {
			return nil, fmt.Errorf(
				"read source %q: %w",
				source.Name,
				errorsBySource[i],
			)
		}

		allRecords = append(allRecords, recordsBySource[i]...)
	}

	return allRecords, nil
}

// readOneSource reads one configured source, preserving source attribution.
func readOneSource(
	ctx context.Context,
	source models.SourceDefinition,
	client *http.Client,
) ([]RawRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var records []RawRecord
	var err error

	switch strings.ToLower(strings.TrimSpace(source.Type)) {
	case "csv", "json":
		if strings.TrimSpace(source.FilePath) == "" {
			return nil, fmt.Errorf("file_path is required")
		}

		filePath, pathErr := resolveSourceFile(source.FilePath)
		if pathErr != nil {
			return nil, pathErr
		}

		records, err = ReadFile(filePath)

	case "api":
		if strings.TrimSpace(source.URL) == "" {
			return nil, fmt.Errorf("URL is required")
		}

		body, requestErr := readAPIResponse(
			ctx,
			client,
			source.URL,
		)
		if requestErr != nil {
			return nil, requestErr
		}

		provider := ""
		if source.Options != nil {
			provider = strings.ToLower(
				strings.TrimSpace(source.Options["provider"]),
			)
		}

		switch provider {
		case "open-meteo":
			records, err = ParseOpenMeteo(body)

		case "coingecko":
			records, err = ParseCoinGecko(body)

		default:
			records, err = parseAPIRecords(body)
		}

	default:
		return nil, fmt.Errorf(
			"unsupported source type %q",
			source.Type,
		)
	}

	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for i := range records {
		if records[i] == nil {
			records[i] = make(RawRecord)
		}

		records[i]["source"] = strings.TrimSpace(source.Name)
	}

	return records, nil
}

func resolveSourceFile(filePath string) (string, error) {
	uploadsDir := strings.TrimSpace(os.Getenv("UPLOADS_DIR"))
	if uploadsDir == "" {
		uploadsDir = "uploads"
	}

	basePath, err := filepath.Abs(uploadsDir)
	if err != nil {
		return "", fmt.Errorf("resolve uploads directory: %w", err)
	}

	basePath, err = filepath.EvalSymlinks(basePath)
	if err != nil {
		return "", fmt.Errorf(
			"resolve uploads directory symlinks: %w",
			err,
		)
	}

	var candidate string

	if filepath.IsAbs(filePath) {
		candidate = filepath.Clean(filePath)
	} else {
		workingPath, absErr := filepath.Abs(filePath)
		if absErr != nil {
			return "", fmt.Errorf("resolve file path: %w", absErr)
		}

		if isWithinDirectory(basePath, workingPath) {
			candidate = workingPath
		} else {
			candidate = filepath.Join(basePath, filePath)
		}
	}

	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve file path: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve file symlinks: %w", err)
	}

	if !isWithinDirectory(basePath, resolvedPath) {
		return "", fmt.Errorf(
			"file path must be inside the uploads directory",
		)
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("inspect source file: %w", err)
	}

	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("source path must identify a regular file")
	}

	return resolvedPath, nil
}

func isWithinDirectory(basePath, candidate string) bool {
	relative, err := filepath.Rel(basePath, candidate)
	if err != nil {
		return false
	}

	return relative != ".." &&
		!strings.HasPrefix(
			relative,
			".."+string(filepath.Separator),
		) &&
		!filepath.IsAbs(relative)
}

func ReadAPI(
	ctx context.Context,
	client *http.Client,
	rawURL string,
) ([]RawRecord, error) {
	body, err := readAPIResponse(ctx, client, rawURL)
	if err != nil {
		return nil, err
	}

	return parseAPIRecords(body)
}

// readAPIResponse fetches an API response with URL, destination, redirect,
// status-code, timeout (through the client), and response-size checks.
func readAPIResponse(
	ctx context.Context,
	client *http.Client,
	rawURL string,
) ([]byte, error) {
	parsedURL, err := url.ParseRequestURI(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("invalid API URL: %w", err)
	}

	if parsedURL.Scheme != "https" ||
		parsedURL.Hostname() == "" ||
		parsedURL.User != nil {
		return nil, fmt.Errorf(
			"API URL must be a valid HTTPS URL without user credentials",
		)
	}

	if err := validatePublicHost(ctx, parsedURL.Hostname()); err != nil {
		return nil, fmt.Errorf("API destination rejected: %w", err)
	}

	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	// Do not follow redirects to another destination.
	safeClient := *client
	safeClient.CheckRedirect = func(
		req *http.Request,
		via []*http.Request,
	) error {
		return http.ErrUseLastResponse
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		parsedURL.String(),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create API request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := safeClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(
			io.Discard,
			io.LimitReader(resp.Body, maxAPIErrorBody),
		)

		return nil, fmt.Errorf("API returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(
		io.LimitReader(resp.Body, maxAPIResponseSize+1),
	)
	if err != nil {
		return nil, fmt.Errorf("read API response: %w", err)
	}

	if len(body) > maxAPIResponseSize {
		return nil, fmt.Errorf(
			"API response exceeds %d bytes",
			maxAPIResponseSize,
		)
	}

	return body, nil
}

// parseAPIRecords converts a JSON array or a "data" array into RawRecord values.
func parseAPIRecords(body []byte) ([]RawRecord, error) {
	var rawRecords []json.RawMessage

	if err := json.Unmarshal(body, &rawRecords); err != nil {
		var envelope struct {
			Data []json.RawMessage `json:"data"`
		}

		if envelopeErr := json.Unmarshal(body, &envelope); envelopeErr != nil ||
			envelope.Data == nil {
			return nil, fmt.Errorf(
				"API response must be a JSON array or an object containing a data array",
			)
		}

		rawRecords = envelope.Data
	}

	records := make([]RawRecord, 0, len(rawRecords))

	for i, raw := range rawRecords {
		var fields map[string]json.RawMessage

		if err := json.Unmarshal(raw, &fields); err != nil ||
			fields == nil {
			return nil, fmt.Errorf(
				"API record %d must be a JSON object",
				i+1,
			)
		}

		record := make(RawRecord, len(fields))

		for key, value := range fields {
			var text string

			if err := json.Unmarshal(value, &text); err != nil {
				// Preserve numbers, booleans, arrays, and objects as JSON text.
				text = string(value)
			}

			record[key] = text
		}

		records = append(records, record)
	}

	return records, nil
}

func validatePublicHost(ctx context.Context, hostname string) error {
	if ip, err := netip.ParseAddr(hostname); err == nil {
		if !isPublicIP(ip) {
			return fmt.Errorf("host resolves to a non-public IP address")
		}
		return nil
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		return fmt.Errorf("resolve API hostname: %w", err)
	}

	if len(ips) == 0 {
		return fmt.Errorf("hostname has no IP addresses")
	}

	for _, result := range ips {
		ip, ok := netip.AddrFromSlice(result.IP)
		if !ok || !isPublicIP(ip) {
			return fmt.Errorf(
				"hostname resolves to a non-public IP address",
			)
		}
	}

	return nil
}

func isPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()

	return ip.IsValid() &&
		!ip.IsPrivate() &&
		!ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() &&
		!ip.IsMulticast() &&
		!ip.IsUnspecified()
}
