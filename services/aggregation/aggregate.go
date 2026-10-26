
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// TransformedRecord matches the generic transformation output.
type TransformedRecord struct {
	Source     string             `json:"source"`
	Date       string             `json:"date"`
	Dimensions map[string]string  `json:"dimensions"`
	Metrics    map[string]float64 `json:"metrics"`
}

// AggregatedRecord contains totals for records sharing a date, source,
// and dimensions. Keeping Source in the grouping key prevents unrelated
// datasets from being combined.
type AggregatedRecord struct {
	Source      string             `json:"source"`
	Date        string             `json:"date"`
	Dimensions  map[string]string  `json:"dimensions"`
	Metrics     map[string]float64 `json:"metrics"`
	RecordCount int                `json:"record_count"`
}

// aggregationKey uniquely identifies an aggregation group.
type aggregationKey struct {
	Source     string            `json:"source"`
	Date       string            `json:"date"`
	Dimensions map[string]string `json:"dimensions"`
}

// AggregateRecords groups records by date, source, and dimensions,
// then sums metrics within each group.
func AggregateRecords(
	records []TransformedRecord,
) ([]AggregatedRecord, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no transformed records to aggregate")
	}

	totals := make(map[string]*AggregatedRecord)

	for index, record := range records {
		date := strings.TrimSpace(record.Date)
		source := strings.TrimSpace(record.Source)

		if date == "" {
			return nil, fmt.Errorf(
				"record %d has an empty date",
				index+1,
			)
		}

		if source == "" {
			source = "unknown"
		}

		if record.Dimensions == nil {
			record.Dimensions = make(map[string]string)
		}

		if record.Metrics == nil {
			record.Metrics = make(map[string]float64)
		}

		// JSON encoding provides a stable grouping key because
		// encoding/json sorts string map keys.
		keyBytes, err := json.Marshal(aggregationKey{
			Source:     source,
			Date:       date,
			Dimensions: record.Dimensions,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"encode grouping key for record %d: %w",
				index+1,
				err,
			)
		}

		key := string(keyBytes)

		aggregate, exists := totals[key]
		if !exists {
			dimensions := make(
				map[string]string,
				len(record.Dimensions),
			)

			for name, value := range record.Dimensions {
				dimensions[name] = value
			}

			aggregate = &AggregatedRecord{
				Source:     source,
				Date:       date,
				Dimensions: dimensions,
				Metrics:    make(map[string]float64),
			}

			totals[key] = aggregate
		}

		for metricName, value := range record.Metrics {
			metricName = strings.TrimSpace(metricName)

			if metricName == "" {
				return nil, fmt.Errorf(
					"record %d contains an empty metric name",
					index+1,
				)
			}

			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf(
					"record %d has a non-finite metric %q",
					index+1,
					metricName,
				)
			}

			sum := aggregate.Metrics[metricName] + value

			if math.IsNaN(sum) || math.IsInf(sum, 0) {
				return nil, fmt.Errorf(
					"metric %q overflow while aggregating record %d",
					metricName,
					index+1,
				)
			}

			aggregate.Metrics[metricName] = sum
		}

		aggregate.RecordCount++
	}

	result := make([]AggregatedRecord, 0, len(totals))

	for _, aggregate := range totals {
		result = append(result, *aggregate)
	}

	// Sort by date, source, and a stable representation of dimensions.
	sort.Slice(result, func(i, j int) bool {
		if result[i].Date != result[j].Date {
			return result[i].Date < result[j].Date
		}

		if result[i].Source != result[j].Source {
			return result[i].Source < result[j].Source
		}

		left, _ := json.Marshal(result[i].Dimensions)
		right, _ := json.Marshal(result[j].Dimensions)

		return string(left) < string(right)
	})

	return result, nil
}

// AggregateFile reads transformed JSON, aggregates the records,
// and writes the result to a separate JSON file.
func AggregateFile(
	inputPath string,
	outputPath string,
) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open transformed file: %w", err)
	}
	defer inputFile.Close()

	var records []TransformedRecord

	decoder := json.NewDecoder(inputFile)

	if err := decoder.Decode(&records); err != nil {
		return fmt.Errorf("decode transformed records: %w", err)
	}

	// Ensure the file contains exactly one JSON value.
	var extra interface{}

	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf(
				"decode transformed records: unexpected trailing JSON value",
			)
		}

		return fmt.Errorf(
			"decode transformed records: unexpected trailing content: %w",
			err,
		)
	}

	aggregatedRecords, err := AggregateRecords(records)
	if err != nil {
		return fmt.Errorf("aggregate records: %w", err)
	}

	outputDir := filepath.Dir(outputPath)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf(
			"create aggregation output directory: %w",
			err,
		)
	}

	// Write to a temporary file to avoid exposing incomplete JSON.
	tempFile, err := os.CreateTemp(
		outputDir,
		"aggregation-*.tmp",
	)
	if err != nil {
		return fmt.Errorf(
			"create temporary aggregation file: %w",
			err,
		)
	}

	tempPath := tempFile.Name()

	defer func() {
		_ = os.Remove(tempPath)
	}()

	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(aggregatedRecords); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("encode aggregated records: %w", err)
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("sync aggregation output: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close aggregation output: %w", err)
	}

	if err := os.Rename(tempPath, outputPath); err != nil {
		return fmt.Errorf("finalize aggregation output: %w", err)
	}

	return nil
}
