
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type TransformedRecord struct {
	Date   string `json:"date"`
	Cases  int    `json:"cases"`
	Deaths int    `json:"deaths"`
}


type AggregatedRecord struct {
	Date          string `json:"date"`
	TotalCases    int    `json:"total_cases"`
	TotalDeaths   int    `json:"total_deaths"`
	RecordCount   int    `json:"record_count"`
}


func AggregateRecords(
	records []TransformedRecord,
) ([]AggregatedRecord, error) {
	totals := make(map[string]*AggregatedRecord)

	for _, record := range records {
		if record.Date == "" {
			return nil, fmt.Errorf("record contains an empty date")
		}

		if record.Cases < 0 || record.Deaths < 0 {
			return nil, fmt.Errorf(
				"record for date %s contains negative values",
				record.Date,
			)
		}

		aggregate, exists := totals[record.Date]
		if !exists {
			aggregate = &AggregatedRecord{
				Date: record.Date,
			}
			totals[record.Date] = aggregate
		}

		aggregate.TotalCases += record.Cases
		aggregate.TotalDeaths += record.Deaths
		aggregate.RecordCount++
	}

	result := make([]AggregatedRecord, 0, len(totals))

	for _, aggregate := range totals {
		result = append(result, *aggregate)
	}

	// Map iteration order is not guaranteed, so sort the output by date.
	sort.Slice(result, func(i, j int) bool {
		return result[i].Date < result[j].Date
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

	aggregatedRecords, err := AggregateRecords(records)
	if err != nil {
		return fmt.Errorf("aggregate records: %w", err)
	}

	outputDir := filepath.Dir(outputPath)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create aggregation output directory: %w", err)
	}

	// Write to a temporary file first to avoid exposing incomplete JSON.
	tempFile, err := os.CreateTemp(
		outputDir,
		"aggregation-*.tmp",
	)
	if err != nil {
		return fmt.Errorf("create temporary aggregation file: %w", err)
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

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close aggregation output: %w", err)
	}

	if err := os.Rename(tempPath, outputPath); err != nil {
		return fmt.Errorf("finalize aggregation output: %w", err)
	}

	return nil
}
