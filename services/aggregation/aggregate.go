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
	Source           string             `json:"source"`
	Date             string             `json:"date"`
	Dimensions       map[string]string  `json:"dimensions"`
	Metrics          map[string]float64 `json:"metrics"`
	MetricOperations map[string]string  `json:"metric_operations,omitempty"`
}

// AggregatedRecord contains aggregated metrics for records sharing
// the same date, source, and dimensions.
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

// AggregateRecords groups records by date, source, and dimensions.
// Each metric uses its configured aggregation operation.
func AggregateRecords(
	records []TransformedRecord,
) ([]AggregatedRecord, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no transformed records to aggregate")
	}

	totals := make(map[string]*AggregatedRecord)
	metricCounts := make(map[string]map[string]int)
	operationsByGroup := make(map[string]map[string]string)

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
			metricCounts[key] = make(map[string]int)
			operationsByGroup[key] = make(map[string]string)
		}

		for metricName, value := range record.Metrics {
			metricName = strings.ToLower(
				strings.TrimSpace(metricName),
			)

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

			operation := ""
			if record.MetricOperations != nil {
				operation = strings.ToLower(
					strings.TrimSpace(
						record.MetricOperations[metricName],
					),
				)
			}

			if operation == "" {
				operation = aggregationOperationForMetric(metricName)
			}

			switch operation {
			case "sum", "avg", "min", "max":
			default:
				return nil, fmt.Errorf(
					"record %d has unsupported aggregation operation %q for metric %q",
					index+1,
					operation,
					metricName,
				)
			}

			existingOperation, alreadyConfigured :=
				operationsByGroup[key][metricName]

			if alreadyConfigured && existingOperation != operation {
				return nil, fmt.Errorf(
					"inconsistent aggregation operations for metric %q",
					metricName,
				)
			}

			operationsByGroup[key][metricName] = operation

			count := metricCounts[key][metricName]
			current := aggregate.Metrics[metricName]

			switch operation {
			case "sum", "avg":
				next := current + value

				if math.IsNaN(next) || math.IsInf(next, 0) {
					return nil, fmt.Errorf(
						"metric %q overflow while aggregating record %d",
						metricName,
						index+1,
					)
				}

				aggregate.Metrics[metricName] = next

			case "min":
				if count == 0 || value < current {
					aggregate.Metrics[metricName] = value
				}

			case "max":
				if count == 0 || value > current {
					aggregate.Metrics[metricName] = value
				}
			}

			metricCounts[key][metricName] = count + 1
		}

		aggregate.RecordCount++
	}

	result := make([]AggregatedRecord, 0, len(totals))

	for key, aggregate := range totals {
		for metricName, operation := range operationsByGroup[key] {
			if operation != "avg" {
				continue
			}

			count := metricCounts[key][metricName]
			if count == 0 {
				return nil, fmt.Errorf(
					"cannot calculate average for metric %q without values",
					metricName,
				)
			}

			aggregate.Metrics[metricName] /= float64(count)
		}

		result = append(result, *aggregate)
	}

	// Sort by date, source, and dimensions for deterministic output.
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

// aggregationOperationForMetric provides defaults for transformed
// files that do not contain explicit metric_operations.
func aggregationOperationForMetric(key string) string {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "temperature_2m_max":
		return "max"

	case "temperature_2m_min":
		return "min"

	case "price_usd", "current_price":
		return "avg"

	case "cases", "deaths", "precipitation_sum":
		return "sum"

	default:
		return "sum"
	}
}

// AggregateFile reads transformed JSON, aggregates records,
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

	// Require exactly one JSON value in the input file.
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
