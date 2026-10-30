package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Record represents raw data received from a CSV, JSON, or API source.
type Record map[string]string

// TransformedRecord represents a normalized record.
// MetricOperations defines how each numeric metric should be aggregated.
type TransformedRecord struct {
	Source           string             `json:"source"`
	Date             string             `json:"date"`
	Dimensions       map[string]string  `json:"dimensions"`
	Metrics          map[string]float64 `json:"metrics"`
	MetricOperations map[string]string  `json:"metric_operations,omitempty"`
}

// TransformRecord converts a raw record into a normalized record.
// Known location and identity fields remain dimensions even when numeric.
func TransformRecord(record Record) (TransformedRecord, error) {
	if len(record) == 0 {
		return TransformedRecord{}, fmt.Errorf("record is empty")
	}

	result := TransformedRecord{
		Source:           strings.TrimSpace(record["source"]),
		Dimensions:       make(map[string]string),
		Metrics:          make(map[string]float64),
		MetricOperations: make(map[string]string),
	}

	for key, rawValue := range record {
		key = strings.TrimSpace(key)
		value := strings.TrimSpace(rawValue)

		if key == "" {
			return TransformedRecord{}, fmt.Errorf(
				"record contains an empty field name",
			)
		}

		normalizedKey := strings.ToLower(key)

		switch normalizedKey {
		case "source":
			result.Source = value
			continue

		case "date":
			normalizedDate, err := normalizeDate(value)
			if err != nil {
				return TransformedRecord{}, fmt.Errorf(
					"invalid date: %w",
					err,
				)
			}
			result.Date = normalizedDate
			continue
		}

		if isDimensionField(normalizedKey) {
			result.Dimensions[normalizedKey] = value
			continue
		}

		if value == "" {
			result.Dimensions[normalizedKey] = value
			continue
		}

		number, err := strconv.ParseFloat(value, 64)
		if err != nil {
			result.Dimensions[normalizedKey] = value
			continue
		}

		if math.IsNaN(number) || math.IsInf(number, 0) {
			return TransformedRecord{}, fmt.Errorf(
				"field %q contains a non-finite number",
				key,
			)
		}

		result.Metrics[normalizedKey] = number
		result.MetricOperations[normalizedKey] =
			aggregationOperationForMetric(normalizedKey)
	}

	if result.Source == "" {
		result.Source = "unknown"
	}

	return result, nil
}

// isDimensionField identifies fields that describe a record
// rather than measurements that should be aggregated.
func isDimensionField(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "latitude",
		"longitude",
		"lat",
		"lon",
		"lng",
		"location",
		"location_id",
		"city",
		"country",
		"region",
		"coin",
		"symbol",
		"name":
		return true
	default:
		return false
	}
}

// aggregationOperationForMetric defines the aggregation policy
// for known metrics. Unknown numeric metrics retain sum behavior.
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

// normalizeDate accepts common date formats.
func normalizeDate(value string) (string, error) {
	value = strings.TrimSpace(value)

	if value == "" {
		return "", nil
	}

	layouts := []string{
		"2006-01-02",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006/01/02",
		"01/02/2006",
	}

	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.Format("2006-01-02"), nil
		}
	}

	return "", fmt.Errorf(
		"unsupported date format %q",
		value,
	)
}
