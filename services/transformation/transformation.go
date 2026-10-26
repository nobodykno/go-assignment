
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

// TransformedRecord represents a normalized, generic record.
type TransformedRecord struct {
	Source     string             `json:"source"`
	Date       string             `json:"date"`
	Dimensions map[string]string  `json:"dimensions"`
	Metrics    map[string]float64 `json:"metrics"`
}

// TransformRecord converts a raw record into a normalized record.
// Numeric fields become metrics; non-numeric fields become dimensions.
func TransformRecord(record Record) (TransformedRecord, error) {
	if len(record) == 0 {
		return TransformedRecord{}, fmt.Errorf("record is empty")
	}

	result := TransformedRecord{
		Source:     strings.TrimSpace(record["source"]),
		Dimensions: make(map[string]string),
		Metrics:    make(map[string]float64),
	}

	for key, rawValue := range record {
		key = strings.TrimSpace(key)
		value := strings.TrimSpace(rawValue)

		if key == "" {
			return TransformedRecord{}, fmt.Errorf(
				"record contains an empty field name",
			)
		}

		switch strings.ToLower(key) {
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

		if value == "" {
			result.Dimensions[key] = value
			continue
		}

		number, err := strconv.ParseFloat(value, 64)
		if err == nil {
			if math.IsNaN(number) || math.IsInf(number, 0) {
				return TransformedRecord{}, fmt.Errorf(
					"field %q contains a non-finite number",
					key,
				)
			}

			result.Metrics[key] = number
		} else {
			result.Dimensions[key] = value
		}
	}

	if result.Source == "" {
		result.Source = "unknown"
	}

	return result, nil
}

// normalizeDate accepts common date formats.
// A date is optional for datasets without a date field.
func normalizeDate(value string) (string, error) {
	value = strings.TrimSpace(value)

	if value == "" {
		return "", nil
	}

	layouts := []string{
		"2006-01-02",
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
