
package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Record represents raw data received from a CSV, JSON, or API source.
type Record map[string]string

// TransformedRecord represents a normalized record.
type TransformedRecord struct {
	Date   string `json:"date"`
	Cases  int    `json:"cases"`
	Deaths int    `json:"deaths"`
}

// TransformRecord validates and transforms one raw record.
func TransformRecord(record Record) (TransformedRecord, error) {
	dateValue := strings.TrimSpace(record["date"])
	casesValue := strings.TrimSpace(record["cases"])
	deathsValue := strings.TrimSpace(record["deaths"])

	if dateValue == "" {
		return TransformedRecord{}, fmt.Errorf("date is required")
	}
	if casesValue == "" {
		return TransformedRecord{}, fmt.Errorf("cases is required")
	}
	if deathsValue == "" {
		return TransformedRecord{}, fmt.Errorf("deaths is required")
	}

	// Parse and normalize the date.
	parsedDate, err := time.Parse("2006-01-02", dateValue)
	if err != nil {
		return TransformedRecord{}, fmt.Errorf(
			"invalid date %q: expected YYYY-MM-DD: %w",
			dateValue,
			err,
		)
	}

	// Convert cases to an integer.
	cases, err := strconv.Atoi(casesValue)
	if err != nil {
		return TransformedRecord{}, fmt.Errorf(
			"invalid cases %q: %w",
			casesValue,
			err,
		)
	}

	// Convert deaths to an integer.
	deaths, err := strconv.Atoi(deathsValue)
	if err != nil {
		return TransformedRecord{}, fmt.Errorf(
			"invalid deaths %q: %w",
			deathsValue,
			err,
		)
	}

	if cases < 0 || deaths < 0 {
		return TransformedRecord{}, fmt.Errorf(
			"cases and deaths must be non-negative",
		)
	}

	return TransformedRecord{
		Date:   parsedDate.Format("2006-01-02"),
		Cases:  cases,
		Deaths: deaths,
	}, nil
}
