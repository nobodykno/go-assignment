
package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Record represents the raw data received from a CSV file.
type Record map[string]string

// TransformedRecord represents the normalized record.
type TransformedRecord struct {
	Date   string `json:"date"`
	Cases  int    `json:"cases"`
	Deaths int    `json:"deaths"`
}

// TransformRecord converts one raw record into structured data.
func TransformRecord(record Record) (TransformedRecord, error) {
	dateValue := strings.TrimSpace(record["date"])
	casesValue := strings.TrimSpace(record["cases"])
	deathsValue := strings.TrimSpace(record["deaths"])

	// Normalize and verify the date.
	parsedDate, err := time.Parse("2006-01-02", dateValue)
	if err != nil {
		return TransformedRecord{}, fmt.Errorf("invalid date: %w", err)
	}

	// Convert cases from string to integer.
	cases, err := strconv.Atoi(casesValue)
	if err != nil {
		return TransformedRecord{}, fmt.Errorf("invalid cases: %w", err)
	}

	// Convert deaths from string to integer.
	deaths, err := strconv.Atoi(deathsValue)
	if err != nil {
		return TransformedRecord{}, fmt.Errorf("invalid deaths: %w", err)
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
