package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type Record map[string]string

func ValidateRecord(record Record) []error {
	var validationErrors []error

	if len(record) == 0 {
		return []error{
			fmt.Errorf("record cannot be empty"),
		}
	}

	seenKeys := make(map[string]struct{}, len(record))

	for rawKey, rawValue := range record {
		key := strings.TrimSpace(rawKey)
		value := strings.TrimSpace(rawValue)

		if key == "" {
			validationErrors = append(
				validationErrors,
				fmt.Errorf("field name cannot be empty"),
			)
			continue
		}

		normalizedKey := strings.ToLower(key)

		if _, exists := seenKeys[normalizedKey]; exists {
			validationErrors = append(
				validationErrors,
				fmt.Errorf("duplicate field name: %s", key),
			)
			continue
		}
		seenKeys[normalizedKey] = struct{}{}

		if normalizedKey == "date" {
			if value == "" {
				validationErrors = append(
					validationErrors,
					fmt.Errorf("date is required"),
				)
			} else if !isValidDate(value) {
				validationErrors = append(
					validationErrors,
					fmt.Errorf(
						"date must be a valid date (YYYY-MM-DD, RFC3339, or supported date format)",
					),
				)
			}

			continue
		}

		// Empty values are allowed for generic dimension fields.
		if value == "" {
			continue
		}

		// Numeric fields are allowed to contain any finite number.
		// Non-numeric values are treated as dimension values.
		number, err := strconv.ParseFloat(value, 64)
		if err == nil && (math.IsNaN(number) || math.IsInf(number, 0)) {
			validationErrors = append(
				validationErrors,
				fmt.Errorf("field %q must not contain NaN or infinity", key),
			)
		}
	}

	if _, exists := record["date"]; !exists {
		// Also handle a date key with different casing or surrounding spaces.
		foundDate := false

		for key := range record {
			if strings.EqualFold(strings.TrimSpace(key), "date") {
				foundDate = true
				break
			}
		}

		if !foundDate {
			validationErrors = append(
				validationErrors,
				fmt.Errorf("date is required"),
			)
		}
	}

	return validationErrors
}

// isValidDate accepts the date formats supported by the transformation stage.
func isValidDate(value string) bool {
	formats := []string{
		"2006-01-02",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006/01/02",
		"01/02/2006",
	}

	for _, format := range formats {
		if _, err := time.Parse(format, value); err == nil {
			return true
		}
	}

	return false
}

// ValidateRecordWithNumber validates a record and preserves its record number.
// This response shape remains compatible with the existing validation worker.
func ValidateRecordWithNumber(
	recordNumber int,
	record Record,
) ValidationResult {
	validationErrors := ValidateRecord(record)

	errorMessages := make([]string, 0, len(validationErrors))

	for _, err := range validationErrors {
		errorMessages = append(errorMessages, err.Error())
	}

	return ValidationResult{
		RecordNumber: recordNumber,
		Record:       record,
		Errors:       errorMessages,
	}
}
