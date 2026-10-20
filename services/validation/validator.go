package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Record represents one parsed input record.
type Record map[string]string

// ValidateRecord checks required fields, data types, and business rules.
// It returns all validation errors found in the record.
func ValidateRecord(record Record) []error {
	var validationErrors []error

	dateValue := strings.TrimSpace(record["date"])
	if dateValue == "" {
		validationErrors = append(validationErrors,
			fmt.Errorf("date is required"))
	} else if _, err := time.Parse("2006-01-02", dateValue); err != nil {
		validationErrors = append(validationErrors,
			fmt.Errorf("date must use YYYY-MM-DD format"))
	}

	casesValue := strings.TrimSpace(record["cases"])
	if casesValue == "" {
		validationErrors = append(validationErrors,
			fmt.Errorf("cases is required"))
	} else if cases, err := strconv.Atoi(casesValue); err != nil {
		validationErrors = append(validationErrors,
			fmt.Errorf("cases must be an integer"))
	} else if cases < 0 {
		validationErrors = append(validationErrors,
			fmt.Errorf("cases cannot be negative"))
	}

	deathsValue := strings.TrimSpace(record["deaths"])
	if deathsValue == "" {
		validationErrors = append(validationErrors,
			fmt.Errorf("deaths is required"))
	} else if deaths, err := strconv.Atoi(deathsValue); err != nil {
		validationErrors = append(validationErrors,
			fmt.Errorf("deaths must be an integer"))
	} else if deaths < 0 {
		validationErrors = append(validationErrors,
			fmt.Errorf("deaths cannot be negative"))
	}

	return validationErrors
}

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