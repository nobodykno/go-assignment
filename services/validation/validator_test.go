package main

import (
	"strings"
	"testing"
)

func TestValidateRecordValidRecord(t *testing.T) {
	record := Record{
		"source":    "covid",
		"date":      "2026-10-27",
		"cases":     "100",
		"deaths":    "5",
		"country":   "India",
		"latitude":  "28.61",
		"longitude": "77.20",
	}

	errors := ValidateRecord(record)

	if len(errors) != 0 {
		t.Fatalf("expected no validation errors, got: %v", errors)
	}
}

func TestValidateRecordRejectsEmptyRecord(t *testing.T) {
	errors := ValidateRecord(Record{})

	if len(errors) != 1 {
		t.Fatalf("expected 1 error for an empty record, got %d", len(errors))
	}

	if !strings.Contains(errors[0].Error(), "record cannot be empty") {
		t.Errorf("unexpected error: %v", errors[0])
	}
}

func TestValidateRecordRequiresDate(t *testing.T) {
	tests := []struct {
		name   string
		record Record
	}{
		{
			name: "missing date",
			record: Record{
				"source": "covid",
				"cases":  "100",
			},
		},
		{
			name: "empty date",
			record: Record{
				"source": "covid",
				"date":   " ",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateRecord(tt.record)

			if len(errors) == 0 {
				t.Fatal("expected a date validation error")
			}

			found := false
			for _, err := range errors {
				if strings.Contains(strings.ToLower(err.Error()), "date is required") {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("expected a required-date error, got: %v", errors)
			}
		})
	}
}

func TestValidateRecordRejectsInvalidDate(t *testing.T) {
	record := Record{
		"date":   "not-a-date",
		"source": "covid",
	}

	errors := ValidateRecord(record)

	if len(errors) == 0 {
		t.Fatal("expected an invalid-date error")
	}

	found := false
	for _, err := range errors {
		if strings.Contains(strings.ToLower(err.Error()), "valid date") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("expected an invalid-date error, got: %v", errors)
	}
}

func TestValidateRecordAcceptsSupportedDateFormats(t *testing.T) {
	dates := []string{
		"2026-10-27",
		"2026-10-27T10:30:00Z",
		"2026-10-27 10:30:00",
		"2026/10/27",
		"10/27/2026",
	}

	for _, date := range dates {
		t.Run(date, func(t *testing.T) {
			errors := ValidateRecord(Record{
				"date": date,
			})

			if len(errors) != 0 {
				t.Errorf("expected date %q to be valid, got: %v", date, errors)
			}
		})
	}
}

func TestValidateRecordRejectsNonFiniteNumbers(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "NaN", value: "NaN"},
		{name: "positive infinity", value: "+Inf"},
		{name: "negative infinity", value: "-Inf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateRecord(Record{
				"date":  "2026-10-27",
				"cases": tt.value,
			})

			if len(errors) == 0 {
				t.Fatalf("expected an error for %q", tt.value)
			}

			found := false
			for _, err := range errors {
				if strings.Contains(strings.ToLower(err.Error()), "nan or infinity") {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("expected a non-finite-number error, got: %v", errors)
			}
		})
	}
}

func TestValidateRecordAllowsNonNumericDimensions(t *testing.T) {
	record := Record{
		"date":     "2026-10-27",
		"country":  "India",
		"coin":     "bitcoin",
		"symbol":   "btc",
		"location": "New Delhi",
	}

	errors := ValidateRecord(record)

	if len(errors) != 0 {
		t.Errorf("expected text dimensions to be valid, got: %v", errors)
	}
}

func TestValidateRecordWithNumber(t *testing.T) {
	record := Record{
		"date":  "2026-10-27",
		"cases": "100",
	}

	result := ValidateRecordWithNumber(7, record)

	if result.RecordNumber != 7 {
		t.Errorf("RecordNumber = %d, want 7", result.RecordNumber)
	}

	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got: %v", result.Errors)
	}

	if result.Record["cases"] != "100" {
		t.Errorf("record was not preserved: %v", result.Record)
	}
}

func TestValidateRecordDetectsDuplicateNormalizedKeys(t *testing.T) {
	record := Record{
		"date":  "2026-10-27",
		"DATE":  "2026-10-28",
		"cases": "100",
	}

	errors := ValidateRecord(record)

	found := false
	for _, err := range errors {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate field name") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("expected duplicate-field error, got: %v", errors)
	}
}
