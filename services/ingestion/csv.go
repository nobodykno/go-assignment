package ingestion

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"go-assignment/packages/models"
)

// RawRecord is the shared input record type; the alias keeps the
// ingestion API unchanged.
type RawRecord = models.RawRecord

// EmitFunc receives parsed records one at a time. Returning an error
// stops parsing and is returned to the caller unchanged.
type EmitFunc func(RawRecord) error

// ReadCSV reads every record of a CSV file into memory. Prefer
// StreamFile for large inputs.
func ReadCSV(filePath string) ([]RawRecord, error) {
	return collect(func(emit EmitFunc) error {
		return streamFile(filePath, "CSV", StreamCSV, emit)
	})
}

// StreamCSV parses CSV row by row, so memory use does not grow with the
// file size. The first row is the header; every data row must have the
// same number of columns.
func StreamCSV(r io.Reader, emit EmitFunc) error {
	reader := csv.NewReader(r)

	// Column counts are checked below to produce a clearer error.
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("CSV file is empty")
		}
		return fmt.Errorf("read CSV headers: %w", err)
	}

	// ReuseRecord shares the slice between reads; keep a copy.
	headers = append([]string(nil), headers...)

	// Normalize and validate the headers.
	seenHeaders := make(map[string]bool, len(headers))

	for i, header := range headers {
		header = strings.TrimSpace(header)

		if header == "" {
			return fmt.Errorf("CSV header at column %d is empty", i+1)
		}

		if seenHeaders[header] {
			return fmt.Errorf("duplicate CSV header %q", header)
		}

		seenHeaders[header] = true
		headers[i] = header
	}

	rowNumber := 1 // The header is row 1.

	for {
		row, err := reader.Read()

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read CSV row %d: %w", rowNumber+1, err)
		}

		rowNumber++

		if len(row) != len(headers) {
			return fmt.Errorf(
				"CSV row %d has %d columns; expected %d",
				rowNumber,
				len(row),
				len(headers),
			)
		}

		record := make(RawRecord, len(headers))

		for i, header := range headers {
			record[header] = strings.TrimSpace(row[i])
		}

		if err := emit(record); err != nil {
			return err
		}
	}
}
