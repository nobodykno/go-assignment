package ingestion

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

type RawRecord map[string]string

func ReadCSV(filePath string) ([]RawRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Require every data row to have the same number of columns as the header.
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("CSV file is empty")
		}
		return nil, fmt.Errorf("read CSV headers: %w", err)
	}

	// Normalize and validate the headers.
	seenHeaders := make(map[string]bool, len(headers))

	for i, header := range headers {
		header = strings.TrimSpace(header)

		if header == "" {
			return nil, fmt.Errorf("CSV header at column %d is empty", i+1)
		}

		if seenHeaders[header] {
			return nil, fmt.Errorf("duplicate CSV header %q", header)
		}

		seenHeaders[header] = true
		headers[i] = header
	}

	var records []RawRecord
	rowNumber := 1 // The header is row 1.

	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row %d: %w", rowNumber+1, err)
		}

		rowNumber++

		if len(row) != len(headers) {
			return nil, fmt.Errorf(
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

		records = append(records, record)
	}

	return records, nil
}
