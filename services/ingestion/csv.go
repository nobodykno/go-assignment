
package ingestion

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type RawRecord map[string]string

func ReadCSV(filePath string) ([]RawRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("CSV file is empty")
		}
		return nil, fmt.Errorf("read CSV headers: %w", err)
	}

	var records []RawRecord

	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row: %w", err)
		}

		record := make(RawRecord, len(headers))

		for i, header := range headers {
			record[header] = row[i]
		}

		records = append(records, record)
	}

	return records, nil
}
