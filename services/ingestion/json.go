package ingestion

import (
	"encoding/json"
	"fmt"
	"os"
)

// ReadJSON reads a JSON array and returns its records.
func ReadJSON(filePath string) ([]RawRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open JSON file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	var items []map[string]json.RawMessage

	if err := decoder.Decode(&items); err != nil {
		return nil, fmt.Errorf("decode JSON records: %w", err)
	}

	// Reject additional JSON values after the first array.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err == nil {
		return nil, fmt.Errorf("JSON file contains multiple top-level values")
	}

	records := make([]RawRecord, 0, len(items))

	for i, item := range items {
		if len(item) == 0 {
			return nil, fmt.Errorf("JSON record %d is empty", i+1)
		}

		record := make(RawRecord, len(item))

		for key, value := range item {
			var stringValue string

			if err := json.Unmarshal(value, &stringValue); err == nil {
				record[key] = stringValue
			} else {
				// Preserve numbers, booleans, null, and nested JSON as text.
				record[key] = string(value)
			}
		}

		records = append(records, record)
	}

	return records, nil
}
