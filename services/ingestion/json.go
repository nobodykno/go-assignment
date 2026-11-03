package ingestion

import (
	"encoding/json"
	"fmt"
	"io"
)

// ReadJSON reads every record of a JSON array file into memory. Prefer
// StreamFile for large inputs.
func ReadJSON(filePath string) ([]RawRecord, error) {
	return collect(func(emit EmitFunc) error {
		return streamFile(filePath, "JSON", StreamJSON, emit)
	})
}

// StreamJSON decodes a top-level JSON array one element at a time, so
// only the current object is held in memory.
func StreamJSON(r io.Reader, emit EmitFunc) error {
	decoder := json.NewDecoder(r)

	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON records: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("decode JSON records: expected a JSON array")
	}

	for number := 1; decoder.More(); number++ {
		var item map[string]json.RawMessage

		if err := decoder.Decode(&item); err != nil {
			return fmt.Errorf("decode JSON record %d: %w", number, err)
		}

		if len(item) == 0 {
			return fmt.Errorf("JSON record %d is empty", number)
		}

		if err := emit(jsonRecord(item)); err != nil {
			return err
		}
	}

	// Consume the closing bracket, then reject trailing values.
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("decode JSON records: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("JSON file contains multiple top-level values")
	}

	return nil
}

// jsonRecord converts one JSON object into a RawRecord.
func jsonRecord(item map[string]json.RawMessage) RawRecord {
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

	return record
}
