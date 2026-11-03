package ingestion

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ReadFile reads every record of a CSV or JSON file into memory,
// choosing the parser from the file extension.
func ReadFile(filePath string) ([]RawRecord, error) {
	return collect(func(emit EmitFunc) error {
		return StreamFile(filePath, emit)
	})
}

// StreamFile parses a CSV or JSON file record by record, choosing the
// parser from the file extension.
func StreamFile(filePath string, emit EmitFunc) error {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".csv":
		return streamFile(filePath, "CSV", StreamCSV, emit)
	case ".json":
		return streamFile(filePath, "JSON", StreamJSON, emit)
	default:
		return fmt.Errorf(
			"unsupported file extension: %s",
			filepath.Ext(filePath),
		)
	}
}

func streamFile(
	filePath string,
	kind string,
	parse func(io.Reader, EmitFunc) error,
	emit EmitFunc,
) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open %s file: %w", kind, err)
	}
	defer file.Close()

	return parse(file, emit)
}

// collect gathers streamed records into a slice.
func collect(stream func(EmitFunc) error) ([]RawRecord, error) {
	var records []RawRecord

	err := stream(func(record RawRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}
