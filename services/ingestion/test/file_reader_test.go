package ingestion_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	ingestion "go-assignment/services/ingestion"
)

func TestReadCSVFile(t *testing.T) {
	tempDir := t.TempDir()

	csvContent := "date,cases,deaths\n2026-10-27,100,5\n2026-10-28,120,6\n"
	filePath := filepath.Join(tempDir, "covid.csv")

	if err := os.WriteFile(filePath, []byte(csvContent), 0600); err != nil {
		t.Fatalf("failed to create CSV fixture: %v", err)
	}

	records, err := ingestion.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}

	if records[0]["date"] != "2026-10-27" {
		t.Errorf("first date = %q, want 2026-10-27", records[0]["date"])
	}

	if records[0]["cases"] != "100" {
		t.Errorf("first cases = %q, want 100", records[0]["cases"])
	}

	if records[1]["deaths"] != "6" {
		t.Errorf("second deaths = %q, want 6", records[1]["deaths"])
	}
}

func TestReadJSONFile(t *testing.T) {
	tempDir := t.TempDir()

	jsonContent := `[
		{"date":"2026-10-27","cases":100,"deaths":5},
		{"date":"2026-10-28","cases":120,"deaths":6}
	]`

	filePath := filepath.Join(tempDir, "covid.json")

	if err := os.WriteFile(filePath, []byte(jsonContent), 0600); err != nil {
		t.Fatalf("failed to create JSON fixture: %v", err)
	}

	records, err := ingestion.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}

	if records[0]["date"] != "2026-10-27" {
		t.Errorf("first date = %q, want 2026-10-27", records[0]["date"])
	}

	if records[0]["cases"] != "100" {
		t.Errorf("first cases = %q, want 100", records[0]["cases"])
	}
}

func TestReadFileRejectsMissingFile(t *testing.T) {
	_, err := ingestion.ReadFile(filepath.Join(t.TempDir(), "missing.csv"))

	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestReadFileRejectsMalformedJSON(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "invalid.json")

	if err := os.WriteFile(filePath, []byte(`[{"date":`), 0600); err != nil {
		t.Fatalf("failed to create JSON fixture: %v", err)
	}

	_, err := ingestion.ReadFile(filePath)
	if err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
}

func TestReadFileRejectsEmptyFile(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "empty.csv")

	if err := os.WriteFile(filePath, []byte(""), 0600); err != nil {
		t.Fatalf("failed to create empty fixture: %v", err)
	}

	_, err := ingestion.ReadFile(filePath)
	if err == nil {
		t.Fatal("expected an error for an empty file")
	}
}

func TestReadCSVRecordsAreIndependent(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "records.csv")
	content := "date,cases\n2026-10-27,100\n2026-10-28,200\n"

	if err := os.WriteFile(filePath, []byte(content), 0600); err != nil {
		t.Fatalf("failed to create CSV fixture: %v", err)
	}

	records, err := ingestion.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}

	records[0]["cases"] = "changed"

	if reflect.DeepEqual(records[0], records[1]) {
		t.Fatal("different input rows unexpectedly share the same record data")
	}

	if records[1]["cases"] != "200" {
		t.Errorf("second record changed unexpectedly: %v", records[1])
	}
}
