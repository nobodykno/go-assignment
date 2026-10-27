package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExportFileSuccess(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")
	outputPath := filepath.Join(dir, "reports", "report.json")

	input := `[
		{
			"source": "covid",
			"date": "2026-10-27",
			"dimensions": {"country": "India"},
			"metrics": {"cases": 100, "deaths": 5},
			"record_count": 1
		},
		{
			"source": "coingecko",
			"date": "2026-10-27",
			"dimensions": {"coin": "bitcoin"},
			"metrics": {"price_usd": 67250.5},
			"record_count": 1
		},
		{
			"source": "covid",
			"date": "2026-10-28",
			"dimensions": {"country": "India"},
			"metrics": {"cases": 120, "deaths": 6},
			"record_count": 1
		}
	]`

	if err := os.WriteFile(inputPath, []byte(input), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	jobID := uint(42)

	dbReports, err := ExportFile(inputPath, outputPath, jobID)
	if err != nil {
		t.Fatalf("ExportFile() error = %v", err)
	}

	if len(dbReports) != 2 {
		t.Fatalf("got %d database reports, want 2 dates", len(dbReports))
	}

	for _, report := range dbReports {
		if report.JobID != jobID {
			t.Errorf("JobID = %d, want %d", report.JobID, jobID)
		}

		if report.GroupKey == "" {
			t.Error("GroupKey should not be empty")
		}

		if report.ReportData == nil {
			t.Error("ReportData should not be nil")
		}
	}

	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	var reports []GlobalDailyReport
	if err := json.Unmarshal(output, &reports); err != nil {
		t.Fatalf("decode output JSON: %v", err)
	}

	if len(reports) != 2 {
		t.Fatalf("got %d output reports, want 2", len(reports))
	}

	if reports[0].Date != "2026-10-27" {
		t.Errorf("first report date = %q, want 2026-10-27", reports[0].Date)
	}

	if reports[1].Date != "2026-10-28" {
		t.Errorf("second report date = %q, want 2026-10-28", reports[1].Date)
	}

	if len(reports[0].Sources) != 2 {
		t.Errorf("first report has %d sources, want 2", len(reports[0].Sources))
	}

	covidReport, exists := reports[0].Sources["covid"]
	if !exists {
		t.Fatal("expected covid source in first report")
	}

	if len(covidReport.Groups) != 1 {
		t.Fatalf("covid groups = %d, want 1", len(covidReport.Groups))
	}

	if got := covidReport.Groups[0].Metrics["cases"]; got != 100 {
		t.Errorf("cases = %v, want 100", got)
	}
}

func TestExportFileRejectsEmptyInput(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")
	outputPath := filepath.Join(dir, "report.json")

	if err := os.WriteFile(inputPath, []byte(`[]`), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	_, err := ExportFile(inputPath, outputPath, 1)
	if err == nil {
		t.Fatal("expected an error for an empty aggregated report")
	}

	if !strings.Contains(err.Error(), "no records") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExportFileRejectsMalformedJSON(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")

	if err := os.WriteFile(inputPath, []byte(`[{"source":`), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	_, err := ExportFile(inputPath, filepath.Join(dir, "report.json"), 1)
	if err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
}

func TestExportFileRejectsTrailingJSON(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")

	input := `[
		{
			"source": "covid",
			"date": "2026-10-27",
			"dimensions": {},
			"metrics": {"cases": 10},
			"record_count": 1
		}
	] {"unexpected": true}`

	if err := os.WriteFile(inputPath, []byte(input), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	_, err := ExportFile(inputPath, filepath.Join(dir, "report.json"), 1)
	if err == nil {
		t.Fatal("expected an error for trailing JSON")
	}

	if !strings.Contains(err.Error(), "trailing") {
		t.Errorf("expected trailing-content error, got: %v", err)
	}
}

func TestExportFileRejectsInvalidDate(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")

	input := `[{
		"source": "covid",
		"date": "not-a-date",
		"dimensions": {},
		"metrics": {"cases": 10},
		"record_count": 1
	}]`

	if err := os.WriteFile(inputPath, []byte(input), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	_, err := ExportFile(inputPath, filepath.Join(dir, "report.json"), 1)
	if err == nil {
		t.Fatal("expected an error for an invalid date")
	}
}

func TestExportFileRejectsInvalidRecordCount(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")

	input := `[{
		"source": "covid",
		"date": "2026-10-27",
		"dimensions": {},
		"metrics": {"cases": 10},
		"record_count": 0
	}]`

	if err := os.WriteFile(inputPath, []byte(input), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	_, err := ExportFile(inputPath, filepath.Join(dir, "report.json"), 1)
	if err == nil {
		t.Fatal("expected an error for record_count = 0")
	}
}

func TestExportFileDoesNotOverwriteOutputOnInvalidInput(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")
	outputPath := filepath.Join(dir, "report.json")

	original := []byte("existing report")

	if err := os.WriteFile(outputPath, original, 0600); err != nil {
		t.Fatalf("write existing output: %v", err)
	}

	if err := os.WriteFile(inputPath, []byte(`[]`), 0600); err != nil {
		t.Fatalf("write invalid input: %v", err)
	}

	_, err := ExportFile(inputPath, outputPath, 1)
	if err == nil {
		t.Fatal("expected export to fail")
	}

	got, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read existing output: %v", readErr)
	}

	if string(got) != string(original) {
		t.Errorf("existing output changed: got %q", got)
	}
}

func TestExportFileRejectsMissingInput(t *testing.T) {
	dir := t.TempDir()

	_, err := ExportFile(
		filepath.Join(dir, "missing.json"),
		filepath.Join(dir, "report.json"),
		1,
	)

	if err == nil {
		t.Fatal("expected an error for a missing input file")
	}
}

func TestExportedReportDateMatchesInputDate(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "aggregated.json")

	input := `[{
		"source": "weather",
		"date": "2026-10-27",
		"dimensions": {"latitude": "28.61"},
		"metrics": {"temperature_2m_max": 32.5},
		"record_count": 1
	}]`

	if err := os.WriteFile(inputPath, []byte(input), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	reports, err := ExportFile(inputPath, filepath.Join(dir, "report.json"), 99)
	if err != nil {
		t.Fatalf("ExportFile() error = %v", err)
	}

	if len(reports) != 1 {
		t.Fatalf("got %d reports, want 1", len(reports))
	}

	wantDate := time.Date(2026, time.October, 27, 0, 0, 0, 0, time.UTC)
	if !reports[0].ReportDate.Equal(wantDate) {
		t.Errorf("ReportDate = %v, want %v", reports[0].ReportDate, wantDate)
	}
}
