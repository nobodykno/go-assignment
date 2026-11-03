package ingestion_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go-assignment/packages/models"
	"go-assignment/services/ingestion"
)

func TestStreamJSONEmitsEachElement(t *testing.T) {
	input := `[{"country":"India","cases":100},{"country":"Nepal","active":true}]`

	var records []ingestion.RawRecord
	err := ingestion.StreamJSON(strings.NewReader(input), func(record ingestion.RawRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamJSON returned error: %v", err)
	}

	if len(records) != 2 || records[0]["cases"] != "100" || records[1]["active"] != "true" {
		t.Fatalf("unexpected records: %v", records)
	}
}

func TestStreamJSONRejectsInvalidInput(t *testing.T) {
	tests := map[string]string{
		"not an array":   `{"a":"b"}`,
		"trailing value": `[{"a":"b"}] [1]`,
		"empty object":   `[{}]`,
		"truncated":      `[{"a":"b"},`,
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			err := ingestion.StreamJSON(strings.NewReader(input), func(ingestion.RawRecord) error { return nil })
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestStreamCSVStopsWhenEmitFails(t *testing.T) {
	input := "a,b\n1,2\n3,4\n5,6\n"
	stop := context.Canceled
	seen := 0

	err := ingestion.StreamCSV(strings.NewReader(input), func(ingestion.RawRecord) error {
		seen++
		if seen == 2 {
			return stop
		}
		return nil
	})

	if err != stop || seen != 2 {
		t.Fatalf("expected to stop after 2 records with the emit error, got seen=%d err=%v", seen, err)
	}
}

func TestStreamCountsAndTagsMixedFileSources(t *testing.T) {
	uploads := t.TempDir()
	t.Setenv("UPLOADS_DIR", uploads)

	writeFile(t, filepath.Join(uploads, "a.csv"), "country,cases\nIndia,1\nNepal,2\n")
	writeFile(t, filepath.Join(uploads, "b.json"), `[{"country":"Peru","cases":"3"}]`)

	stream, err := ingestion.OpenSources(context.Background(), []models.SourceDefinition{
		{Name: "first", Type: "csv", FilePath: "a.csv"},
		{Name: "second", Type: "json", FilePath: "b.json"},
	}, nil)
	if err != nil {
		t.Fatalf("OpenSources returned error: %v", err)
	}

	count, err := stream.Count(context.Background())
	if err != nil || count != 3 {
		t.Fatalf("expected 3 records, got %d (err %v)", count, err)
	}

	var sources []string
	err = stream.Each(context.Background(), func(record ingestion.RawRecord) error {
		sources = append(sources, record["source"])
		return nil
	})
	if err != nil {
		t.Fatalf("Each returned error: %v", err)
	}

	if strings.Join(sources, ",") != "first,first,second" {
		t.Fatalf("records not tagged in source order: %v", sources)
	}
}

func TestOpenSourcesRejectsPathOutsideUploads(t *testing.T) {
	t.Setenv("UPLOADS_DIR", t.TempDir())

	outside := filepath.Join(t.TempDir(), "secret.csv")
	writeFile(t, outside, "a\n1\n")

	_, err := ingestion.OpenSources(context.Background(), []models.SourceDefinition{
		{Name: "bad", Type: "csv", FilePath: outside},
	}, nil)
	if err == nil {
		t.Fatal("expected a path outside the uploads directory to be rejected")
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
