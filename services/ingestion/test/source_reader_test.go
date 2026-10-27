package ingestion_test

import (
	"context"
	"net/http"
	"testing"

	"go-assignment/packages/models"
	ingestion "go-assignment/services/ingestion"
)

func TestReadSourcesRejectsEmptySources(t *testing.T) {
	_, err := ingestion.ReadSources(
		context.Background(),
		nil,
		nil,
	)

	if err == nil {
		t.Fatal("expected an error when no sources are configured")
	}
}

func TestReadSourcesRejectsUnsupportedSourceType(t *testing.T) {
	sources := []models.SourceDefinition{
		{
			Name: "unsupported-source",
			Type: "unknown",
		},
	}

	_, err := ingestion.ReadSources(
		context.Background(),
		sources,
		&http.Client{},
	)

	if err == nil {
		t.Fatal("expected an error for an unsupported source type")
	}
}

func TestReadSourcesRejectsMissingAPIURL(t *testing.T) {
	sources := []models.SourceDefinition{
		{
			Name: "coin-source",
			Type: "api",
			URL:  "",
		},
	}

	_, err := ingestion.ReadSources(
		context.Background(),
		sources,
		&http.Client{},
	)

	if err == nil {
		t.Fatal("expected an error when the API URL is missing")
	}
}

func TestReadSourcesRejectsMissingFilePath(t *testing.T) {
	sources := []models.SourceDefinition{
		{
			Name:     "file-source",
			Type:     "csv",
			FilePath: "",
		},
	}

	_, err := ingestion.ReadSources(
		context.Background(),
		sources,
		nil,
	)

	if err == nil {
		t.Fatal("expected an error when the file path is missing")
	}
}

func TestReadSourcesReturnsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sources := []models.SourceDefinition{
		{
			Name: "cancelled-source",
			Type: "api",
			URL:  "https://example.com/data",
		},
	}

	_, err := ingestion.ReadSources(
		ctx,
		sources,
		nil,
	)

	if err == nil {
		t.Fatal("expected an error when the context is cancelled")
	}
}
