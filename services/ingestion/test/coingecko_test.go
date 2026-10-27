package ingestion_test

import (
	"encoding/json"
	ingestion "go-assignment/services/ingestion"
	"strings"
	"testing"
)

func TestParseCoinGecko(t *testing.T) {
	body := []byte(`[
		{
			"id": "bitcoin",
			"symbol": "btc",
			"name": "Bitcoin",
			"current_price": 67250.50,
			"last_updated": "2026-10-27T12:30:00.000Z"
		},
		{
			"id": "ethereum",
			"symbol": "eth",
			"name": "Ethereum",
			"current_price": 2500.75,
			"last_updated": "2026-10-27T13:00:00Z"
		}
	]`)

	records, err := ingestion.ParseCoinGecko(body)
	if err != nil {
		t.Fatalf("ParseCoinGecko() error = %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}

	first := records[0]

	expected := map[string]string{
		"date":      "2026-10-27",
		"coin":      "bitcoin",
		"price_usd": "67250.5",
		"symbol":    "btc",
		"name":      "Bitcoin",
	}

	for key, want := range expected {
		if got := first[key]; got != want {
			t.Errorf("record[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestParseCoinGeckoRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "invalid JSON",
			body: `[{"id":`,
			want: "decode CoinGecko",
		},
		{
			name: "empty response",
			body: `[]`,
			want: "no market records",
		},
		{
			name: "missing coin ID",
			body: `[{"symbol":"btc","current_price":10,"last_updated":"2026-10-27T12:00:00Z"}]`,
			want: "no coin ID",
		},
		{
			name: "missing price",
			body: `[{"id":"bitcoin","last_updated":"2026-10-27T12:00:00Z"}]`,
			want: "no current price",
		},
		{
			name: "zero price",
			body: `[{"id":"bitcoin","current_price":0,"last_updated":"2026-10-27T12:00:00Z"}]`,
			want: "invalid price",
		},
		{
			name: "missing timestamp",
			body: `[{"id":"bitcoin","current_price":100}]`,
			want: "no last_updated",
		},
		{
			name: "invalid timestamp",
			body: `[{"id":"bitcoin","current_price":100,"last_updated":"yesterday"}]`,
			want: "invalid last_updated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ingestion.ParseCoinGecko([]byte(tt.body))
			if err == nil {
				t.Fatal("expected an error")
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestParseCoinGeckoPreservesSourceAssignment(t *testing.T) {
	body := []byte(`[
		{
			"id": "bitcoin",
			"symbol": "btc",
			"name": "Bitcoin",
			"current_price": 100,
			"last_updated": "2026-10-27T12:00:00Z"
		}
	]`)

	records, err := ingestion.ParseCoinGecko(body)
	if err != nil {
		t.Fatalf("ParseCoinGecko() error = %v", err)
	}

	// ParseCoinGecko normalizes the provider payload.
	// ReadSources assigns the configured source name afterwards.
	if _, exists := records[0]["source"]; exists {
		t.Error("ParseCoinGecko should not assign source attribution itself")
	}
}

func TestParseCoinGeckoRejectsTrailingJSON(t *testing.T) {
	body := []byte(`[
		{
			"id": "bitcoin",
			"current_price": 100,
			"last_updated": "2026-10-27T12:00:00Z"
		}
	] {"unexpected":true}`)

	_, err := ingestion.ParseCoinGecko(body)
	if err == nil {
		t.Fatal("expected trailing JSON to be rejected")
	}
}

func TestParseCoinGeckoOutputIsValidJSON(t *testing.T) {
	body := []byte(`[
		{
			"id": "bitcoin",
			"symbol": "btc",
			"name": "Bitcoin",
			"current_price": 100,
			"last_updated": "2026-10-27T12:00:00Z"
		}
	]`)

	records, err := ingestion.ParseCoinGecko(body)
	if err != nil {
		t.Fatalf("ParseCoinGecko() error = %v", err)
	}

	encoded, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("failed to encode normalized records: %v", err)
	}

	var decoded []ingestion.RawRecord
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("normalized records are not valid JSON: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("decoded %d records, want 1", len(decoded))
	}
}
