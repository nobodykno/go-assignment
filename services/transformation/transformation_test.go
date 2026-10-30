package main

import (
	"math"
	"testing"
)

func TestTransformRecord(t *testing.T) {
	tests := []struct {
		name           string
		input          Record
		wantSource     string
		wantDate       string
		wantDimensions map[string]string
		wantMetrics    map[string]float64
		wantMetricOps  map[string]string
	}{
		{
			name: "weather record preserves coordinates as dimensions",
			input: Record{
				"source":             "open-meteo",
				"date":               "2026-10-27",
				"latitude":           "28.61",
				"longitude":          "77.20",
				"temperature_2m_max": "32.5",
				"temperature_2m_min": "24.1",
				"precipitation_sum":  "4.2",
			},
			wantSource: "open-meteo",
			wantDate:   "2026-10-27",
			wantDimensions: map[string]string{
				"latitude":  "28.61",
				"longitude": "77.20",
			},
			wantMetrics: map[string]float64{
				"temperature_2m_max": 32.5,
				"temperature_2m_min": 24.1,
				"precipitation_sum":  4.2,
			},
			wantMetricOps: map[string]string{
				"temperature_2m_max": "max",
				"temperature_2m_min": "min",
				"precipitation_sum":  "sum",
			},
		},
		{
			name: "CoinGecko record preserves coin information as dimensions",
			input: Record{
				"source":    "coingecko",
				"date":      "2026-10-27",
				"coin":      "bitcoin",
				"symbol":    "btc",
				"name":      "Bitcoin",
				"price_usd": "67250.50",
			},
			wantSource: "coingecko",
			wantDate:   "2026-10-27",
			wantDimensions: map[string]string{
				"coin":   "bitcoin",
				"symbol": "btc",
				"name":   "Bitcoin",
			},
			wantMetrics: map[string]float64{
				"price_usd": 67250.50,
			},
			wantMetricOps: map[string]string{
				"price_usd": "avg",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TransformRecord(tt.input)
			if err != nil {
				t.Fatalf("TransformRecord() returned unexpected error: %v", err)
			}

			if got.Source != tt.wantSource {
				t.Errorf("Source = %q, want %q", got.Source, tt.wantSource)
			}

			if got.Date != tt.wantDate {
				t.Errorf("Date = %q, want %q", got.Date, tt.wantDate)
			}

			for key, want := range tt.wantDimensions {
				if got.Dimensions[key] != want {
					t.Errorf(
						"Dimensions[%q] = %q, want %q",
						key,
						got.Dimensions[key],
						want,
					)
				}

				if _, exists := got.Metrics[key]; exists {
					t.Errorf("dimension %q was incorrectly stored as a metric", key)
				}
			}

			for key, want := range tt.wantMetrics {
				gotValue, exists := got.Metrics[key]
				if !exists {
					t.Errorf("missing metric %q", key)
					continue
				}

				if math.Abs(gotValue-want) > 1e-9 {
					t.Errorf("Metrics[%q] = %v, want %v", key, gotValue, want)
				}
			}

			for key, want := range tt.wantMetricOps {
				if got.MetricOperations[key] != want {
					t.Errorf(
						"MetricOperations[%q] = %q, want %q",
						key,
						got.MetricOperations[key],
						want,
					)
				}
			}
		})
	}
}

func TestTransformRecordInvalidDate(t *testing.T) {
	input := Record{
		"source": "test",
		"date":   "not-a-date",
		"cases":  "10",
	}

	_, err := TransformRecord(input)
	if err == nil {
		t.Fatal("expected an error for an invalid date")
	}
}

func TestTransformRecordRejectsNonFiniteNumbers(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "NaN", value: "NaN"},
		{name: "positive infinity", value: "+Inf"},
		{name: "negative infinity", value: "-Inf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := Record{
				"source": "test",
				"date":   "2026-10-27",
				"cases":  tt.value,
			}

			_, err := TransformRecord(input)
			if err == nil {
				t.Fatalf("expected an error for %q", tt.value)
			}
		})
	}
}
