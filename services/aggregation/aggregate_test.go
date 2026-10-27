package main

import (
	"math"
	"testing"
)

func TestAggregateRecordsOperations(t *testing.T) {
	records := []TransformedRecord{
		{
			Source:     "test",
			Date:       "2026-10-27",
			Dimensions: map[string]string{"city": "Delhi"},
			Metrics: map[string]float64{
				"cases":              10,
				"price_usd":          100,
				"temperature_2m_min": 20,
				"temperature_2m_max": 30,
			},
			MetricOperations: map[string]string{
				"cases":              "sum",
				"price_usd":          "avg",
				"temperature_2m_min": "min",
				"temperature_2m_max": "max",
			},
		},
		{
			Source:     "test",
			Date:       "2026-10-27",
			Dimensions: map[string]string{"city": "Delhi"},
			Metrics: map[string]float64{
				"cases":              5,
				"price_usd":          200,
				"temperature_2m_min": 18,
				"temperature_2m_max": 32,
			},
			MetricOperations: map[string]string{
				"cases":              "sum",
				"price_usd":          "avg",
				"temperature_2m_min": "min",
				"temperature_2m_max": "max",
			},
		},
	}

	got, err := AggregateRecords(records)
	if err != nil {
		t.Fatalf("AggregateRecords() returned unexpected error: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d aggregate groups, want 1", len(got))
	}

	result := got[0]

	tests := []struct {
		metric string
		want   float64
	}{
		{metric: "cases", want: 15},
		{metric: "price_usd", want: 150},
		{metric: "temperature_2m_min", want: 18},
		{metric: "temperature_2m_max", want: 32},
	}

	for _, tt := range tests {
		t.Run(tt.metric, func(t *testing.T) {
			value, exists := result.Metrics[tt.metric]
			if !exists {
				t.Fatalf("missing metric %q", tt.metric)
			}

			if math.Abs(value-tt.want) > 1e-9 {
				t.Errorf("metric %q = %v, want %v", tt.metric, value, tt.want)
			}
		})
	}

	if result.RecordCount != 2 {
		t.Errorf("RecordCount = %d, want 2", result.RecordCount)
	}
}

func TestAggregateRecordsKeepsDimensionsSeparate(t *testing.T) {
	records := []TransformedRecord{
		{
			Source:     "open-meteo",
			Date:       "2026-10-27",
			Dimensions: map[string]string{"latitude": "28.61", "longitude": "77.20"},
			Metrics:    map[string]float64{"precipitation_sum": 4},
		},
		{
			Source:     "open-meteo",
			Date:       "2026-10-27",
			Dimensions: map[string]string{"latitude": "19.07", "longitude": "72.87"},
			Metrics:    map[string]float64{"precipitation_sum": 8},
		},
	}

	got, err := AggregateRecords(records)
	if err != nil {
		t.Fatalf("AggregateRecords() returned unexpected error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d groups, want 2 because locations differ", len(got))
	}
}

func TestAggregateRecordsRejectsInvalidMetric(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := []TransformedRecord{
				{
					Source:  "test",
					Date:    "2026-10-27",
					Metrics: map[string]float64{"cases": tt.value},
				},
			}

			_, err := AggregateRecords(records)
			if err == nil {
				t.Fatal("expected an error for a non-finite metric")
			}
		})
	}
}

func TestAggregateRecordsRejectsEmptyInput(t *testing.T) {
	_, err := AggregateRecords(nil)
	if err == nil {
		t.Fatal("expected an error for empty input")
	}
}
