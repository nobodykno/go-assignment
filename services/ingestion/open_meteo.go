package ingestion

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// OpenMeteoResponse represents the daily fields returned by Open-Meteo.
type OpenMeteoResponse struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Daily     struct {
		Time             []string  `json:"time"`
		TemperatureMax   []float64 `json:"temperature_2m_max"`
		TemperatureMin   []float64 `json:"temperature_2m_min"`
		PrecipitationSum []float64 `json:"precipitation_sum"`
	} `json:"daily"`
}

// ParseOpenMeteo converts Open-Meteo's nested daily response into RawRecord values.
func ParseOpenMeteo(body []byte) ([]RawRecord, error) {
	var response OpenMeteoResponse

	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode Open-Meteo response: %w", err)
	}

	daily := response.Daily
	count := len(daily.Time)

	if count == 0 {
		return nil, fmt.Errorf("Open-Meteo response contains no daily dates")
	}

	if len(daily.TemperatureMax) != count ||
		len(daily.TemperatureMin) != count ||
		len(daily.PrecipitationSum) != count {
		return nil, fmt.Errorf("Open-Meteo daily arrays have different lengths")
	}

	records := make([]RawRecord, 0, count)

	for i, date := range daily.Time {
		records = append(records, RawRecord{
			"date":               date,
			"latitude":           strconv.FormatFloat(response.Latitude, 'f', -1, 64),
			"longitude":          strconv.FormatFloat(response.Longitude, 'f', -1, 64),
			"temperature_2m_max": strconv.FormatFloat(daily.TemperatureMax[i], 'f', -1, 64),
			"temperature_2m_min": strconv.FormatFloat(daily.TemperatureMin[i], 'f', -1, 64),
			"precipitation_sum":  strconv.FormatFloat(daily.PrecipitationSum[i], 'f', -1, 64),
		})
	}

	return records, nil
}
