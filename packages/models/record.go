package models

type Record struct {
	Source     string             `json:"source"`
	Date       string             `json:"date"`
	Dimensions map[string]string  `json:"dimensions"`
	Metrics    map[string]float64 `json:"metrics"`
}
