package models

// The record shapes below are shared by every pipeline stage, so each
// service reads and writes the same structures instead of its own copy.

// RawRecord is one input record from a CSV row, JSON object or API
// response, with every value kept as text.
type RawRecord map[string]string

// TransformedRecord is a normalized record produced by transformation.
// MetricOperations names how each metric is aggregated (sum, avg, ...).
type TransformedRecord struct {
	Source           string             `json:"source"`
	Date             string             `json:"date"`
	Dimensions       map[string]string  `json:"dimensions"`
	Metrics          map[string]float64 `json:"metrics"`
	MetricOperations map[string]string  `json:"metric_operations,omitempty"`
}

// AggregatedRecord holds the aggregated metrics of all records sharing
// the same source, date and dimensions.
type AggregatedRecord struct {
	Source      string             `json:"source"`
	Date        string             `json:"date"`
	Dimensions  map[string]string  `json:"dimensions"`
	Metrics     map[string]float64 `json:"metrics"`
	RecordCount int                `json:"record_count"`
}

// ValidateRequest is the body of POST /validate on the validation service.
type ValidateRequest struct {
	JobID   uint        `json:"job_id"`
	Records []RawRecord `json:"records"`
}

// ValidationResult is the outcome for one record; RecordNumber is the
// 1-based position of the record within its request.
type ValidationResult struct {
	RecordNumber int       `json:"record_number"`
	Record       RawRecord `json:"record"`
	Errors       []string  `json:"errors"`
}

// ValidateResponse is the response of POST /validate.
type ValidateResponse struct {
	JobID          uint               `json:"job_id"`
	TotalRecords   int                `json:"total_records"`
	ValidRecords   int                `json:"valid_records"`
	InvalidRecords int                `json:"invalid_records"`
	Results        []ValidationResult `json:"results"`
}
