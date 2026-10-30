package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const maxRecordsPerRequest = 1000

type TransformRequest struct {
	JobID   uint     `json:"job_id"`
	Records []Record `json:"records"`
}

type TransformResponse struct {
	JobID        uint              `json:"job_id"`
	TotalRecords int               `json:"total_records"`
	Transformed  int               `json:"transformed_records"`
	Failed       int               `json:"failed_records"`
	Results      []TransformResult `json:"results"`
}

type TransformResult struct {
	RecordNumber int                `json:"record_number"`
	Record       *TransformedRecord `json:"record,omitempty"`
	Error        string             `json:"error,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func NewHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /transform", transformHandler)

	return mux
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func transformHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 5<<20)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)

	var request TransformRequest
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: fmt.Sprintf("invalid request body: %v", err),
		})
		return
	}

	// Reject additional JSON values after the first object.
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "request must contain a single JSON object",
		})
		return
	}

	if request.JobID == 0 {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "job_id is required",
		})
		return
	}

	if len(request.Records) == 0 {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "at least one record is required",
		})
		return
	}

	if len(request.Records) > maxRecordsPerRequest {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "maximum 1000 records per request",
		})
		return
	}

	response := TransformResponse{
		JobID:        request.JobID,
		TotalRecords: len(request.Records),
		Results:      make([]TransformResult, 0, len(request.Records)),
	}

	for i, record := range request.Records {
		transformed, err := TransformRecord(record)

		result := TransformResult{
			RecordNumber: i + 1,
		}

		if err != nil {
			result.Error = err.Error()
			response.Failed++
		} else {
			result.Record = &transformed
			response.Transformed++
		}

		response.Results = append(response.Results, result)
	}

	writeJSON(w, http.StatusOK, response)
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		return
	}
}
