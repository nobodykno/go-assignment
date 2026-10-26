
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const maxRecordsPerRequest = 1000

type ValidationHandler struct {
	Pool WorkerPool
}

type ValidateRequest struct {
	JobID   uint     `json:"job_id"`
	Records []Record `json:"records"`
}

type ValidateResponse struct {
	JobID          uint               `json:"job_id"`
	TotalRecords   int                `json:"total_records"`
	ValidRecords   int                `json:"valid_records"`
	InvalidRecords int                `json:"invalid_records"`
	Results        []ValidationResult `json:"results"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// NewHandler registers the validation service endpoints.
func NewHandler(workerCount int) http.Handler {
	mux := http.NewServeMux()

	if workerCount < 1 {
		workerCount = 1
	}

	handler := &ValidationHandler{
		Pool: WorkerPool{
			WorkerCount: workerCount,
		},
	}

	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("POST /validate", handler.Validate)

	return mux
}

// Health checks whether the validation service is running.
func (h *ValidationHandler) Health(
	w http.ResponseWriter,
	r *http.Request,
) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

// Validate validates a batch of records concurrently.
func (h *ValidationHandler) Validate(
	w http.ResponseWriter,
	r *http.Request,
) {
	r.Body = http.MaxBytesReader(w, r.Body, 5<<20)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)

	var request ValidateRequest

	if err := decoder.Decode(&request); err != nil {
		status := http.StatusBadRequest
		message := fmt.Sprintf("invalid request body: %v", err)

		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			status = http.StatusRequestEntityTooLarge
			message = "request body exceeds the 5 MB limit"
		}

		writeJSON(w, status, ErrorResponse{Error: message})
		return
	}

	// Reject additional JSON values after the request object.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
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
			Error: fmt.Sprintf(
				"maximum %d records per request",
				maxRecordsPerRequest,
			),
		})
		return
	}

	ctx := r.Context()

	input := make(chan RecordJob, len(request.Records))
	output := make(chan ValidationResult, len(request.Records))

	for i, record := range request.Records {
		input <- RecordJob{
			RecordNumber: i + 1,
			Record:       record,
		}
	}
	close(input)

	// Run the worker pool concurrently.
	go h.Pool.Run(ctx, input, output)

	response := ValidateResponse{
		JobID:        request.JobID,
		TotalRecords: len(request.Records),
		Results:      make([]ValidationResult, 0, len(request.Records)),
	}

	for {
		select {
		case <-ctx.Done():
			// The client disconnected or cancelled the request.
			return

		case result, ok := <-output:
			if !ok {
				writeJSON(w, http.StatusOK, response)
				return
			}

			if len(result.Errors) == 0 {
				result.Errors = []string{}
				response.ValidRecords++
			} else {
				response.InvalidRecords++
			}

			response.Results = append(response.Results, result)
		}
	}
}

func writeJSON(
	w http.ResponseWriter,
	statusCode int,
	data interface{},
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	_ = json.NewEncoder(w).Encode(data)
}
