
package controller

import (
	"encoding/json"
	"net/http"
	"strings"

	"go-assignment/apps/api/dto/request"
	"go-assignment/apps/api/dto/response"
	"go-assignment/apps/api/service"
)

type JobController struct {
	JobService *service.JobService
}

func (ctrl *JobController) CreateJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req request.CreateJobRequest

	// Decode the incoming JSON request.
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate the job name.
	if strings.TrimSpace(req.Name) == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}

	// Create the job through the service.
	job, err := ctrl.JobService.CreateJob(req.Name)
	if err != nil {
		http.Error(
			w,
			"Failed to create job",
			http.StatusInternalServerError,
		)
		return
	}

	// Map the job to the response DTO.
	res := response.CreateJobResponse{
		Message: "Job created successfully",
		Data: response.JobResponseData{
			ID:     job.ID,
			Name:   job.Name,
			Status: job.Status,
		},
	}

	// Return the JSON response.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		return
	}
}
