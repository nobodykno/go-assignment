package controller

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"go-assignment/apps/api/service"
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type JobsController struct {
	JobService *service.JobService
}

type JobProgressResponse struct {
	ID                  uint    `json:"id"`
	Name                string  `json:"name"`
	Status              string  `json:"status"`
	ProgressPercentage  float64 `json:"progress_percentage"`
	TotalRecords        int     `json:"total_records"`
	ProcessedRecords    int     `json:"processed_records"`
	ValidRecords        int     `json:"valid_records"`
	InvalidRecords      int     `json:"invalid_records"`
	ErrorMessage        *string `json:"error_message"`
	TransformedFilePath string  `json:"transformed_file_path"`
	AggregatedFilePath  string  `json:"aggregated_file_path"`
	ExportedFilePath    string  `json:"exported_file_path"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
}

type JobsListResponse struct {
	Count int                   `json:"count"`
	Jobs  []JobProgressResponse `json:"jobs"`
}

type JobsErrorResponse struct {
	Error string `json:"error"`
}

func NewJobsController(jobService *service.JobService) *JobsController {
	return &JobsController{
		JobService: jobService,
	}
}

// GetJob handles GET /jobs/{id}.
func (c *JobsController) GetJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJobsError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id, err := strconv.ParseUint(
		strings.TrimSpace(r.PathValue("id")),
		10,
		64,
	)
	if err != nil || id == 0 {
		writeJobsError(
			w,
			http.StatusBadRequest,
			"job ID must be a positive integer",
		)
		return
	}

	job, err := c.JobService.GetJobByID(uint(id))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeJobsError(w, http.StatusNotFound, "job not found")
			return
		}

		writeJobsError(
			w,
			http.StatusInternalServerError,
			"failed to retrieve job",
		)
		return
	}

	writeJobsJSON(w, http.StatusOK, toJobProgressResponse(job))
}

// ListJobs handles GET /jobs.
func (c *JobsController) ListJobs(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJobsError(
			w,
			http.StatusMethodNotAllowed,
			"method not allowed",
		)
		return
	}

	jobs, err := c.JobService.GetAllJobs()
	if err != nil {
		writeJobsError(
			w,
			http.StatusInternalServerError,
			"failed to retrieve jobs",
		)
		return
	}

	responses := make([]JobProgressResponse, 0, len(jobs))

	for i := range jobs {
		responses = append(
			responses,
			toJobProgressResponse(&jobs[i]),
		)
	}

	writeJobsJSON(w, http.StatusOK, JobsListResponse{
		Count: len(responses),
		Jobs:  responses,
	})
}

func toJobProgressResponse(job *models.Job) JobProgressResponse {
	var progress float64

	switch job.Status {
	case "pending":
		progress = 0

	case "processing":
		// Ingestion and validation are in progress.
		if job.TotalRecords > 0 {
			progress = float64(job.ProcessedRecords) /
				float64(job.TotalRecords) * 25

			if progress > 25 {
				progress = 25
			}
		} else {
			progress = 5
		}

	case "validated":
		progress = 25

	case "transforming":
		progress = 35

	case "transformed":
		progress = 50

	case "aggregating":
		progress = 65

	case "aggregated":
		progress = 75

	case "exporting":
		progress = 90

	case "exported":
		progress = 100

	case "failed":
		// Preserve partial progress when the job fails.
		switch {
		case job.ExportedFilePath != "":
			progress = 100
		case job.AggregatedFilePath != "":
			progress = 75
		case job.TransformedFilePath != "":
			progress = 50
		case job.TotalRecords > 0 &&
			job.ProcessedRecords > 0:
			progress = float64(job.ProcessedRecords) /
				float64(job.TotalRecords) * 25
			if progress > 25 {
				progress = 25
			}
		}
	}

	return JobProgressResponse{
		ID:                  job.ID,
		Name:                job.Name,
		Status:              job.Status,
		ProgressPercentage:  progress,
		TotalRecords:        job.TotalRecords,
		ProcessedRecords:    job.ProcessedRecords,
		ValidRecords:        job.ValidRecords,
		InvalidRecords:      job.InvalidRecords,
		ErrorMessage:        job.ErrorMessage,
		TransformedFilePath: job.TransformedFilePath,
		AggregatedFilePath:  job.AggregatedFilePath,
		ExportedFilePath:    job.ExportedFilePath,
		CreatedAt:           job.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:           job.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

func writeJobsJSON(
	w http.ResponseWriter,
	status int,
	value interface{},
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeJobsError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	writeJobsJSON(w, status, JobsErrorResponse{
		Error: message,
	})
}
