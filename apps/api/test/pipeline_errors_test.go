
package test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"go-assignment/packages/models"
	"gorm.io/gorm"
)

type PipelineErrorsController struct {
	DB *gorm.DB
}

type PipelineErrorsResponse struct {
	JobID  uint                  `json:"job_id"`
	Count  int                   `json:"count"`
	Errors []models.PipelineError `json:"errors"`
}

type PipelineErrorsErrorResponse struct {
	Error string `json:"error"`
}

func (c *PipelineErrorsController) GetByJobID(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		writePipelineErrorsJSON(w, http.StatusMethodNotAllowed,
			PipelineErrorsErrorResponse{Error: "Method not allowed"})
		return
	}

	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writePipelineErrorsJSON(w, http.StatusBadRequest,
			PipelineErrorsErrorResponse{Error: "Invalid job ID"})
		return
	}

	if c.DB == nil {
		writePipelineErrorsJSON(w, http.StatusInternalServerError,
			PipelineErrorsErrorResponse{Error: "Database unavailable"})
		return
	}

	var job models.Job
	err = c.DB.WithContext(r.Context()).First(&job, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		writePipelineErrorsJSON(w, http.StatusNotFound,
			PipelineErrorsErrorResponse{Error: "Job not found"})
		return
	}
	if err != nil {
		writePipelineErrorsJSON(w, http.StatusInternalServerError,
			PipelineErrorsErrorResponse{Error: "Failed to retrieve job"})
		return
	}

	var pipelineErrors []models.PipelineError
	err = c.DB.WithContext(r.Context()).
		Where("job_id = ?", id).
		Order("created_at ASC").
		Find(&pipelineErrors).Error
	if err != nil {
		writePipelineErrorsJSON(w, http.StatusInternalServerError,
			PipelineErrorsErrorResponse{Error: "Failed to retrieve pipeline errors"})
		return
	}

	if pipelineErrors == nil {
		pipelineErrors = []models.PipelineError{}
	}

	writePipelineErrorsJSON(w, http.StatusOK, PipelineErrorsResponse{
		JobID:  uint(id),
		Count:  len(pipelineErrors),
		Errors: pipelineErrors,
	})
}

func writePipelineErrorsJSON(
	w http.ResponseWriter,
	status int,
	data interface{},
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		return
	}
}
