
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"go-assignment/packages/models"
	"go-assignment/services/ingestion"

	"gorm.io/gorm"
)

type JobWorker struct {
	DB         *gorm.DB
	HTTPClient *http.Client
}

type ValidateRequest struct {
	JobID   uint                `json:"job_id"`
	Records []map[string]string `json:"records"`
}

type ValidationResult struct {
	RecordNumber int      `json:"record_number"`
	Record       map[string]string `json:"record"`
	Errors       []string `json:"errors"`
}

type ValidateResponse struct {
	JobID          uint               `json:"job_id"`
	TotalRecords   int                `json:"total_records"`
	ValidRecords   int                `json:"valid_records"`
	InvalidRecords int                `json:"invalid_records"`
	Results        []ValidationResult `json:"results"`
}

func (w *JobWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			log.Println("ingestion worker stopped")
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			log.Printf("ingestion worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("ingestion worker stopped")
			return
		case <-ticker.C:
		}
	}
}

func (w *JobWorker) processPendingJob(ctx context.Context) error {
	var job models.Job

	err := w.DB.
		Where("status = ?", "pending").
		Order("created_at ASC").
		First(&job).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	// Claim the job only if it is still pending.
	result := w.DB.Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "pending").
		Updates(map[string]interface{}{
			"status":        "processing",
			"error_message": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}

	if job.FilePath == "" {
		return w.failJob(job.ID, "job has no uploaded file path")
	}

	// Read records from the uploaded CSV or JSON file.
	records, err := ingestion.ReadFile(job.FilePath)
	if err != nil {
		return w.failJob(job.ID, err.Error())
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Convert ingestion records into the JSON request format.
	requestRecords := make([]map[string]string, 0, len(records))

	for _, record := range records {
		requestRecords = append(
			requestRecords,
			map[string]string(record),
		)
	}

	// Send all records in one batch to the validation service.
	validationResponse, err := w.validateRecords(
		ctx,
		job.ID,
		requestRecords,
	)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return w.failJob(job.ID, err.Error())
	}

	// Collect validation errors for storage and logging.
	var errorMessages []string

	for _, result := range validationResponse.Results {
		for _, validationError := range result.Errors {
			errorMessages = append(
				errorMessages,
				fmt.Sprintf(
					"record %d: %s",
					result.RecordNumber,
					validationError,
				),
			)
		}
	}

	errorMessage := strings.Join(errorMessages, "; ")

	// Every returned result counts as processed, including invalid records.

	return w.DB.Model(&models.Job{}).
    Where("id = ?", job.ID).
    Updates(map[string]interface{}{
        "status":            "validated",
        "total_records":     validationResponse.TotalRecords,
        "processed_records": len(validationResponse.Results),
        "invalid_records":   validationResponse.InvalidRecords,
        "error_message":     nullableMessage(errorMessage),
    }).Error
}

func (w *JobWorker) validateRecords(
	ctx context.Context,
	jobID uint,
	records []map[string]string,
) (*ValidateResponse, error) {
	validationURL := os.Getenv("VALIDATION_SERVICE_URL")
	if validationURL == "" {
		validationURL = "http://localhost:8081"
	}
	validationURL = strings.TrimRight(validationURL, "/") + "/validate"

	requestBody := ValidateRequest{
		JobID:   jobID,
		Records: records,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("encode validation request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		validationURL,
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return nil, fmt.Errorf("create validation request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := w.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 35 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call validation service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf(
			"validation service returned HTTP %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	var response ValidateResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode validation response: %w", err)
	}

	if response.JobID != jobID {
		return nil, fmt.Errorf(
			"validation response job ID mismatch: expected %d, got %d",
			jobID,
			response.JobID,
		)
	}

	return &response, nil
}

func nullableMessage(message string) interface{} {
	if message == "" {
		return nil
	}
	return message
}

func (w *JobWorker) failJob(jobID uint, message string) error {
	return w.DB.Model(&models.Job{}).
		Where("id = ?", jobID).
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
		}).Error
}
