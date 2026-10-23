
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
	RecordNumber int               `json:"record_number"`
	Record       map[string]string `json:"record"`
	Errors       []string          `json:"errors"`
}

type ValidateResponse struct {
	JobID          uint               `json:"job_id"`
	TotalRecords   int                `json:"total_records"`
	ValidRecords   int                `json:"valid_records"`
	InvalidRecords int                `json:"invalid_records"`
	Results        []ValidationResult `json:"results"`
}

// Run continuously looks for pending jobs until the context is cancelled.
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

// processPendingJob claims and processes the oldest pending job.
func (w *JobWorker) processPendingJob(ctx context.Context) error {
	var job models.Job

	err := w.DB.WithContext(ctx).
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
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "pending").
		Updates(map[string]interface{}{
			"status":        "processing",
			"error_message": nil,
		})

	if claim.Error != nil {
		return claim.Error
	}
	if claim.RowsAffected == 0 {
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

	if err := ctx.Err(); err != nil {
		return w.failJob(job.ID, err.Error())
	}

	// Convert ingestion records into the validation request format.
	requestRecords := make([]map[string]string, 0, len(records))

	for _, record := range records {
		requestRecords = append(
			requestRecords,
			map[string]string(record),
		)
	}

	if len(requestRecords) == 0 {
		return w.failJob(job.ID, "uploaded file contains no records")
	}

	// Send records to the validation service.
	validationResponse, err := w.validateRecords(
		ctx,
		job.ID,
		requestRecords,
	)
	if err != nil {
		if ctx.Err() != nil {
			return w.failJob(job.ID, ctx.Err().Error())
		}
		return w.failJob(job.ID, err.Error())
	}

	// Verify that the response accounts for every submitted record.
	if validationResponse.TotalRecords != len(requestRecords) ||
		len(validationResponse.Results) != validationResponse.TotalRecords {
		return w.failJob(
			job.ID,
			fmt.Sprintf(
				"validation result count mismatch: submitted %d, reported total %d, returned results %d",
				len(requestRecords),
				validationResponse.TotalRecords,
				len(validationResponse.Results),
			),
		)
	}

	// Collect validation errors for the job summary.
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

	// Convert each validation result into a database model.
	jobRecords := make([]models.JobRecord, 0, len(validationResponse.Results))

	for i := 0; i < len(validationResponse.Results); i++ {
		result := validationResponse.Results[i]
	
		validationErrors := result.Errors
		if validationErrors == nil {
			validationErrors = []string{}
		}
	
		jobRecords = append(jobRecords, models.JobRecord{
			JobID:            job.ID,
			RecordNumber:     result.RecordNumber,
			RecordData:       result.Record,
			IsValid:          len(result.Errors) == 0,
			ValidationErrors: validationErrors,
		})
	}
	if err := ctx.Err(); err != nil {
		return w.failJob(job.ID, err.Error())
	}

	// Save the records and job summary in one transaction.
	err = w.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(jobRecords) > 0 {
			if err := tx.CreateInBatches(&jobRecords, 500).Error; err != nil {
				return fmt.Errorf("save validation results: %w", err)
			}
		}

		result := tx.Model(&models.Job{}).
			Where("id = ? AND status = ?", job.ID, "processing").
			Updates(map[string]interface{}{
				"status":            "validated",
				"total_records":     validationResponse.TotalRecords,
				"processed_records": len(validationResponse.Results),
				"valid_records":     validationResponse.ValidRecords,
				"invalid_records":   validationResponse.InvalidRecords,
				"error_message":     nullableMessage(errorMessage),
			})

		if result.Error != nil {
			return fmt.Errorf("update job summary: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			return fmt.Errorf(
				"job %d was not found or is no longer processing",
				job.ID,
			)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("persist job %d validation results: %w", job.ID, err)
	}

	log.Printf(
		"job %d validated: total=%d valid=%d invalid=%d",
		job.ID,
		validationResponse.TotalRecords,
		validationResponse.ValidRecords,
		validationResponse.InvalidRecords,
	)

	return nil
}

// validateRecords sends a batch of records to the validation service.
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

// nullableMessage returns nil when there is no error message.
func nullableMessage(message string) interface{} {
	if message == "" {
		return nil
	}
	return message
}

// failJob marks a job as failed.
func (w *JobWorker) failJob(jobID uint, message string) error {
	return w.DB.Model(&models.Job{}).
		Where("id = ?", jobID).
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
		}).Error
}
