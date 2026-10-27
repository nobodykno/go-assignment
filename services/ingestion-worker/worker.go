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

	pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/jobs"
	"go-assignment/packages/models"
	"go-assignment/services/ingestion"

	"gorm.io/gorm"
)

const (
	validationBatchSize        = 500
	maxValidationRequestBytes  = 4 << 20 // 4 MiB
	maxValidationResponseBytes = 4 << 20 // 4 MiB
	validationRequestTimeout   = 35 * time.Second
	ingestionCleanupTimeout    = 5 * time.Second
	ingestionPollInterval      = 2 * time.Second
)

type JobWorker struct {
	DB             *gorm.DB
	HTTPClient     *http.Client
	ErrorCollector *pipelineerrors.Collector
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
	ticker := time.NewTicker(ingestionPollInterval)
	defer ticker.Stop()

	log.Println("Ingestion worker started")
	defer log.Println("Ingestion worker stopped")

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			if (errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded)) &&
				ctx.Err() != nil {
				return
			}

			log.Printf("Ingestion worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims and processes the oldest pending job.
func (w *JobWorker) processPendingJob(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var job models.Job

	err := w.DB.WithContext(ctx).
		Where("status = ?", "pending").
		Order("created_at ASC").
		First(&job).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("find pending job: %w", err)
	}

	// Claim the job atomically and initialize its heartbeat.
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "pending").
		Updates(map[string]interface{}{
			"status":            "processing",
			"error_message":     nil,
			"heartbeat_at":      time.Now().UTC(),
			"total_records":     0,
			"processed_records": 0,
			"valid_records":     0,
			"invalid_records":   0,
		})

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}
	if claim.RowsAffected == 0 {
		return nil
	}

	// Keep the heartbeat alive during processing. Stop it when this
	// processing attempt returns.
	stopHeartbeat := jobs.StartHeartbeat(
		ctx,
		w.DB,
		job.ID,
		"processing",
		jobs.DefaultHeartbeatInterval,
	)
	defer stopHeartbeat()

	log.Printf("Started ingestion for job %d", job.ID)

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Read records from mixed sources or a single uploaded file.
	var records []ingestion.RawRecord

	if job.SourceType == "mixed" {
		records, err = ingestion.ReadSources(
			ctx,
			job.SourceConfig.Sources,
			w.HTTPClient,
		)
	} else {
		if strings.TrimSpace(job.FilePath) == "" {
			return w.failJob(
				ctx,
				job.ID,
				"job has no uploaded file path",
			)
		}

		records, err = ingestion.ReadFile(job.FilePath)
	}

	if err != nil {
		if ctx.Err() != nil {
			return w.failJob(ctx, job.ID, ctx.Err().Error())
		}

		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("read input records: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	requestRecords := make([]map[string]string, 0, len(records))

	for _, record := range records {
		requestRecords = append(
			requestRecords,
			map[string]string(record),
		)
	}

	if len(requestRecords) == 0 {
		return w.failJob(
			ctx,
			job.ID,
			"input sources contain no records",
		)
	}

	// Set the total up front so clients can calculate progress.
	result := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "processing").
		Update("total_records", len(requestRecords))

	if result.Error != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("initialize validation progress: %v", result.Error),
		)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("job %d is no longer processing", job.ID)
	}

	var errorMessages []string
	totalValid := 0
	totalInvalid := 0

	// Process bounded batches. Each successful batch persists its records
	// and advances the progress counters in one database transaction.
	for start := 0; start < len(requestRecords); {
		if err := ctx.Err(); err != nil {
			return w.failJob(ctx, job.ID, err.Error())
		}

		end, err := validationBatchEnd(
			job.ID,
			requestRecords,
			start,
		)
		if err != nil {
			return w.failJob(ctx, job.ID, err.Error())
		}

		batch := requestRecords[start:end]

		response, err := w.validateRecords(
			ctx,
			job.ID,
			batch,
		)
		if err != nil {
			if ctx.Err() != nil {
				return w.failJob(ctx, job.ID, ctx.Err().Error())
			}

			return w.failJob(
				ctx,
				job.ID,
				fmt.Sprintf(
					"validate batch starting at record %d: %v",
					start+1,
					err,
				),
			)
		}

		// Verify each result has a unique, valid batch-local number.
		if err := validateBatchResults(response, len(batch)); err != nil {
			return w.failJob(
				ctx,
				job.ID,
				fmt.Sprintf(
					"invalid validation response for batch starting at record %d: %v",
					start+1,
					err,
				),
			)
		}

		// Convert batch-local numbers into original input positions.
		for i := range response.Results {
			response.Results[i].RecordNumber += start

			for _, validationError := range response.Results[i].Errors {
				errorMessages = append(
					errorMessages,
					fmt.Sprintf(
						"record %d: %s",
						response.Results[i].RecordNumber,
						validationError,
					),
				)
			}
		}

		if err := ctx.Err(); err != nil {
			return w.failJob(ctx, job.ID, err.Error())
		}

		if err := w.persistValidationBatch(ctx, job.ID, response); err != nil {
			if ctx.Err() != nil {
				return w.failJob(ctx, job.ID, ctx.Err().Error())
			}

			return w.failJob(
				ctx,
				job.ID,
				fmt.Sprintf(
					"persist validation batch starting at record %d: %v",
					start+1,
					err,
				),
			)
		}

		totalValid += response.ValidRecords
		totalInvalid += response.InvalidRecords
		start = end

		log.Printf(
			"job %d validation progress: processed=%d/%d valid=%d invalid=%d",
			job.ID,
			start,
			len(requestRecords),
			totalValid,
			totalInvalid,
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	errorMessage := strings.Join(errorMessages, "; ")

	// Advance to validated only after every batch has been persisted.
	err = w.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&models.Job{}).
			Where("id = ? AND status = ?", job.ID, "processing").
			Updates(map[string]interface{}{
				"status":        "validated",
				"error_message": nullableMessage(errorMessage),
			})

		if result.Error != nil {
			return fmt.Errorf("finalize validation status: %w", result.Error)
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
		if ctx.Err() != nil {
			return w.failJob(ctx, job.ID, ctx.Err().Error())
		}

		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("finalize validation: %v", err),
		)
	}

	log.Printf(
		"job %d validated: total=%d valid=%d invalid=%d",
		job.ID,
		len(requestRecords),
		totalValid,
		totalInvalid,
	)

	return nil
}

// validationBatchEnd finds a batch boundary without exceeding the
// record count or encoded request-body size limits.
func validationBatchEnd(
	jobID uint,
	records []map[string]string,
	start int,
) (int, error) {
	if start < 0 || start >= len(records) {
		return 0, fmt.Errorf("invalid batch start index %d", start)
	}

	end := start + validationBatchSize
	if end > len(records) {
		end = len(records)
	}

	for {
		body, err := json.Marshal(ValidateRequest{
			JobID:   jobID,
			Records: records[start:end],
		})
		if err != nil {
			return 0, fmt.Errorf("encode validation batch: %w", err)
		}

		if len(body) <= maxValidationRequestBytes {
			return end, nil
		}

		if end-start == 1 {
			return 0, fmt.Errorf(
				"record %d exceeds the validation request size limit of %d bytes",
				start+1,
				maxValidationRequestBytes,
			)
		}

		end = start + (end-start)/2
	}
}

// validateRecords sends one bounded batch to the validation service.
func (w *JobWorker) validateRecords(
	ctx context.Context,
	jobID uint,
	records []map[string]string,
) (*ValidateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("cannot validate an empty batch")
	}

	validationURL := os.Getenv("VALIDATION_SERVICE_URL")
	if validationURL == "" {
		validationURL = "http://localhost:8081"
	}
	validationURL = strings.TrimRight(validationURL, "/") + "/validate"

	jsonBody, err := json.Marshal(ValidateRequest{
		JobID:   jobID,
		Records: records,
	})
	if err != nil {
		return nil, fmt.Errorf("encode validation request: %w", err)
	}

	if len(jsonBody) > maxValidationRequestBytes {
		return nil, fmt.Errorf(
			"validation request exceeds %d bytes",
			maxValidationRequestBytes,
		)
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
	req.Header.Set("Accept", "application/json")

	client := w.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: validationRequestTimeout,
		}
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

	// Read a bounded response and reject oversized or trailing JSON.
	responseBody, err := io.ReadAll(
		io.LimitReader(resp.Body, maxValidationResponseBytes+1),
	)
	if err != nil {
		return nil, fmt.Errorf("read validation response: %w", err)
	}
	if len(responseBody) > maxValidationResponseBytes {
		return nil, fmt.Errorf(
			"validation response exceeds %d bytes",
			maxValidationResponseBytes,
		)
	}

	var response ValidateResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("decode validation response: %w", err)
	}

	if response.JobID != jobID {
		return nil, fmt.Errorf(
			"validation response job ID mismatch: expected %d, got %d",
			jobID,
			response.JobID,
		)
	}

	if response.TotalRecords != len(records) ||
		len(response.Results) != response.TotalRecords {
		return nil, fmt.Errorf(
			"validation result count mismatch: submitted %d, reported total %d, returned results %d",
			len(records),
			response.TotalRecords,
			len(response.Results),
		)
	}

	if response.ValidRecords < 0 ||
		response.InvalidRecords < 0 ||
		response.ValidRecords+response.InvalidRecords != response.TotalRecords {
		return nil, fmt.Errorf("validation record counts are inconsistent")
	}

	return &response, nil
}

// validateBatchResults verifies that every submitted record has exactly
// one result, with a valid and unique batch-local record number.
func validateBatchResults(
	response *ValidateResponse,
	batchSize int,
) error {
	if response == nil {
		return fmt.Errorf("validation response is nil")
	}
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}

	seen := make([]bool, batchSize+1)
	validCount := 0
	invalidCount := 0

	for _, result := range response.Results {
		number := result.RecordNumber

		if number < 1 || number > batchSize {
			return fmt.Errorf(
				"record number %d is outside batch range 1-%d",
				number,
				batchSize,
			)
		}

		if seen[number] {
			return fmt.Errorf("duplicate result for record %d", number)
		}
		seen[number] = true

		if result.Record == nil {
			return fmt.Errorf("record %d has no record data", number)
		}

		if len(result.Errors) == 0 {
			validCount++
		} else {
			invalidCount++
		}
	}

	for number := 1; number <= batchSize; number++ {
		if !seen[number] {
			return fmt.Errorf("missing result for record %d", number)
		}
	}

	if validCount != response.ValidRecords ||
		invalidCount != response.InvalidRecords {
		return fmt.Errorf(
			"validation counts do not match results: results valid=%d invalid=%d; reported valid=%d invalid=%d",
			validCount,
			invalidCount,
			response.ValidRecords,
			response.InvalidRecords,
		)
	}

	return nil
}

// persistValidationBatch saves validation records and advances progress
// atomically, so counters cannot advance without the corresponding records.
func (w *JobWorker) persistValidationBatch(
	ctx context.Context,
	jobID uint,
	response *ValidateResponse,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	jobRecords := make([]models.JobRecord, 0, len(response.Results))

	for _, result := range response.Results {
		validationErrors := result.Errors
		if validationErrors == nil {
			validationErrors = []string{}
		}

		jobRecords = append(jobRecords, models.JobRecord{
			JobID:            jobID,
			RecordNumber:     result.RecordNumber,
			RecordData:       result.Record,
			IsValid:          len(validationErrors) == 0,
			ValidationErrors: validationErrors,
		})
	}

	return w.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(jobRecords) > 0 {
			if err := tx.CreateInBatches(&jobRecords, 500).Error; err != nil {
				return fmt.Errorf("save validation results: %w", err)
			}
		}

		result := tx.Model(&models.Job{}).
			Where("id = ? AND status = ?", jobID, "processing").
			Updates(map[string]interface{}{
				"processed_records": gorm.Expr(
					"processed_records + ?",
					len(jobRecords),
				),
				"valid_records": gorm.Expr(
					"valid_records + ?",
					response.ValidRecords,
				),
				"invalid_records": gorm.Expr(
					"invalid_records + ?",
					response.InvalidRecords,
				),
			})

		if result.Error != nil {
			return fmt.Errorf("update validation progress: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf(
				"job %d was not found or is no longer processing",
				jobID,
			)
		}

		return nil
	})
}

// nullableMessage returns nil when there is no error message.
func nullableMessage(message string) interface{} {
	if message == "" {
		return nil
	}
	return message
}

// failJob reports the error and marks a processing job as failed.
// A bounded cleanup context allows the database update after cancellation.
func (w *JobWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		ingestionCleanupTimeout,
	)
	defer cancel()

	if w.ErrorCollector != nil {
		err := w.ErrorCollector.Report(
			cleanupCtx,
			pipelineerrors.ErrorEvent{
				JobID:   jobID,
				Stage:   "ingestion",
				Message: message,
			},
		)

		if err != nil {
			log.Printf(
				"Failed to report ingestion error for job %d: %v",
				jobID,
				err,
			)
		}
	}

	result := w.DB.WithContext(cleanupCtx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "processing").
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
		})

	if result.Error != nil {
		return fmt.Errorf(
			"job %d failed (%s); unable to save failure status: %w",
			jobID,
			message,
			result.Error,
		)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d could not be marked failed: %s",
			jobID,
			message,
		)
	}

	log.Printf("Job %d ingestion failed: %s", jobID, message)

	return fmt.Errorf("job %d ingestion failed: %s", jobID, message)
}
