package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/logger"
	"go-assignment/packages/metrics"
	"go-assignment/packages/models"
	"go-assignment/packages/worker"
	"go-assignment/services/ingestion"

	"gorm.io/gorm"
)

const (
	validationBatchSize        = 500
	maxValidationRequestBytes  = 4 << 20 // 4 MiB
	maxValidationResponseBytes = 4 << 20 // 4 MiB
	validationRequestTimeout   = 35 * time.Second
	ingestionCleanupTimeout    = 5 * time.Second
)

type JobWorker struct {
	DB             *gorm.DB
	HTTPClient     *http.Client
	ErrorCollector *pipelineerrors.Collector
}

// maxStoredErrorMessages caps how many validation errors are copied into
// the job's error_message; every error is still stored per record.
const maxStoredErrorMessages = 100

// Stage describes the ingestion stage for the shared worker runtime.
// Claiming resets the counters so a requeued job starts from zero.
func (w *JobWorker) Stage() worker.Stage {
	return worker.Stage{
		Name:         "ingestion",
		FromStatus:   "pending",
		ActiveStatus: "processing",
		ClaimUpdates: map[string]interface{}{
			"total_records":     0,
			"processed_records": 0,
			"valid_records":     0,
			"invalid_records":   0,
		},
		Process: w.processJob,
	}
}

// validationSummary accumulates results across all batches of a job.
type validationSummary struct {
	valid    int
	invalid  int
	messages []string
	omitted  int
}

func (s *validationSummary) addError(message string) {
	if len(s.messages) < maxStoredErrorMessages {
		s.messages = append(s.messages, message)
		return
	}
	s.omitted++
}

func (s *validationSummary) errorMessage() string {
	message := strings.Join(s.messages, "; ")
	if s.omitted > 0 {
		message += fmt.Sprintf("; and %d more validation error(s)", s.omitted)
	}
	return message
}

// processJob streams a claimed job's input through the validation
// service in bounded batches and moves the job to validated. Records are
// never all held in memory: the input is read once to count records (for
// progress) and once more to validate them.
func (w *JobWorker) processJob(ctx context.Context, job *models.Job) error {
	log := logger.FromContext(ctx)

	stream, err := w.openStream(ctx, job)
	if err != nil {
		return w.failInput(ctx, job.ID, err)
	}

	total, err := stream.Count(ctx)
	if err != nil {
		return w.failInput(ctx, job.ID, err)
	}
	if total == 0 {
		return w.failJob(ctx, job.ID, "input sources contain no records")
	}

	// Set the total up front so clients can calculate progress.
	result := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "processing").
		Update("total_records", total)

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

	var summary validationSummary
	processed := 0
	batch := make([]models.RawRecord, 0, validationBatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := w.validateBatch(ctx, job.ID, batch, processed, &summary); err != nil {
			return err
		}

		processed += len(batch)
		batch = batch[:0]

		log.Info(
			"validation progress",
			"processed", processed,
			"total", total,
			"valid", summary.valid,
			"invalid", summary.invalid,
		)
		return nil
	}

	err = stream.Each(ctx, func(record models.RawRecord) error {
		batch = append(batch, record)
		if len(batch) >= validationBatchSize {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		if ctx.Err() != nil {
			return w.failJob(ctx, job.ID, ctx.Err().Error())
		}
		return w.failJob(ctx, job.ID, err.Error())
	}

	if processed != total {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("input changed while processing: counted %d records, read %d", total, processed),
		)
	}

	// Advance to validated only after every batch has been persisted.
	result = w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "processing").
		Updates(map[string]interface{}{
			"status":        "validated",
			"error_message": nullableMessage(summary.errorMessage()),
		})

	if result.Error != nil {
		if ctx.Err() != nil {
			return w.failJob(ctx, job.ID, ctx.Err().Error())
		}
		return w.failJob(ctx, job.ID, fmt.Sprintf("finalize validation: %v", result.Error))
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("job %d was not found or is no longer processing", job.ID)
	}

	metrics.RecordsProcessed.WithLabelValues("ingestion").Add(float64(processed))

	log.Info(
		"validation completed",
		"record_count", processed,
		"valid", summary.valid,
		"invalid", summary.invalid,
	)

	return nil
}

// openStream opens the job's input: its configured sources for mixed
// jobs, otherwise the single uploaded file.
func (w *JobWorker) openStream(ctx context.Context, job *models.Job) (*ingestion.Stream, error) {
	if job.SourceType == "mixed" {
		return ingestion.OpenSources(ctx, job.SourceConfig.Sources, w.HTTPClient)
	}

	if strings.TrimSpace(job.FilePath) == "" {
		return nil, errors.New("job has no uploaded file path")
	}

	return ingestion.OpenFile(job.FilePath), nil
}

// failInput marks the job failed because its input could not be read.
func (w *JobWorker) failInput(ctx context.Context, jobID uint, err error) error {
	if ctx.Err() != nil {
		return w.failJob(ctx, jobID, ctx.Err().Error())
	}
	return w.failJob(ctx, jobID, fmt.Sprintf("read input records: %v", err))
}

// validateBatch validates and persists one streamed batch. offset is the
// number of records already processed, used to number records globally.
// The batch is split further if its encoded size exceeds the request limit.
func (w *JobWorker) validateBatch(
	ctx context.Context,
	jobID uint,
	batch []models.RawRecord,
	offset int,
	summary *validationSummary,
) error {
	for start := 0; start < len(batch); {
		if err := ctx.Err(); err != nil {
			return err
		}

		end, err := validationBatchEnd(jobID, batch, start)
		if err != nil {
			return err
		}

		firstRecord := offset + start + 1
		chunk := batch[start:end]

		response, err := w.validateRecords(ctx, jobID, chunk)
		if err != nil {
			return fmt.Errorf("validate batch starting at record %d: %w", firstRecord, err)
		}

		// Verify each result has a unique, valid batch-local number.
		if err := validateBatchResults(response, len(chunk)); err != nil {
			return fmt.Errorf(
				"invalid validation response for batch starting at record %d: %w",
				firstRecord,
				err,
			)
		}

		// Convert batch-local numbers into positions in the whole input.
		for i := range response.Results {
			response.Results[i].RecordNumber += offset + start

			for _, validationError := range response.Results[i].Errors {
				summary.addError(fmt.Sprintf(
					"record %d: %s",
					response.Results[i].RecordNumber,
					validationError,
				))
			}
		}

		if err := w.persistValidationBatch(ctx, jobID, response); err != nil {
			return fmt.Errorf("persist validation batch starting at record %d: %w", firstRecord, err)
		}

		summary.valid += response.ValidRecords
		summary.invalid += response.InvalidRecords
		start = end
	}

	return nil
}

// validationBatchEnd finds a batch boundary without exceeding the
// record count or encoded request-body size limits.
func validationBatchEnd(
	jobID uint,
	records []models.RawRecord,
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
		body, err := json.Marshal(models.ValidateRequest{
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
	records []models.RawRecord,
) (*models.ValidateResponse, error) {
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

	jsonBody, err := json.Marshal(models.ValidateRequest{
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

	var response models.ValidateResponse
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
	response *models.ValidateResponse,
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
	response *models.ValidateResponse,
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
			logger.FromContext(ctx).Warn("report pipeline error failed", "error", err)
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

	return fmt.Errorf("job %d ingestion failed: %s", jobID, message)
}
