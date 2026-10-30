package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/jobs"
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type TransformationWorker struct {
	DB             *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

const transformationPollInterval = 2 * time.Second
const transformationCleanupTimeout = 5 * time.Second

// Run continuously checks for validated jobs until the context is cancelled.
func (w *TransformationWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(transformationPollInterval)
	defer ticker.Stop()

	log.Println("Transformation worker started")
	defer log.Println("Transformation worker stopped")

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			if ctx.Err() != nil &&
				(errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded)) {
				return
			}

			log.Printf("Transformation worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims and processes the oldest validated job.
func (w *TransformationWorker) processPendingJob(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.DB == nil {
		return errors.New("transformation worker database is not configured")
	}

	var job models.Job

	err := w.DB.WithContext(ctx).
		Where("status = ?", "validated").
		Order("created_at ASC").
		First(&job).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("find validated job: %w", err)
	}

	// Atomically claim the job so another service instance cannot
	// claim it while its status is still validated.
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "validated").
		Updates(map[string]interface{}{
			"status":        "transforming",
			"error_message": nil,
			"heartbeat_at":  time.Now().UTC(),
		})

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}

	if claim.RowsAffected == 0 {
		return nil
	}

	log.Printf("Started transformation for job %d", job.ID)

	// Keep the heartbeat fresh while this worker owns the job.
	stopHeartbeat := jobs.StartHeartbeat(
		ctx,
		w.DB,
		job.ID,
		"transforming",
		jobs.DefaultHeartbeatInterval,
	)
	defer stopHeartbeat()

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Load only records that passed validation.
	var records []models.JobRecord

	err = w.DB.WithContext(ctx).
		Where("job_id = ? AND is_valid = ?", job.ID, true).
		Order("record_number ASC").
		Find(&records).Error

	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("load records: %v", err),
		)
	}

	if len(records) == 0 {
		return w.failJob(
			ctx,
			job.ID,
			"no valid records available for transformation",
		)
	}

	// Configure the number of concurrent transformation workers.
	workerCount := 5

	if value := os.Getenv("TRANSFORMATION_WORKERS"); value != "" {
		parsed, parseErr := strconv.Atoi(value)
		if parseErr != nil || parsed < 1 {
			log.Printf(
				"Invalid TRANSFORMATION_WORKERS value %q; using %d workers",
				value,
				workerCount,
			)
		} else {
			workerCount = parsed
		}
	}

	// Never start more workers than there are records.
	if workerCount > len(records) {
		workerCount = len(records)
	}

	type transformResult struct {
		index       int
		transformed TransformedRecord
		err         error
	}

	recordJobs := make(chan int, len(records))
	results := make(chan transformResult, len(records))

	for i := range records {
		recordJobs <- i
	}
	close(recordJobs)

	var wg sync.WaitGroup

	// Start the transformation worker pool.
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				if ctx.Err() != nil {
					return
				}

				select {
				case <-ctx.Done():
					return

				case index, ok := <-recordJobs:
					if !ok {
						return
					}

					if ctx.Err() != nil {
						return
					}

					transformed, transformErr := TransformRecord(
						Record(records[index].RecordData),
					)

					result := transformResult{
						index:       index,
						transformed: transformed,
						err:         transformErr,
					}

					select {
					case results <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	// Close results only after every worker has finished.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Store results by original index to preserve record ordering.
	transformedByIndex := make([]TransformedRecord, len(records))
	transformErrors := make([]error, len(records))
	processed := make([]bool, len(records))

	for result := range results {
		transformedByIndex[result.index] = result.transformed
		transformErrors[result.index] = result.err
		processed[result.index] = true
	}

	// Do not write partial output if the job was cancelled.
	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	transformedRecords := make(
		[]TransformedRecord,
		0,
		len(records),
	)

	transformationFailures := 0

	// Collect results in original order and persist transformation errors.
	for i := range records {
		if !processed[i] {
			return w.failJob(
				ctx,
				job.ID,
				fmt.Sprintf(
					"record %d was not processed due to an incomplete worker result",
					records[i].RecordNumber,
				),
			)
		}

		if transformErrors[i] != nil {
			transformationFailures++

			message := fmt.Sprintf(
				"transformation failed: %v",
				transformErrors[i],
			)

			records[i].ValidationErrors = append(
				records[i].ValidationErrors,
				message,
			)

			updateRecord := w.DB.WithContext(ctx).
				Model(&models.JobRecord{}).
				Where(
					"id = ? AND job_id = ?",
					records[i].ID,
					job.ID,
				).
				Update(
					"validation_errors",
					records[i].ValidationErrors,
				)

			if updateRecord.Error != nil {
				return w.failJob(
					ctx,
					job.ID,
					fmt.Sprintf(
						"save transformation error for record %d: %v",
						records[i].RecordNumber,
						updateRecord.Error,
					),
				)
			}

			log.Printf(
				"Job %d: record %d transformation failed: %v",
				job.ID,
				records[i].RecordNumber,
				transformErrors[i],
			)

			continue
		}

		transformedRecords = append(
			transformedRecords,
			transformedByIndex[i],
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	if len(transformedRecords) == 0 {
		return w.failJob(
			ctx,
			job.ID,
			"no records could be transformed",
		)
	}

	// Preserve a summary when some records fail transformation.
	var errorSummary *string

	if transformationFailures > 0 {
		summary := fmt.Sprintf(
			"%d record(s) failed transformation",
			transformationFailures,
		)
		errorSummary = &summary
	}

	summaryUpdate := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transforming").
		Update("error_message", errorSummary)

	if summaryUpdate.Error != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf(
				"save transformation summary: %v",
				summaryUpdate.Error,
			),
		)
	}

	if summaryUpdate.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d is no longer transforming",
			job.ID,
		)
	}

	// Write transformed records to the configured output directory.
	outputDir := strings.TrimSpace(os.Getenv("TRANSFORMED_OUTPUT_DIR"))
	if outputDir == "" {
		outputDir = "./output"
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("create output directory: %v", err),
		)
	}

	outputPath := filepath.Join(
		outputDir,
		fmt.Sprintf("job-%d-transformed.json", job.ID),
	)

	// Write to a temporary file so the final output is not exposed
	// while the JSON is being generated.
	tempFile, err := os.CreateTemp(
		outputDir,
		fmt.Sprintf("job-%d-*.tmp", job.ID),
	)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("create temporary output file: %v", err),
		)
	}

	tempPath := tempFile.Name()

	defer func() {
		if err := os.Remove(tempPath); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			log.Printf(
				"Failed to remove temporary transformation output for job %d: %v",
				job.ID,
				err,
			)
		}
	}()

	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(transformedRecords); err != nil {
		_ = tempFile.Close()

		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("encode transformed records: %v", err),
		)
	}

	if err := tempFile.Close(); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("close temporary output file: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	if err := os.Rename(tempPath, outputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("finalize transformed output: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Complete the stage and clear the heartbeat.
	finalUpdate := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transforming").
		Updates(map[string]interface{}{
			"status":                "transformed",
			"transformed_file_path": outputPath,
			"heartbeat_at":          nil,
		})

	if finalUpdate.Error != nil {
		return fmt.Errorf(
			"save transformed status for job %d: %w",
			job.ID,
			finalUpdate.Error,
		)
	}

	if finalUpdate.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d is no longer transforming",
			job.ID,
		)
	}

	log.Printf(
		"Job %d transformation completed: output_records=%d, transformation_failures=%d, workers=%d, output=%s",
		job.ID,
		len(transformedRecords),
		transformationFailures,
		workerCount,
		outputPath,
	)

	return nil
}

// failJob marks a transforming job as failed and clears its heartbeat.
// A bounded cleanup context permits the DB update after cancellation.
func (w *TransformationWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		transformationCleanupTimeout,
	)
	defer cancel()

	if w.ErrorCollector != nil {
		err := w.ErrorCollector.Report(
			cleanupCtx,
			pipelineerrors.ErrorEvent{
				JobID:   jobID,
				Stage:   "transformation",
				Message: message,
			},
		)

		if err != nil {
			log.Printf(
				"Failed to report transformation error for job %d: %v",
				jobID,
				err,
			)
		}
	}

	if w.DB == nil {
		return fmt.Errorf(
			"job %d failed (%s); database is not configured",
			jobID,
			message,
		)
	}

	result := w.DB.WithContext(cleanupCtx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "transforming").
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
			"heartbeat_at":  nil,
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
			"job %d could not be marked failed because it is no longer transforming: %s",
			jobID,
			message,
		)
	}

	log.Printf("Job %d marked as failed: %s", jobID, message)

	return fmt.Errorf("job %d transformation failed: %s", jobID, message)
}
