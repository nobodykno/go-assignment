
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type TransformationWorker struct {
	DB *gorm.DB
}

// Run continuously checks for validated jobs until the context is cancelled.
func (w *TransformationWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("Transformation worker started")

	for {
		if err := ctx.Err(); err != nil {
			log.Println("Transformation worker stopped")
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					log.Println("Transformation worker stopped")
					return
				}
			}

			log.Printf("Transformation worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("Transformation worker stopped")
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims and processes the oldest validated job.
func (w *TransformationWorker) processPendingJob(ctx context.Context) error {
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

	// Claim the job atomically so another worker cannot claim it
	// after its status has already changed.
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "validated").
		Update("status", "transforming")

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}

	if claim.RowsAffected == 0 {
		// Another worker may already have claimed this job.
		return nil
	}

	log.Printf("Started transformation for job %d", job.ID)

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

	transformedRecords := make(
		[]TransformedRecord,
		0,
		len(records),
	)

	transformationFailures := 0

	// Transform records individually. A single bad record should not
	// prevent other valid records from being transformed.
	for i := range records {
		if err := ctx.Err(); err != nil {
			return w.failJob(ctx, job.ID, err.Error())
		}

		transformed, transformErr := TransformRecord(
			Record(records[i].RecordData),
		)

		if transformErr != nil {
			transformationFailures++

			message := fmt.Sprintf(
				"transformation failed: %v",
				transformErr,
			)

			// Preserve existing validation errors and append the
			// transformation error. IsValid remains unchanged.
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
				transformErr,
			)

			continue
		}

		transformedRecords = append(
			transformedRecords,
			transformed,
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
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

	// Use a map so GORM writes error_message even when its value is nil.
	summaryUpdate := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transforming").
		Updates(map[string]interface{}{
			"error_message": errorSummary,
		})

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
	outputDir := os.Getenv("TRANSFORMED_OUTPUT_DIR")
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

	// Write to a temporary file first so the final output is not
	// exposed as a partially written JSON file.
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
		_ = os.Remove(tempPath)
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

	// Mark the job as transformed. Do not clear error_message here:
	// it may contain the partial-transformation failure summary.
	finalUpdate := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transforming").
		Updates(map[string]interface{}{
			"status":                "transformed",
			"transformed_file_path": outputPath,
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
		"Job %d transformation completed: output_records=%d, transformation_failures=%d, output=%s",
		job.ID,
		len(transformedRecords),
		transformationFailures,
		outputPath,
	)

	return nil
}

// failJob marks a transforming job as failed.
//
// A separate bounded context allows the failure status to be saved
// even if the original processing context has been cancelled.
func (w *TransformationWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()

	result := w.DB.WithContext(cleanupCtx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "transforming").
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
		})

	if result.Error != nil {
		return fmt.Errorf(
			"job %d failed (%s); also failed to save failure status: %w",
			jobID,
			message,
			result.Error,
		)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d could not be marked failed; it may no longer be transforming: %s",
			jobID,
			message,
		)
	}

	log.Printf("Job %d marked as failed: %s", jobID, message)

	return fmt.Errorf("job %d transformation failed: %s", jobID, message)
}
