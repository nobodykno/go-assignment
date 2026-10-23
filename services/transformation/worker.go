
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

// Run continuously looks for validated jobs until the context is cancelled.
func (w *TransformationWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("transformation worker started")

	for {
		if err := ctx.Err(); err != nil {
			log.Println("transformation worker stopped")
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			// Do not log cancellation as a processing error.
			if ctx.Err() != nil {
				log.Println("transformation worker stopped")
				return
			}

			log.Printf("transformation worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("transformation worker stopped")
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims and transforms one validated job.
func (w *TransformationWorker) processPendingJob(
	ctx context.Context,
) error {
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

	// Claim the job only if it is still validated.
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
			fmt.Sprintf("load valid records: %v", err),
		)
	}

	transformedRecords := make(
		[]TransformedRecord,
		0,
		len(records),
	)

	// Transform every valid record.
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return w.failJob(ctx, job.ID, err.Error())
		}

		transformed, err := TransformRecord(
			Record(record.RecordData),
		)
		if err != nil {
			return w.failJob(
				ctx,
				job.ID,
				fmt.Sprintf(
					"record %d transformation failed: %v",
					record.RecordNumber,
					err,
				),
			)
		}

		transformedRecords = append(
			transformedRecords,
			transformed,
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Choose the output directory.
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

	// Build the output filename for this job.
	outputPath := filepath.Join(
		outputDir,
		fmt.Sprintf("job-%d-transformed.json", job.ID),
	)

	// Write to a temporary file before publishing the result.
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
	tempFileClosed := false

	// Ensure the temporary file is removed if processing fails.
	defer func() {
		if !tempFileClosed {
			_ = tempFile.Close()
		}
		_ = os.Remove(tempPath)
	}()

	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(transformedRecords); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("encode transformed records: %v", err),
		)
	}

	if err := tempFile.Close(); err != nil {
		tempFileClosed = true
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("close temporary output file: %v", err),
		)
	}
	tempFileClosed = true

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Publish the completed output file.
	if err := os.Rename(tempPath, outputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("publish transformed output: %v", err),
		)
	}

	// Save the output path and mark the job as transformed.
	result := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transforming").
		Updates(map[string]interface{}{
			"status":                "transformed",
			"transformed_file_path": outputPath,
			"error_message":         nil,
		})

	if result.Error != nil {
		return fmt.Errorf(
			"save transformation result for job %d: %w",
			job.ID,
			result.Error,
		)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d is no longer transforming",
			job.ID,
		)
	}

	log.Printf(
		"job %d transformed successfully: records=%d output=%s",
		job.ID,
		len(transformedRecords),
		outputPath,
	)

	return nil
}

// failJob marks a job as failed.
func (w *TransformationWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	result := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "transforming").
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
		})

	if result.Error != nil {
		return fmt.Errorf(
			"mark job %d as failed: %w",
			jobID,
			result.Error,
		)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf(
			"could not mark job %d as failed: job is no longer transforming",
			jobID,
		)
	}

	log.Printf("job %d marked as failed: %s", jobID, message)

	return nil
}
