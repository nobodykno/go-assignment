
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
    pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type AggregationWorker struct {
	DB *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

// Run continuously checks for transformed jobs until cancelled.
func (w *AggregationWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("Aggregation worker started")

	for {
		if err := ctx.Err(); err != nil {
			log.Println("Aggregation worker stopped")
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			if (errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded)) &&
				ctx.Err() != nil {
				log.Println("Aggregation worker stopped")
				return
			}

			log.Printf("Aggregation worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("Aggregation worker stopped")
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims and aggregates the oldest transformed job.
func (w *AggregationWorker) processPendingJob(
	ctx context.Context,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var job models.Job

	err := w.DB.WithContext(ctx).
		Where("status = ?", "transformed").
		Order("created_at ASC").
		First(&job).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("find transformed job: %w", err)
	}

	// Atomically claim the job so only one worker can process it.
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transformed").
		Update("status", "aggregating")

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}

	if claim.RowsAffected == 0 {
		return nil
	}

	log.Printf("Started aggregation for job %d", job.ID)

	// Validate the input after claiming so failJob can update its status.
	if job.TransformedFilePath == "" {
		return w.failJob(
			ctx,
			job.ID,
			"transformed file path is empty",
		)
	}

	info, err := os.Stat(job.TransformedFilePath)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("inspect transformed file: %v", err),
		)
	}

	if !info.Mode().IsRegular() {
		return w.failJob(
			ctx,
			job.ID,
			"transformed path is not a regular file",
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	outputDir := os.Getenv("AGGREGATED_OUTPUT_DIR")
	if outputDir == "" {
		outputDir = "./output"
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("create aggregation output directory: %v", err),
		)
	}

	outputPath := filepath.Join(
		outputDir,
		fmt.Sprintf("job-%d-aggregated.json", job.ID),
	)

	// AggregateFile reads the transformed JSON and writes aggregated JSON.
	if err := AggregateFile(
		job.TransformedFilePath,
		outputPath,
	); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("aggregate records: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Save the output path and advance to the export stage.
	update := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "aggregating").
		Updates(map[string]interface{}{
			"status":               "aggregated",
			"aggregated_file_path": outputPath,
			"error_message":        nil,
		})

	if update.Error != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("save aggregated status: %v", update.Error),
		)
	}

	if update.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d is no longer aggregating; aggregation output: %s",
			job.ID,
			outputPath,
		)
	}

	log.Printf(
		"Job %d aggregation completed: output=%s",
		job.ID,
		outputPath,
	)

	return nil
}

// failJob marks a currently aggregating job as failed.
// A bounded cleanup context permits the DB update after cancellation.

func (w *AggregationWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()

	// Report the aggregation-stage error.
	if w.ErrorCollector != nil {
		err := w.ErrorCollector.Report(
			cleanupCtx,
			pipelineerrors.ErrorEvent{
				JobID:   jobID,
				Stage:   "aggregation",
				Message: message,
			},
		)

		if err != nil {
			log.Printf(
				"Failed to report aggregation error for job %d: %v",
				jobID,
				err,
			)
		}
	}

	// Preserve the existing job failure update.
	result := w.DB.WithContext(cleanupCtx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "aggregating").
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

	log.Printf("Job %d aggregation failed: %s", jobID, message)

	return fmt.Errorf("job %d aggregation failed: %s", jobID, message)
}
