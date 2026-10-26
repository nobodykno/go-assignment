
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type AggregationWorker struct {
	DB *gorm.DB
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

// processPendingJob claims the oldest transformed job and aggregates it.
func (w *AggregationWorker) processPendingJob(ctx context.Context) error {
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

	if job.TransformedFilePath == "" {
		return w.failJob(
			ctx,
			job.ID,
			"transformed file path is empty",
		)
	}

	// Atomically claim the job to prevent multiple workers processing it.
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

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	outputDir := os.Getenv("AGGREGATED_OUTPUT_DIR")
	if outputDir == "" {
		outputDir = "./output"
	}

	outputPath := filepath.Join(
		outputDir,
		fmt.Sprintf("job-%d-aggregated.json", job.ID),
	)

	// AggregateFile reads the transformed JSON and writes aggregated JSON.
	if err := AggregateFile(job.TransformedFilePath, outputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("aggregate records: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Store the result path and advance the job to the export stage.
	// The aggregated_file_path column must exist in the database.
	update := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "aggregating").
		Updates(map[string]interface{}{
			"status":               "aggregated",
			"aggregated_file_path": outputPath,
			"error_message":        nil,
		})

	if update.Error != nil {
		return fmt.Errorf(
			"save aggregated status for job %d: %w",
			job.ID,
			update.Error,
		)
	}

	if update.RowsAffected == 0 {
		return fmt.Errorf(
			"job %d is no longer aggregating",
			job.ID,
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
// A bounded cleanup context allows a database update after cancellation.
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
