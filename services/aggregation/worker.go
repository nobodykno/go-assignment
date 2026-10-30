package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/jobs"
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type AggregationWorker struct {
	DB             *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

const aggregationPollInterval = 2 * time.Second
const aggregationCleanupTimeout = 5 * time.Second

// Run continuously checks for transformed jobs until cancelled.
func (w *AggregationWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(aggregationPollInterval)
	defer ticker.Stop()

	log.Println("Aggregation worker started")
	defer log.Println("Aggregation worker stopped")

	for {
		if ctx.Err() != nil {
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			if ctx.Err() != nil &&
				(errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded)) {
				return
			}

			log.Printf("Aggregation worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims and aggregates the oldest transformed job.
func (w *AggregationWorker) processPendingJob(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.DB == nil {
		return errors.New("aggregation worker database is not configured")
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

	// Atomically claim the job. Only the worker that changes the status
	// from transformed to aggregating is allowed to process it.
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "transformed").
		Updates(map[string]interface{}{
			"status":        "aggregating",
			"error_message": nil,
			"heartbeat_at":  time.Now().UTC(),
		})

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}

	if claim.RowsAffected == 0 {
		return nil
	}

	log.Printf("Started aggregation for job %d", job.ID)

	// Refresh the heartbeat while the job remains in aggregating status.
	stopHeartbeat := jobs.StartHeartbeat(
		ctx,
		w.DB,
		job.ID,
		"aggregating",
		jobs.DefaultHeartbeatInterval,
	)
	defer stopHeartbeat()

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	inputPath := strings.TrimSpace(job.TransformedFilePath)
	if inputPath == "" {
		return w.failJob(ctx, job.ID, "transformed file path is empty")
	}

	// Reject symbolic links and non-regular input files.
	inputInfo, err := os.Lstat(inputPath)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("inspect transformed file: %v", err),
		)
	}

	if inputInfo.Mode()&os.ModeSymlink != 0 {
		return w.failJob(
			ctx,
			job.ID,
			"transformed input must not be a symbolic link",
		)
	}

	if !inputInfo.Mode().IsRegular() {
		return w.failJob(
			ctx,
			job.ID,
			"transformed path is not a regular file",
		)
	}

	outputDir := strings.TrimSpace(os.Getenv("AGGREGATED_OUTPUT_DIR"))
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

	// Use a unique temporary file so interrupted attempts do not expose
	// partially generated aggregation output.
	tempFile, err := os.CreateTemp(
		outputDir,
		fmt.Sprintf("job-%d-aggregated-*.tmp", job.ID),
	)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("create temporary aggregation output: %v", err),
		)
	}

	tempOutputPath := tempFile.Name()

	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempOutputPath)
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("close temporary aggregation output: %v", err),
		)
	}

	defer func() {
		if err := os.Remove(tempOutputPath); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			log.Printf(
				"Failed to remove temporary aggregation output for job %d: %v",
				job.ID,
				err,
			)
		}
	}()

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// AggregateFile currently has no context parameter, so cancellation
	// cannot interrupt the aggregation operation itself.
	if err := AggregateFile(inputPath, tempOutputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("aggregate records: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Verify the generated output before publishing it.
	outputInfo, err := os.Lstat(tempOutputPath)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("inspect aggregation output: %v", err),
		)
	}

	if outputInfo.Mode()&os.ModeSymlink != 0 ||
		!outputInfo.Mode().IsRegular() {
		return w.failJob(
			ctx,
			job.ID,
			"aggregation output is not a regular file",
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Publish the completed output. The temporary file is in the same
	// directory as the final file, allowing a same-filesystem rename.
	if err := os.Rename(tempOutputPath, outputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("publish aggregation output: %v", err),
		)
	}

	// Persist the output path and advance the job to the export stage.
	// The status condition prevents overwriting a job owned by another
	// process or already moved to another stage.
	update := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "aggregating").
		Updates(map[string]interface{}{
			"status":               "aggregated",
			"aggregated_file_path": outputPath,
			"error_message":        nil,
			"heartbeat_at":         nil,
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

// failJob reports the error and marks the currently aggregating job
// as failed. A bounded cleanup context allows the DB update to proceed
// even if the worker's original context has been cancelled.
func (w *AggregationWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		aggregationCleanupTimeout,
	)
	defer cancel()

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

	if w.DB == nil {
		return fmt.Errorf(
			"job %d failed (%s); database is not configured",
			jobID,
			message,
		)
	}

	result := w.DB.WithContext(cleanupCtx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "aggregating").
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
			"job %d could not be marked failed because it is no longer aggregating: %s",
			jobID,
			message,
		)
	}

	log.Printf("Job %d aggregation failed: %s", jobID, message)

	return fmt.Errorf("job %d aggregation failed: %s", jobID, message)
}
