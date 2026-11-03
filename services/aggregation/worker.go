package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/logger"
	"go-assignment/packages/models"
	"go-assignment/packages/worker"

	"gorm.io/gorm"
)

type AggregationWorker struct {
	DB             *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

const aggregationCleanupTimeout = 5 * time.Second

// Stage describes the aggregation stage for the shared worker runtime,
// which claims jobs, keeps their heartbeat and records metrics.
func (w *AggregationWorker) Stage() worker.Stage {
	return worker.Stage{
		Name:         "aggregation",
		FromStatus:   "transformed",
		ActiveStatus: "aggregating",
		Process:      w.processJob,
	}
}

// processJob aggregates one claimed job and moves it to aggregated.
func (w *AggregationWorker) processJob(ctx context.Context, job *models.Job) error {
	if w.DB == nil {
		return errors.New("aggregation worker database is not configured")
	}

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
			logger.FromContext(ctx).Warn("remove temporary output failed", "error", err)
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

	logger.FromContext(ctx).Info("aggregation output written", "output", outputPath)

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
			logger.FromContext(ctx).Warn("report pipeline error failed", "error", err)
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

	return fmt.Errorf("job %d aggregation failed: %s", jobID, message)
}
