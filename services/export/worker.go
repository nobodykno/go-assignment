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
	"go-assignment/packages/metrics"
	"go-assignment/packages/models"
	"go-assignment/packages/worker"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ExportWorker struct {
	DB             *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

const exportCleanupTimeout = 5 * time.Second

// Stage describes the export stage for the shared worker runtime,
// which claims jobs, keeps their heartbeat and records metrics.
func (w *ExportWorker) Stage() worker.Stage {
	return worker.Stage{
		Name:         "export",
		FromStatus:   "aggregated",
		ActiveStatus: "exporting",
		Process:      w.processJob,
	}
}

// processJob exports one claimed job and moves it to exported.
func (w *ExportWorker) processJob(ctx context.Context, job *models.Job) error {
	if w.DB == nil {
		return errors.New("export worker database is not configured")
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	inputPath := strings.TrimSpace(job.AggregatedFilePath)
	if inputPath == "" {
		return w.failJob(ctx, job.ID, "aggregated file path is empty")
	}

	// Reject symbolic links and non-regular input files.
	inputInfo, err := os.Lstat(inputPath)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("inspect aggregated file: %v", err),
		)
	}

	if inputInfo.Mode()&os.ModeSymlink != 0 {
		return w.failJob(
			ctx,
			job.ID,
			"aggregated input must not be a symbolic link",
		)
	}

	if !inputInfo.Mode().IsRegular() {
		return w.failJob(
			ctx,
			job.ID,
			"aggregated path is not a regular file",
		)
	}

	outputDir := strings.TrimSpace(os.Getenv("EXPORTED_OUTPUT_DIR"))
	if outputDir == "" {
		outputDir = "./output"
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("create export output directory: %v", err),
		)
	}

	outputPath := filepath.Join(
		outputDir,
		fmt.Sprintf("job-%d-final-report.json", job.ID),
	)

	// Create a unique temporary filename in the output directory.
	// ExportFile writes to this path; the final path is published only
	// after the export operation succeeds.
	tempFile, err := os.CreateTemp(
		outputDir,
		fmt.Sprintf("job-%d-final-report-*.tmp", job.ID),
	)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("create temporary export file: %v", err),
		)
	}

	tempOutputPath := tempFile.Name()

	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempOutputPath)
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("close temporary export file: %v", err),
		)
	}

	// ExportFile may need to create or truncate its destination.
	if err := os.Remove(tempOutputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("prepare temporary export path: %v", err),
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

	reports, err := ExportFile(
		inputPath,
		tempOutputPath,
		job.ID,
	)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("export report: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Verify the generated report before publishing it.
	outputInfo, err := os.Lstat(tempOutputPath)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("inspect exported file: %v", err),
		)
	}

	if outputInfo.Mode()&os.ModeSymlink != 0 ||
		!outputInfo.Mode().IsRegular() {
		return w.failJob(
			ctx,
			job.ID,
			"export output is not a regular file",
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Publish the complete output. The temporary file is in the same
	// directory, so the rename stays on the same filesystem.
	if err := os.Rename(tempOutputPath, outputPath); err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("publish export output: %v", err),
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	// Save report records and complete the job in one database transaction.
	// The unique report key makes retries safe for the database records.
	err = w.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, report := range reports {
			result := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "job_id"},
					{Name: "report_date"},
					{Name: "group_key"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"report_data",
					"updated_at",
				}),
			}).Create(&report)

			if result.Error != nil {
				return fmt.Errorf(
					"save report for date %s and group %s: %w",
					report.ReportDate.Format("2006-01-02"),
					report.GroupKey,
					result.Error,
				)
			}
		}

		result := tx.Model(&models.Job{}).
			Where("id = ? AND status = ?", job.ID, "exporting").
			Updates(map[string]interface{}{
				"status":             "exported",
				"exported_file_path": outputPath,
				"error_message":      nil,
				"heartbeat_at":       nil,
			})

		if result.Error != nil {
			return fmt.Errorf("update job status: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			return fmt.Errorf(
				"job %d is no longer exporting",
				job.ID,
			)
		}

		return nil
	})

	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("save export results: %v", err),
		)
	}

	metrics.RecordsProcessed.WithLabelValues("export").Add(float64(len(reports)))

	logger.FromContext(ctx).Info(
		"export report written",
		"record_count", len(reports),
		"output", outputPath,
	)

	return nil
}

// failJob reports the error and marks a currently exporting job as failed.
// The bounded cleanup context permits the database update after cancellation.
func (w *ExportWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		exportCleanupTimeout,
	)
	defer cancel()

	if w.ErrorCollector != nil {
		err := w.ErrorCollector.Report(
			cleanupCtx,
			pipelineerrors.ErrorEvent{
				JobID:   jobID,
				Stage:   "export",
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
		Where("id = ? AND status = ?", jobID, "exporting").
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
			"job %d could not be marked failed because it is no longer exporting: %s",
			jobID,
			message,
		)
	}

	return fmt.Errorf("job %d export failed: %s", jobID, message)
}
