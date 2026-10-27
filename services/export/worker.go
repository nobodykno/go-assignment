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
	"gorm.io/gorm/clause"
)

type ExportWorker struct {
	DB             *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

const exportPollInterval = 2 * time.Second
const exportCleanupTimeout = 5 * time.Second

// Run continuously checks for aggregated jobs until cancelled.
func (w *ExportWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(exportPollInterval)
	defer ticker.Stop()

	log.Println("Export worker started")
	defer log.Println("Export worker stopped")

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

			log.Printf("Export worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob claims the oldest aggregated job and exports it.
func (w *ExportWorker) processPendingJob(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.DB == nil {
		return errors.New("export worker database is not configured")
	}

	var job models.Job

	err := w.DB.WithContext(ctx).
		Where("status = ?", "aggregated").
		Order("created_at ASC").
		First(&job).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("find aggregated job: %w", err)
	}

	// Atomically claim the job so only one worker exports it.
	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "aggregated").
		Updates(map[string]interface{}{
			"status":        "exporting",
			"error_message": nil,
			"heartbeat_at":  time.Now().UTC(),
		})

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}

	if claim.RowsAffected == 0 {
		return nil
	}

	log.Printf("Started export for job %d", job.ID)

	// Keep heartbeat_at updated while this worker owns the job.
	stopHeartbeat := jobs.StartHeartbeat(
		ctx,
		w.DB,
		job.ID,
		"exporting",
		jobs.DefaultHeartbeatInterval,
	)
	defer stopHeartbeat()

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
			log.Printf(
				"Failed to remove temporary export file for job %d: %v",
				job.ID,
				err,
			)
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

	log.Printf(
		"Job %d export completed: %d report records, output=%s",
		job.ID,
		len(reports),
		outputPath,
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
			log.Printf(
				"Failed to report export error for job %d: %v",
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

	log.Printf("Job %d export failed: %s", jobID, message)

	return fmt.Errorf("job %d export failed: %s", jobID, message)
}
