
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
	pipelineerrors "go-assignment/packages/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ExportWorker struct {
	DB *gorm.DB
	ErrorCollector *pipelineerrors.Collector
}

// Run continuously checks for aggregated jobs until cancelled.
func (w *ExportWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("Export worker started")

	for {
		if err := ctx.Err(); err != nil {
			log.Println("Export worker stopped")
			return
		}

		if err := w.processPendingJob(ctx); err != nil {
			if (errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded)) &&
				ctx.Err() != nil {
				log.Println("Export worker stopped")
				return
			}

			log.Printf("Export worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("Export worker stopped")
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

	claim := w.DB.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "aggregated").
		Update("status", "exporting")

	if claim.Error != nil {
		return fmt.Errorf("claim job %d: %w", job.ID, claim.Error)
	}

	if claim.RowsAffected == 0 {
		return nil
	}

	log.Printf("Started export for job %d", job.ID)

	if job.AggregatedFilePath == "" {
		return w.failJob(
			ctx,
			job.ID,
			"aggregated file path is empty",
		)
	}

	info, err := os.Stat(job.AggregatedFilePath)
	if err != nil {
		return w.failJob(
			ctx,
			job.ID,
			fmt.Sprintf("inspect aggregated file: %v", err),
		)
	}

	if !info.Mode().IsRegular() {
		return w.failJob(
			ctx,
			job.ID,
			"aggregated path is not a regular file",
		)
	}

	if err := ctx.Err(); err != nil {
		return w.failJob(ctx, job.ID, err.Error())
	}

	outputDir := os.Getenv("EXPORTED_OUTPUT_DIR")
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

	reports, err := ExportFile(
		job.AggregatedFilePath,
		outputPath,
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


func (w *ExportWorker) failJob(
	ctx context.Context,
	jobID uint,
	message string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()

	// Report the export-stage error.
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

	// Preserve the existing job failure update.
	result := w.DB.WithContext(cleanupCtx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", jobID, "exporting").
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

	log.Printf("Job %d export failed: %s", jobID, message)

	return fmt.Errorf("job %d export failed: %s", jobID, message)
}

