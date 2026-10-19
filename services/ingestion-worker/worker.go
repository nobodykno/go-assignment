
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"go-assignment/packages/models"
	"go-assignment/services/ingestion"

	"gorm.io/gorm"
)

type JobWorker struct {
	DB *gorm.DB
}

// Run polls PostgreSQL until the context is cancelled.
func (w *JobWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := w.processPendingJob(); err != nil {
			log.Printf("ingestion worker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("ingestion worker stopped")
			return
		case <-ticker.C:
		}
	}
}

// processPendingJob fetches and ingests one pending job.
func (w *JobWorker) processPendingJob() error {
	var job models.Job

	err := w.DB.
		Where("status = ?", "pending").
		Order("created_at ASC").
		First(&job).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	// Claim the job only if it is still pending.
	result := w.DB.Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, "pending").
		Updates(map[string]interface{}{
			"status":        "processing",
			"error_message": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}

	if job.FilePath == "" {
		return w.failJob(job.ID, "job has no uploaded file path")
	}

	records, err := ingestion.ReadFile(job.FilePath)
	if err != nil {
		return w.failJob(job.ID, err.Error())
	}

	return w.DB.Model(&models.Job{}).
		Where("id = ?", job.ID).
		Updates(map[string]interface{}{
			"status":        "ingested",
			"total_records": len(records),
			"error_message": nil,
		}).Error
}

func (w *JobWorker) failJob(jobID uint, message string) error {
	return w.DB.Model(&models.Job{}).
		Where("id = ?", jobID).
		Updates(map[string]interface{}{
			"status":        "failed",
			"error_message": message,
		}).Error
}
