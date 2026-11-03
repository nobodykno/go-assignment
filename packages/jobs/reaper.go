package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go-assignment/packages/metrics"
	"go-assignment/packages/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Reaper defaults. StaleAfter must comfortably exceed the heartbeat
// interval so a slow but healthy worker is not treated as dead.
const (
	DefaultStaleAfter     = 60 * time.Second
	DefaultMaxRetries     = 3
	DefaultReaperInterval = 15 * time.Second

	reapBatchLimit = 50
)

var stalledStages = map[string]struct {
	requeueStatus string
	stage         string
}{
	"processing":   {requeueStatus: "pending", stage: "ingestion"},
	"transforming": {requeueStatus: "validated", stage: "transformation"},
	"aggregating":  {requeueStatus: "transformed", stage: "aggregation"},
	"exporting":    {requeueStatus: "aggregated", stage: "export"},
}

type Reaper struct {
	DB         *gorm.DB
	StaleAfter time.Duration
	MaxRetries int
	Interval   time.Duration
}

func NewReaperFromEnv(db *gorm.DB) *Reaper {
	reaper := &Reaper{
		DB:         db,
		StaleAfter: DefaultStaleAfter,
		MaxRetries: DefaultMaxRetries,
		Interval:   DefaultReaperInterval,
	}

	if value, err := time.ParseDuration(os.Getenv("JOB_STALE_AFTER")); err == nil && value > 0 {
		reaper.StaleAfter = value
	}
	if value, err := strconv.Atoi(os.Getenv("JOB_MAX_RETRIES")); err == nil && value >= 0 {
		reaper.MaxRetries = value
	}
	if value, err := time.ParseDuration(os.Getenv("JOB_REAPER_INTERVAL")); err == nil && value > 0 {
		reaper.Interval = value
	}

	return reaper
}

// Run checks for stalled jobs until the context is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	interval := r.Interval
	if interval <= 0 {
		interval = DefaultReaperInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info(
		"stalled-job reaper started",
		"stale_after", r.staleAfter().String(),
		"max_retries", r.MaxRetries,
		"interval", interval.String(),
	)
	defer slog.Info("stalled-job reaper stopped")

	for {
		if _, err := r.ReapOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("stalled-job reaper failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Reaper) ReapOnce(ctx context.Context) (int, error) {
	activeStatuses := make([]string, 0, len(stalledStages))
	for status := range stalledStages {
		activeStatuses = append(activeStatuses, status)
	}

	cutoff := time.Now().UTC().Add(-r.staleAfter())
	var recovered []recovery

	err := r.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var stalled []models.Job

		err := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
			Options:  "SKIP LOCKED",
		}).
			Where("status IN ?", activeStatuses).
			Where("COALESCE(heartbeat_at, updated_at) < ?", cutoff).
			Order("id ASC").
			Limit(reapBatchLimit).
			Find(&stalled).Error
		if err != nil {
			return fmt.Errorf("find stalled jobs: %w", err)
		}

		for _, job := range stalled {
			result, err := r.recoverJob(tx, job)
			if err != nil {
				return err
			}
			if result != nil {
				recovered = append(recovered, *result)
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	// Report only after the transaction has committed.
	for _, result := range recovered {
		metrics.JobsRecovered.WithLabelValues(result.stage, result.outcome).Inc()
		slog.Warn(
			"recovered stalled job",
			"job_id", result.jobID,
			"stage", result.stage,
			"outcome", result.outcome,
			"reason", result.message,
		)
	}

	return len(recovered), nil
}

// recovery describes one stalled job handled by a reaper pass.
type recovery struct {
	jobID   uint
	stage   string
	outcome string
	message string
}

func (r *Reaper) recoverJob(tx *gorm.DB, job models.Job) (*recovery, error) {
	stage := stalledStages[job.Status]

	var message, outcome string
	updates := map[string]interface{}{
		"heartbeat_at": nil,
	}

	if job.RetryCount >= r.MaxRetries {
		message = fmt.Sprintf(
			"%s worker stopped responding; job failed after %d retries",
			stage.stage,
			job.RetryCount,
		)
		updates["status"] = "failed"
		outcome = "failed"
	} else {
		message = fmt.Sprintf(
			"%s worker stopped responding; requeued as %s (retry %d of %d)",
			stage.stage,
			stage.requeueStatus,
			job.RetryCount+1,
			r.MaxRetries,
		)
		updates["status"] = stage.requeueStatus
		updates["retry_count"] = job.RetryCount + 1
		outcome = "requeued"

		if job.Status == "processing" {
			err := tx.Where("job_id = ?", job.ID).
				Delete(&models.JobRecord{}).Error
			if err != nil {
				return nil, fmt.Errorf("clear records for job %d: %w", job.ID, err)
			}
		}
	}
	updates["error_message"] = message

	result := tx.Model(&models.Job{}).
		Where("id = ? AND status = ?", job.ID, job.Status).
		Updates(updates)
	if result.Error != nil {
		return nil, fmt.Errorf("recover job %d: %w", job.ID, result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, nil
	}

	pipelineError := models.PipelineError{
		JobID:   job.ID,
		Stage:   stage.stage,
		Message: message,
	}
	if err := tx.Create(&pipelineError).Error; err != nil {
		return nil, fmt.Errorf("record recovery of job %d: %w", job.ID, err)
	}

	return &recovery{
		jobID:   job.ID,
		stage:   stage.stage,
		outcome: outcome,
		message: message,
	}, nil
}

func (r *Reaper) staleAfter() time.Duration {
	if r.StaleAfter <= 0 {
		return DefaultStaleAfter
	}
	return r.StaleAfter
}
