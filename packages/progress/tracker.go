package progress

import (
	"context"
	"log/slog"
	"time"

	"go-assignment/packages/metrics"
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

// activeStatuses are the statuses of jobs still moving through the
// pipeline.
var activeStatuses = []string{
	"pending",
	"processing",
	"validated",
	"transforming",
	"transformed",
	"aggregating",
	"aggregated",
	"exporting",
}

// Tracker periodically monitors active pipeline jobs.
type Tracker struct {
	DB       *gorm.DB
	Interval time.Duration
}

// Run monitors active jobs until the context is cancelled.
func (t *Tracker) Run(ctx context.Context) {
	interval := t.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("progress tracker started")

	// Check immediately, then periodically.
	for {
		if err := t.trackActiveJobs(ctx); err != nil {
			if ctx.Err() != nil {
				break
			}

			slog.Error("progress tracker failed", "error", err)
		}

		select {
		case <-ctx.Done():
			slog.Info("progress tracker stopped")
			return
		case <-ticker.C:
		}
	}
}

// trackActiveJobs reads active jobs without modifying their state.
func (t *Tracker) trackActiveJobs(ctx context.Context) error {
	var jobs []models.Job

	err := t.DB.WithContext(ctx).
		Where(
			"status IN ?",
			activeStatuses,
		).
		Order("created_at ASC").
		Find(&jobs).Error
	if err != nil {
		return err
	}

	// Publish queue depth per status; a growing count means a stage
	// is falling behind.
	counts := make(map[string]int, len(activeStatuses))
	for _, status := range activeStatuses {
		counts[status] = 0
	}

	for _, job := range jobs {
		counts[job.Status]++

		slog.Debug(
			"job progress",
			"job_id", job.ID,
			"name", job.Name,
			"status", job.Status,
			"processed", job.ProcessedRecords,
			"total", job.TotalRecords,
		)
	}

	for status, count := range counts {
		metrics.JobsInStatus.WithLabelValues(status).Set(float64(count))
	}

	return nil
}
