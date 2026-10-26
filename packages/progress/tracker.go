
package progress

import (
	"context"
	"log"
	"time"

	"go-assignment/packages/models"

	"gorm.io/gorm"
)

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

	log.Println("Progress tracker started")

	// Check immediately, then periodically.
	for {
		if err := t.trackActiveJobs(ctx); err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("Progress tracker error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("Progress tracker stopped")
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
			[]string{
				"pending",
				"processing",
				"validated",
				"transforming",
				"transformed",
				"aggregating",
				"aggregated",
				"exporting",
			},
		).
		Order("created_at ASC").
		Find(&jobs).Error
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}

		log.Printf(
			"Job progress: id=%d name=%q status=%s processed=%d total=%d",
			job.ID,
			job.Name,
			job.Status,
			job.ProcessedRecords,
			job.TotalRecords,
		)
	}

	return nil
}
