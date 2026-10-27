package jobs

import (
	"context"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"
)

const DefaultHeartbeatInterval = 10 * time.Second

// StartHeartbeat refreshes heartbeat_at while a job remains in its
// expected active status. The returned function cancels the goroutine
// and waits for it to stop.
func StartHeartbeat(
	ctx context.Context,
	db *gorm.DB,
	jobID uint,
	activeStatus string,
	interval time.Duration,
) func() {
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}

	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return

			case <-ticker.C:
				result := db.WithContext(heartbeatCtx).
					Table("jobs").
					Where("id = ? AND status = ?", jobID, activeStatus).
					Update("heartbeat_at", time.Now().UTC())

				if result.Error != nil {
					if heartbeatCtx.Err() == nil {
						log.Printf(
							"Failed to update heartbeat for job %d: %v",
							jobID,
							result.Error,
						)
					}
					continue
				}

				if result.RowsAffected == 0 {
					// The job changed status or was removed.
					return
				}
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// SetHeartbeat initializes the heartbeat when a worker claims a job.
func SetHeartbeat(
	ctx context.Context,
	db *gorm.DB,
	jobID uint,
	activeStatus string,
) error {
	result := db.WithContext(ctx).
		Table("jobs").
		Where("id = ? AND status = ?", jobID, activeStatus).
		Update("heartbeat_at", time.Now().UTC())

	if result.Error != nil {
		return fmt.Errorf("initialize heartbeat for job %d: %w", jobID, result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf(
			"cannot initialize heartbeat for job %d: status is no longer %q",
			jobID,
			activeStatus,
		)
	}

	return nil
}
