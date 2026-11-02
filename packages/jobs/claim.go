package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-assignment/packages/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func ClaimNext(
	ctx context.Context,
	db *gorm.DB,
	fromStatus string,
	toStatus string,
	extra map[string]interface{},
) (*models.Job, error) {
	var job models.Job

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Find with Limit avoids GORM logging "record not found" on
		// every idle poll.
		result := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
			Options:  "SKIP LOCKED",
		}).
			Where("status = ?", fromStatus).
			Order("created_at ASC").
			Limit(1).
			Find(&job)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}

		updates := map[string]interface{}{
			"status":        toStatus,
			"error_message": nil,
			"heartbeat_at":  time.Now().UTC(),
		}
		for column, value := range extra {
			updates[column] = value
		}

		return tx.Model(&models.Job{}).
			Where("id = ?", job.ID).
			Updates(updates).Error
	})

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim %s job: %w", fromStatus, err)
	}

	job.Status = toStatus
	return &job, nil
}
