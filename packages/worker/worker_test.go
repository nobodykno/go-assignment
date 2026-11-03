package worker

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go-assignment/packages/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not configured; skipping PostgreSQL integration test")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("connect to test PostgreSQL database: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get underlying SQL database: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	return db
}

func TestRunStageProcessesQueuedJobsBackToBack(t *testing.T) {
	db := openTestDB(t)
	suffix := time.Now().Format("150405.000000")
	from, active, done := "rs-from-"+suffix, "rs-active-"+suffix, "rs-done-"+suffix

	const jobCount = 3
	ids := make([]uint, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		job := models.Job{Name: "run-stage-test", Status: from}
		if err := db.Create(&job).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
		ids = append(ids, job.ID)
	}
	t.Cleanup(func() { db.Delete(&models.Job{}, ids) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int32
	stage := Stage{
		Name:         "test",
		FromStatus:   from,
		ActiveStatus: active,
		// A long poll interval proves jobs are taken back to back
		// rather than one per poll.
		PollInterval: time.Hour,
		Process: func(ctx context.Context, job *models.Job) error {
			if job.Status != active {
				return errors.New("job was not claimed")
			}
			if processed.Add(1) == jobCount {
				defer cancel()
			}
			return db.Model(&models.Job{}).Where("id = ?", job.ID).Update("status", done).Error
		},
	}

	finished := make(chan struct{})
	go func() {
		RunStage(ctx, db, nil, stage)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out; processed %d of %d jobs", processed.Load(), jobCount)
	}

	var remaining int64
	db.Model(&models.Job{}).Where("id IN ? AND status <> ?", ids, done).Count(&remaining)
	if remaining != 0 {
		t.Fatalf("expected every job to reach %q, %d did not", done, remaining)
	}
}
