package jobs

import (
	"context"
	"os"
	"sync"
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

// createJob inserts a job and removes it (and dependent rows) afterwards.
func createJob(t *testing.T, db *gorm.DB, job models.Job) models.Job {
	t.Helper()

	if err := db.Create(&job).Error; err != nil {
		t.Fatalf("create test job: %v", err)
	}

	t.Cleanup(func() {
		db.Where("job_id = ?", job.ID).Delete(&models.PipelineError{})
		db.Where("job_id = ?", job.ID).Delete(&models.JobRecord{})
		db.Delete(&models.Job{}, job.ID)
	})

	return job
}

func reload(t *testing.T, db *gorm.DB, id uint) models.Job {
	t.Helper()

	var job models.Job
	if err := db.First(&job, id).Error; err != nil {
		t.Fatalf("reload job %d: %v", id, err)
	}
	return job
}

func TestClaimNextClaimsEachJobOnce(t *testing.T) {
	db := openTestDB(t)
	status := "claim-test-" + time.Now().Format("150405.000000")

	const jobCount = 5
	for i := 0; i < jobCount; i++ {
		createJob(t, db, models.Job{Name: "claim-test", Status: status})
	}

	// Many concurrent claimers must claim every job exactly once.
	var (
		mu      sync.Mutex
		claimed = map[uint]int{}
		wg      sync.WaitGroup
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, err := ClaimNext(context.Background(), db, status, status+"-done", nil)
				if err != nil {
					t.Errorf("ClaimNext: %v", err)
					return
				}
				if job == nil {
					return
				}
				mu.Lock()
				claimed[job.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimed) != jobCount {
		t.Fatalf("expected %d claimed jobs, got %d", jobCount, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("job %d claimed %d times", id, count)
		}
		job := reload(t, db, id)
		if job.Status != status+"-done" || job.HeartbeatAt == nil {
			t.Errorf("job %d not claimed correctly: status=%q heartbeat=%v", id, job.Status, job.HeartbeatAt)
		}
	}
}

func TestReaperRequeuesStalledJob(t *testing.T) {
	db := openTestDB(t)
	stale := time.Now().UTC().Add(-time.Hour)

	job := createJob(t, db, models.Job{
		Name:        "reaper-requeue",
		Status:      "processing",
		HeartbeatAt: &stale,
	})

	// A partial ingestion attempt left records behind.
	record := models.JobRecord{
		JobID:            job.ID,
		RecordNumber:     1,
		RecordData:       map[string]string{"a": "b"},
		ValidationErrors: []string{},
	}
	if err := db.Create(&record).Error; err != nil {
		t.Fatalf("create record: %v", err)
	}

	reaper := &Reaper{DB: db, StaleAfter: time.Minute, MaxRetries: 2}
	if _, err := reaper.ReapOnce(context.Background()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}

	got := reload(t, db, job.ID)
	if got.Status != "pending" || got.RetryCount != 1 || got.HeartbeatAt != nil {
		t.Fatalf("expected pending with retry 1, got status=%q retry=%d heartbeat=%v",
			got.Status, got.RetryCount, got.HeartbeatAt)
	}

	var records int64
	db.Model(&models.JobRecord{}).Where("job_id = ?", job.ID).Count(&records)
	if records != 0 {
		t.Fatalf("expected partial records to be removed, found %d", records)
	}

	var errorsSaved int64
	db.Model(&models.PipelineError{}).Where("job_id = ? AND stage = ?", job.ID, "ingestion").Count(&errorsSaved)
	if errorsSaved != 1 {
		t.Fatalf("expected one pipeline error, found %d", errorsSaved)
	}
}

func TestReaperFailsJobAfterMaxRetries(t *testing.T) {
	db := openTestDB(t)
	stale := time.Now().UTC().Add(-time.Hour)

	job := createJob(t, db, models.Job{
		Name:        "reaper-fail",
		Status:      "aggregating",
		RetryCount:  2,
		HeartbeatAt: &stale,
	})

	reaper := &Reaper{DB: db, StaleAfter: time.Minute, MaxRetries: 2}
	if _, err := reaper.ReapOnce(context.Background()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}

	got := reload(t, db, job.ID)
	if got.Status != "failed" || got.ErrorMessage == nil {
		t.Fatalf("expected failed job with message, got status=%q message=%v", got.Status, got.ErrorMessage)
	}
}

func TestReaperIgnoresHealthyJob(t *testing.T) {
	db := openTestDB(t)
	fresh := time.Now().UTC()

	job := createJob(t, db, models.Job{
		Name:        "reaper-healthy",
		Status:      "transforming",
		HeartbeatAt: &fresh,
	})

	reaper := &Reaper{DB: db, StaleAfter: time.Minute, MaxRetries: 2}
	if _, err := reaper.ReapOnce(context.Background()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}

	if got := reload(t, db, job.ID); got.Status != "transforming" || got.RetryCount != 0 {
		t.Fatalf("healthy job changed: status=%q retry=%d", got.Status, got.RetryCount)
	}
}

func TestNotifierWakesSubscriberOnStatusChange(t *testing.T) {
	db := openTestDB(t)
	status := "notify-" + time.Now().Format("150405.000000")

	notifier := NewNotifier(os.Getenv("TEST_DATABASE_URL"))
	wake := notifier.Subscribe(status)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go notifier.Run(ctx)

	// The first signal comes from (re)connecting; drain it once LISTEN
	// is active so the next one can only come from the trigger.
	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("notifier did not connect")
	}

	createJob(t, db, models.Job{Name: "notify-test", Status: status})

	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("expected a notification after inserting a job (is migration 013 applied?)")
	}
}
