// Package worker is the shared runtime for the pipeline stage workers
// (ingestion, transformation, aggregation and export). It owns the parts
// every stage had duplicated: startup and graceful shutdown, claiming
// jobs, waking on LISTEN/NOTIFY, heartbeats, logging and metrics.
package worker

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-assignment/packages/database"
	pipelineerrors "go-assignment/packages/errors"
	"go-assignment/packages/jobs"
	"go-assignment/packages/logger"
	"go-assignment/packages/metrics"
	"go-assignment/packages/models"

	"github.com/joho/godotenv"
	"gorm.io/gorm"
)

const (
	// DefaultPollInterval is the fallback check for work when no
	// notification arrives. Notifications normally wake workers at once.
	DefaultPollInterval = 30 * time.Second

	claimErrorBackoff     = 5 * time.Second
	collectorDrainTimeout = 10 * time.Second
)

// Stage describes one pipeline stage.
type Stage struct {
	// Name is the stage label used in logs, metrics and pipeline errors.
	Name string
	// FromStatus is the status of jobs waiting for this stage.
	FromStatus string
	// ActiveStatus is the status while this stage owns the job.
	ActiveStatus string
	// ClaimUpdates are extra columns set when a job is claimed.
	ClaimUpdates map[string]interface{}
	// Process does the stage's work and moves the job to its next
	// status. The context carries a logger tagged with job_id and stage.
	Process func(ctx context.Context, job *models.Job) error
	// PollInterval overrides DefaultPollInterval.
	PollInterval time.Duration
}

// Deps are the shared resources passed to a stage's constructor.
type Deps struct {
	DB        *gorm.DB
	Collector *pipelineerrors.Collector
}

// Main runs a stage worker service until SIGINT or SIGTERM. build
// creates the stage once the database and error collector are ready.
func Main(service string, build func(deps Deps) Stage) {
	if err := godotenv.Load(); err != nil {
		slog.Debug("no .env file found; using environment variables")
	}

	log := logger.Setup(service)

	db, err := database.Connect()
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The collector gets its own context so queued errors can still be
	// saved after the shutdown signal.
	collector := pipelineerrors.NewCollector(db, 100)
	collectorCtx, cancelCollector := context.WithCancel(context.Background())
	defer cancelCollector()
	go collector.Run(collectorCtx)

	dsn, err := database.DSNFromEnv()
	if err != nil {
		log.Error("database configuration invalid", "error", err)
		os.Exit(1)
	}
	notifier := jobs.NewNotifier(dsn)
	go notifier.Run(ctx)

	go metrics.Serve(ctx, func(ctx context.Context) error {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.PingContext(ctx)
	})

	stage := build(Deps{DB: db, Collector: collector})
	RunStage(ctx, db, notifier, stage)

	// Stop new reports and save the ones already queued.
	collector.Close()

	drained := make(chan struct{})
	go func() {
		collector.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		log.Info("pipeline error collector drained")
	case <-time.After(collectorDrainTimeout):
		log.Warn("timed out waiting for pipeline error collector")
	}

	log.Info("service stopped")
}

// RunStage claims and processes jobs until ctx is cancelled. After each
// job it immediately looks for the next one; when the queue is empty it
// sleeps until notified or until the fallback poll interval elapses.
func RunStage(ctx context.Context, db *gorm.DB, notifier *jobs.Notifier, stage Stage) {
	log := slog.With("stage", stage.Name)

	pollInterval := stage.PollInterval
	if pollInterval <= 0 {
		pollInterval = pollIntervalFromEnv()
	}

	var wake <-chan struct{}
	if notifier != nil {
		wake = notifier.Subscribe(stage.FromStatus)
	}

	log.Info("stage worker started", "from_status", stage.FromStatus, "poll_interval", pollInterval.String())
	defer log.Info("stage worker stopped")

	for ctx.Err() == nil {
		job, err := jobs.ClaimNext(ctx, db, stage.FromStatus, stage.ActiveStatus, stage.ClaimUpdates)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("claim job failed", "error", err)
			sleep(ctx, nil, claimErrorBackoff)
			continue
		}

		if job == nil {
			sleep(ctx, wake, pollInterval)
			continue
		}

		runJob(ctx, db, stage, job)
	}
}

// runJob processes one claimed job with a heartbeat, logs and metrics.
func runJob(ctx context.Context, db *gorm.DB, stage Stage, job *models.Job) {
	log := slog.With("stage", stage.Name, "job_id", job.ID)
	jobCtx := logger.WithContext(ctx, log)

	stopHeartbeat := jobs.StartHeartbeat(ctx, db, job.ID, stage.ActiveStatus, jobs.DefaultHeartbeatInterval)
	defer stopHeartbeat()

	log.Info("job started")
	started := time.Now()

	err := stage.Process(jobCtx, job)

	duration := time.Since(started)
	metrics.StageDuration.WithLabelValues(stage.Name).Observe(duration.Seconds())

	if err != nil {
		metrics.JobsTotal.WithLabelValues(stage.Name, "failed").Inc()
		log.Error("job failed", "error", err, "duration_ms", duration.Milliseconds())
		return
	}

	metrics.JobsTotal.WithLabelValues(stage.Name, "completed").Inc()
	log.Info("job completed", "duration_ms", duration.Milliseconds())
}

// sleep waits for ctx cancellation, a wake signal or the timeout.
func sleep(ctx context.Context, wake <-chan struct{}, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-wake:
	case <-timer.C:
	}
}

func pollIntervalFromEnv() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("WORKER_POLL_INTERVAL")); err == nil && value > 0 {
		return value
	}
	return DefaultPollInterval
}
