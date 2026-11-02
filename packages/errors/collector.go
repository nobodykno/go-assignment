package errors

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go-assignment/packages/models"

	"gorm.io/gorm"
)

const (
	// defaultBatchSize is the maximum number of events written per INSERT.
	defaultBatchSize = 200
	// defaultFlushInterval bounds how long a partial batch waits in memory.
	defaultFlushInterval = 500 * time.Millisecond
)

type ErrorEvent struct {
	JobID   uint
	Stage   string
	Message string
}

type Collector struct {
	DB     *gorm.DB
	Events chan ErrorEvent

	// BatchSize and FlushInterval control how queued events are grouped
	// into multi-row inserts. Zero values use the package defaults.
	BatchSize     int
	FlushInterval time.Duration

	mu      sync.RWMutex
	closed  bool
	done    chan struct{}
	stopped chan struct{}
	reports sync.WaitGroup
}

func NewCollector(db *gorm.DB, bufferSize int) *Collector {
	if bufferSize < 1 {
		bufferSize = 100
	}

	return &Collector{
		DB:      db,
		Events:  make(chan ErrorEvent, bufferSize),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Run consumes error events and stores them in the database in batches.
// A batch is written when it reaches BatchSize events or when
// FlushInterval elapses, whichever comes first. Run must be called
// exactly once.
func (c *Collector) Run(ctx context.Context) {
	defer close(c.stopped)

	log.Println("Pipeline error collector started")
	defer log.Println("Pipeline error collector stopped")

	batchSize := c.batchSize()
	batch := make([]ErrorEvent, 0, batchSize)

	ticker := time.NewTicker(c.flushInterval())
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.persist(ctx, batch); err != nil {
			log.Printf("Failed to persist %d pipeline error(s): %v", len(batch), err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case event := <-c.Events:
			batch = append(batch, event)
			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-c.done:
			c.reports.Wait()
			batch = c.drain(batch)
			flush()
			return

		case <-ctx.Done():
			// Stop accepting reports and allow in-flight reports to exit.
			c.Close()
			c.reports.Wait()
			batch = c.drain(batch)
			flush()
			return
		}
	}
}

// Report submits an error event to the collector.
func (c *Collector) Report(
	ctx context.Context,
	event ErrorEvent,
) error {
	if event.JobID == 0 {
		return fmt.Errorf("job ID is required")
	}

	if event.Stage == "" {
		return fmt.Errorf("pipeline stage is required")
	}

	if event.Message == "" {
		return fmt.Errorf("error message is required")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Register the report while holding the read lock. Close() obtains
	// the write lock, preventing new reports from registering afterward.
	c.mu.RLock()

	if c.closed {
		c.mu.RUnlock()
		return fmt.Errorf("error collector is closed")
	}

	c.reports.Add(1)
	c.mu.RUnlock()

	defer c.reports.Done()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-c.done:
		return fmt.Errorf("error collector is closed")

	case c.Events <- event:
		return nil
	}
}

// Close prevents new reports and wakes blocked Report and Run calls.
// Events remains open so concurrent reports cannot panic by sending
// to a closed channel.
func (c *Collector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.closed = true
		close(c.done)
	}
}

func (c *Collector) Wait() {
	<-c.stopped
}

// drain appends events already queued before shutdown to batch.
func (c *Collector) drain(batch []ErrorEvent) []ErrorEvent {
	for {
		select {
		case event := <-c.Events:
			batch = append(batch, event)
		default:
			return batch
		}
	}
}

func (c *Collector) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return defaultBatchSize
}

func (c *Collector) flushInterval() time.Duration {
	if c.FlushInterval > 0 {
		return c.FlushInterval
	}
	return defaultFlushInterval
}

func (c *Collector) persist(
	ctx context.Context,
	events []ErrorEvent,
) error {
	persistCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()

	records := make([]models.PipelineError, 0, len(events))
	for _, event := range events {
		records = append(records, models.PipelineError{
			JobID:   event.JobID,
			Stage:   event.Stage,
			Message: event.Message,
		})
	}

	err := c.DB.WithContext(persistCtx).
		CreateInBatches(&records, c.batchSize()).Error
	if err == nil {
		return nil
	}
	if len(records) == 1 {
		return fmt.Errorf("save pipeline errors: %w", err)
	}

	failed := 0
	for i := range records {
		if rowErr := c.DB.WithContext(persistCtx).Create(&records[i]).Error; rowErr != nil {
			failed++
			log.Printf(
				"Failed to persist pipeline error for job %d: %v",
				records[i].JobID,
				rowErr,
			)
		}
	}

	if failed > 0 {
		return fmt.Errorf("save pipeline errors: %d of %d failed", failed, len(records))
	}

	return nil
}
