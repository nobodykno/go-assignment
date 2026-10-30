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

type ErrorEvent struct {
	JobID   uint
	Stage   string
	Message string
}

type Collector struct {
	DB     *gorm.DB
	Events chan ErrorEvent

	mu      sync.RWMutex
	closed  bool
	done    chan struct{}
	workers sync.WaitGroup
	reports sync.WaitGroup
}

func NewCollector(db *gorm.DB, bufferSize int) *Collector {
	if bufferSize < 1 {
		bufferSize = 100
	}

	return &Collector{
		DB:     db,
		Events: make(chan ErrorEvent, bufferSize),
		done:   make(chan struct{}),
	}
}

// Run consumes error events and stores them in the database.
func (c *Collector) Run(ctx context.Context) {
	c.workers.Add(1)
	defer c.workers.Done()

	log.Println("Pipeline error collector started")
	defer log.Println("Pipeline error collector stopped")

	for {
		select {
		case event := <-c.Events:
			if err := c.persist(ctx, event); err != nil {
				log.Printf("Failed to persist pipeline error: %v", err)
			}

		case <-c.done:
			c.reports.Wait()
			c.drain(ctx)
			return

		case <-ctx.Done():
			// Stop accepting reports and allow in-flight reports to exit.
			c.Close()
			c.reports.Wait()
			c.drain(ctx)
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

// Wait waits for the collector goroutine to finish.
func (c *Collector) Wait() {
	c.workers.Wait()
}

// drain persists events already queued before shutdown.
func (c *Collector) drain(ctx context.Context) {
	for {
		select {
		case event := <-c.Events:
			if err := c.persist(ctx, event); err != nil {
				log.Printf(
					"Failed to persist queued pipeline error: %v",
					err,
				)
			}

		default:
			return
		}
	}
}

func (c *Collector) persist(
	ctx context.Context,
	event ErrorEvent,
) error {
	// Give database writes a bounded opportunity to finish even if
	// the worker context was cancelled during shutdown.
	persistCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()

	record := models.PipelineError{
		JobID:   event.JobID,
		Stage:   event.Stage,
		Message: event.Message,
	}

	if err := c.DB.WithContext(persistCtx).Create(&record).Error; err != nil {
		return fmt.Errorf("save pipeline error: %w", err)
	}

	return nil
}
