
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
	workers sync.WaitGroup
}

func NewCollector(db *gorm.DB, bufferSize int) *Collector {
	if bufferSize < 1 {
		bufferSize = 100
	}

	return &Collector{
		DB:     db,
		Events: make(chan ErrorEvent, bufferSize),
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
		case event, ok := <-c.Events:
			if !ok {
				return
			}

			if err := c.persist(ctx, event); err != nil {
				log.Printf("Failed to persist pipeline error: %v", err)
			}

		case <-ctx.Done():
			// Process events already queued before exiting.
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

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("error collector is closed")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.Events <- event:
		return nil
	}
}

// Close prevents further reports and closes the event channel.
func (c *Collector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.closed = true
		close(c.Events)
	}
}

// Wait waits for the collector goroutine to finish.
func (c *Collector) Wait() {
	c.workers.Wait()
}

// drain persists events already queued when the context is cancelled.
func (c *Collector) drain(ctx context.Context) {
	for {
		select {
		case event, ok := <-c.Events:
			if !ok {
				return
			}

			if err := c.persist(ctx, event); err != nil {
				log.Printf("Failed to persist queued pipeline error: %v", err)
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
	// Allow a bounded database write even if the worker context
	// has been cancelled during shutdown.
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
