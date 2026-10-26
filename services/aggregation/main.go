package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-assignment/packages/database"
	pipelineerrors "go-assignment/packages/errors"
)

func main() {
	db, err := database.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	// Initialize the error collector.
	collector := pipelineerrors.NewCollector(db, 100)
	collectorCtx, cancelCollector := context.WithCancel(
		context.Background(),
	)

	go collector.Run(collectorCtx)

	// Inject the collector into the aggregation worker.
	worker := &AggregationWorker{
		DB:             db,
		ErrorCollector: collector,
	}

	// Run the worker until a shutdown signal arrives.
	worker.Run(ctx)

	// Allow queued error events to be processed before exiting.
	collector.Close()

	drained := make(chan struct{})
	go func() {
		collector.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		log.Println("Pipeline error collector drained")
	case <-time.After(10 * time.Second):
		log.Println("Timed out waiting for error collector")
	}

	cancelCollector()
	stop()

	log.Println("Aggregation worker stopped")
}
