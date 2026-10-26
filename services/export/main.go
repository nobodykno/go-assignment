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
		log.Fatalf("connect to database: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("get database connection: %v", err)
	}
	defer sqlDB.Close()

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

	// Inject the collector into the export worker.
	worker := &ExportWorker{
		DB:             db,
		ErrorCollector: collector,
	}

	worker.Run(ctx)

	// The worker has stopped; drain queued errors before exiting.
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

	log.Println("Export worker stopped")
}
