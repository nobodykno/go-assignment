package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	sharedDB "go-assignment/packages/database"
	pipelineerrors "go-assignment/packages/errors"

	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using environment variables")
	}

	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("Worker database connection failed: %v", err)
	}

	log.Println("Ingestion worker connected to PostgreSQL")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	collector := pipelineerrors.NewCollector(db, 100)

	// Use a separate context so shutdown can drain queued events
	// before the collector is cancelled.
	collectorCtx, cancelCollector := context.WithCancel(
		context.Background(),
	)

	go collector.Run(collectorCtx)

	worker := &JobWorker{
		DB:             db,
		ErrorCollector: collector,
	}

	worker.Run(ctx)

	// The worker has stopped. Prevent additional reports and drain
	// events already queued for the collector.
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

	log.Println("Ingestion worker stopped")
}
