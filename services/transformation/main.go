package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sharedDB "go-assignment/packages/database"
	pipelineerrors "go-assignment/packages/errors"
)

func main() {
	// Connect to the database.
	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	// Create a context cancelled by shutdown signals.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	// Initialize and start the error collector.
	collector := pipelineerrors.NewCollector(db, 100)
	collectorCtx, cancelCollector := context.WithCancel(
		context.Background(),
	)

	go collector.Run(collectorCtx)

	// Start the background transformation worker.
	worker := &TransformationWorker{
		DB:             db,
		ErrorCollector: collector,
	}

	go worker.Run(ctx)

	// Configure the HTTP server.
	port := os.Getenv("TRANSFORMATION_PORT")
	if port == "" {
		port = "8082"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("transformation service listening on :%s", port)
		serverErrors <- server.ListenAndServe()
	}()

	// Wait for a shutdown signal or a server error.
	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("transformation server failed: %v", err)
		}
		stop()
	}

	// Stop accepting requests and allow active requests to finish.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)

		if closeErr := server.Close(); closeErr != nil {
			log.Printf("server close failed: %v", closeErr)
		}
	}
	cancel()

	// Stop the worker before shutting down the collector.
	stop()

	// Close the channel so the collector can drain queued events.
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

	log.Println("transformation service stopped")
}
