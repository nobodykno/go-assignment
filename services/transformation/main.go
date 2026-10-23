
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
)

func main() {
	// Connect to the database.
	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	// Create a context that is cancelled on shutdown signals.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Start the background transformation worker.
	worker := &TransformationWorker{
		DB: db,
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

	// Start the HTTP server in a goroutine.
	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("transformation service listening on :%s", port)
		serverErrors <- server.ListenAndServe()
	}()

	// Wait for shutdown or a server error.
	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("transformation server failed: %v", err)
		}
		stop()
	}

	// Allow active HTTP requests to finish.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)

		if closeErr := server.Close(); closeErr != nil {
			log.Printf("server close failed: %v", closeErr)
		}
	}

	log.Println("transformation service stopped")
}
