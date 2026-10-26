
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	port := os.Getenv("VALIDATION_PORT")
	if port == "" {
		port = "8081"
	}

	// Configure the validation worker count.
	workerCount := 5

	if value := os.Getenv("VALIDATION_WORKERS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			log.Printf(
				"Invalid VALIDATION_WORKERS value %q; using default %d",
				value,
				workerCount,
			)
		} else {
			workerCount = parsed
		}
	}

	// Configure the HTTP server.
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           NewHandler(workerCount),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Handle graceful shutdown on interrupt or termination.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)

			if closeErr := server.Close(); closeErr != nil {
				log.Printf("HTTP server force-close error: %v", closeErr)
			}
		}
	}()

	log.Printf(
		"Validation service listening on :%s with %d workers",
		port,
		workerCount,
	)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Validation server failed: %v", err)
	}

	log.Println("Validation service stopped")
}
