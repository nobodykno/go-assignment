package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go-assignment/packages/logger"
)

func main() {
	log := logger.Setup("validation")

	port := os.Getenv("VALIDATION_PORT")
	if port == "" {
		port = "8081"
	}

	// Configure the validation worker count.
	workerCount := 5

	if value := os.Getenv("VALIDATION_WORKERS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			log.Warn("invalid VALIDATION_WORKERS; using default", "value", value, "workers", workerCount)
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
			log.Error("HTTP server shutdown failed", "error", err)

			if closeErr := server.Close(); closeErr != nil {
				log.Error("HTTP server force-close failed", "error", closeErr)
			}
		}
	}()

	log.Info("validation service listening", "port", port, "workers", workerCount)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("validation server failed", "error", err)
		os.Exit(1)
	}

	log.Info("validation service stopped")
}
