
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-assignment/apps/api/middleware"
	"go-assignment/apps/api/route"
	sharedDB "go-assignment/packages/database"
	"go-assignment/packages/progress"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables.
	// Existing environment variables take precedence over .env values.
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using environment variables")
	}

	log.Println("Environment variables loaded")

	// Connect to PostgreSQL.
	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}

	log.Println("Connected to PostgreSQL successfully")

	// Create a context cancelled by an operating-system signal.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Start the background progress tracker.
	tracker := &progress.Tracker{
		DB:       db,
		Interval: 5 * time.Second,
	}

	go tracker.Run(ctx)

	// Register API routes and dependencies.
	v1Router := route.NewRouter(db)

	// Create the root router.
	mux := http.NewServeMux()

	// Apply rate limiting to versioned API routes.
	v1Handler := middleware.RateLimit(
		http.StripPrefix("/v1", v1Router),
	)
	mux.Handle("/v1/", v1Handler)

	// Configure the API port.
	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Shut down the server gracefully when a signal arrives.
	go func() {
		<-ctx.Done()
		log.Println("Shutting down API server...")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)

			if err := server.Close(); err != nil {
				log.Printf("Force close HTTP server: %v", err)
			}
		}
	}()

	log.Printf("API server listening on :%s", port)

	if err := server.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {
		log.Fatalf("API server failed: %v", err)
	}

	log.Println("API server stopped")
}
