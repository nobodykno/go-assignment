package main

import (
	"log"
	"net/http"

	"go-assignment/apps/api/middleware"
	"go-assignment/apps/api/route"
	sharedDB "go-assignment/packages/database"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables.
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using environment variables")
	}

	log.Println("Environment variables loaded")

	// Connect to PostgreSQL once.
	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}

	log.Println("Connected to PostgreSQL successfully")

	// Create all API routes and dependencies.
	v1Router := route.NewRouter(db)

	// Create the root router.
	mux := http.NewServeMux()

	// Apply rate limiting to versioned API routes.
	v1Handler := middleware.RateLimit(
		http.StripPrefix("/v1", v1Router),
	)
	mux.Handle("/v1/", v1Handler)

	log.Println("API server listening on :8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
