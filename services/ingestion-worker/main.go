
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	sharedDB "go-assignment/packages/database"

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
	defer stop()

	worker := &JobWorker{DB: db}
	worker.Run(ctx)
}
