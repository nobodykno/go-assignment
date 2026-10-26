
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-assignment/packages/database"
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
	defer stop()

	worker := &AggregationWorker{
		DB: db,
	}

	worker.Run(ctx)
}
