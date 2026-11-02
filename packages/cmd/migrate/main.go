// Command migrate applies the Goose migrations in packages/migrations
// using the same DB_* environment variables as the services.
//
// Usage: migrate [up|down|status|version]   (default: up)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	sharedDB "go-assignment/packages/database"
	"go-assignment/packages/migrations"

	"github.com/joho/godotenv"
	"github.com/pressly/goose/v3"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using environment variables")
	}

	command := "up"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("Migration database connection failed: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Get SQL connection: %v", err)
	}
	defer sqlDB.Close()

	goose.SetBaseFS(migrations.FS)

	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatalf("Set migration dialect: %v", err)
	}

	if err := goose.RunContext(ctx, command, sqlDB, "."); err != nil {
		log.Fatalf("Migration %q failed: %v", command, err)
	}

	log.Printf("Migration %q completed", command)
}
