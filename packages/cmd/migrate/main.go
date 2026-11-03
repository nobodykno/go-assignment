// Command migrate applies the Goose migrations in packages/migrations
// using the same DB_* environment variables as the services.
//
// Usage: migrate [up|down|status|version]   (default: up)
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	sharedDB "go-assignment/packages/database"
	"go-assignment/packages/logger"
	"go-assignment/packages/migrations"

	"github.com/joho/godotenv"
	"github.com/pressly/goose/v3"
)

func main() {
	if err := godotenv.Load(); err != nil {
		slog.Debug("no .env file found; using environment variables")
	}

	log := logger.Setup("migrate")

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
		log.Error("migration database connection failed", "error", err)
		os.Exit(1)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Error("get SQL connection failed", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	goose.SetBaseFS(migrations.FS)

	if err := goose.SetDialect("postgres"); err != nil {
		log.Error("set migration dialect failed", "error", err)
		os.Exit(1)
	}

	if err := goose.RunContext(ctx, command, sqlDB, "."); err != nil {
		log.Error("migration failed", "command", command, "error", err)
		os.Exit(1)
	}

	log.Info("migration completed", "command", command)
}
