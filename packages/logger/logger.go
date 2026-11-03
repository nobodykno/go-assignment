// Package logger configures structured logging with log/slog.
//
// LOG_FORMAT selects "json" (for log aggregators) or "text" (default,
// readable in a terminal). LOG_LEVEL selects debug, info (default),
// warn or error. Every line carries the service name, and the standard
// library log package is redirected to the same handler.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	gormlogger "gorm.io/gorm/logger"
)

type contextKey struct{}

// Setup builds the service logger and installs it as the slog default.
func Setup(service string) *slog.Logger {
	options := &slog.HandlerOptions{Level: levelFromEnv()}

	var handler slog.Handler
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "json") {
		handler = slog.NewJSONHandler(os.Stdout, options)
	} else {
		handler = slog.NewTextHandler(os.Stdout, options)
	}

	logger := slog.New(handler).With("service", service)
	slog.SetDefault(logger)

	return logger
}

// WithContext returns a copy of ctx carrying logger.
func WithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

// FromContext returns the logger stored in ctx, or the default logger.
// Handlers and workers use it so request and job IDs reach every line.
func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(contextKey{}).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}

// GORM returns a GORM logger that writes through slog, so SQL warnings
// use the same format as application logs. Missing rows are not errors.
func GORM(logger *slog.Logger) gormlogger.Interface {
	return gormlogger.NewSlogLogger(logger, gormlogger.Config{
		SlowThreshold:             500 * time.Millisecond,
		LogLevel:                  gormlogger.Warn,
		IgnoreRecordNotFoundError: true,
	})
}

func levelFromEnv() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
