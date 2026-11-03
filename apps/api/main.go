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

	"go-assignment/apps/api/middleware"
	"go-assignment/apps/api/route"
	sharedDB "go-assignment/packages/database"
	"go-assignment/packages/jobs"
	"go-assignment/packages/logger"
	"go-assignment/packages/metrics"
	"go-assignment/packages/progress"

	"github.com/joho/godotenv"
)

// Rate limit defaults: 100 requests per minute per client on average,
// with bursts of up to 20.
const (
	defaultRateLimitPerMinute = 100
	defaultRateLimitBurst     = 20
)

func main() {
	// Existing environment variables take precedence over .env values.
	envErr := godotenv.Load()

	log := logger.Setup("api")
	if envErr != nil {
		log.Debug("no .env file found; using environment variables")
	}

	// Connect to PostgreSQL.
	db, err := sharedDB.Connect()
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	log.Info("connected to PostgreSQL")

	// Create a context cancelled by an operating-system signal.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Publish queue depth per status and recover stalled jobs.
	tracker := &progress.Tracker{
		DB:       db,
		Interval: 5 * time.Second,
	}
	go tracker.Run(ctx)

	reaper := jobs.NewReaperFromEnv(db)
	go reaper.Run(ctx)

	ping := func(ctx context.Context) error {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.PingContext(ctx)
	}

	// Metrics stay on an internal port, separate from the public API.
	go metrics.Serve(ctx, ping)

	proxies, err := middleware.ParseTrustedProxies(os.Getenv("TRUSTED_PROXIES"))
	if err != nil {
		log.Error("invalid TRUSTED_PROXIES", "error", err)
		os.Exit(1)
	}

	apiKeys := middleware.ParseAPIKeys(os.Getenv("API_KEYS"))

	// Versioned API: rate limit, then authenticate, then route.
	// The health check is public so load balancers can probe it.
	var v1 http.Handler = http.StripPrefix("/v1", route.NewRouter(db, ping))

	if len(apiKeys) > 0 {
		v1 = middleware.APIKeyAuth(apiKeys, "/v1/health")(v1)
	} else {
		log.Warn("API_KEYS is not set; API authentication is disabled")
	}

	limiter := middleware.NewRateLimiter(
		ctx,
		envInt("RATE_LIMIT_PER_MINUTE", defaultRateLimitPerMinute),
		envInt("RATE_LIMIT_BURST", defaultRateLimitBurst),
		proxies,
	)
	v1 = limiter.Middleware(v1)

	mux := http.NewServeMux()
	mux.Handle("/v1/", v1)

	// Every response gets security headers, a request ID, an access log
	// line and request metrics.
	handler := middleware.SecurityHeaders(
		middleware.RequestLogger(proxies)(mux),
	)

	// Configure the API port.
	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Shut down the server gracefully when a signal arrives.
	go func() {
		<-ctx.Done()
		log.Info("shutting down API server")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Error("HTTP server shutdown failed", "error", err)

			if err := server.Close(); err != nil {
				log.Error("force close HTTP server failed", "error", err)
			}
		}
	}()

	log.Info("API server listening", "port", port, "auth_enabled", len(apiKeys) > 0)

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		log.Error("API server failed", "error", err)
		os.Exit(1)
	}

	log.Info("API server stopped")
}

// envInt returns a positive integer from the environment, or fallback.
func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
