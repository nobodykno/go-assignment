// Package metrics defines the Prometheus metrics shared by the API and
// the pipeline workers, and serves them with a health endpoint.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// JobsTotal counts jobs finished by each stage, by outcome
	// ("completed" or "failed").
	JobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_jobs_total",
		Help: "Jobs finished by a pipeline stage, by outcome.",
	}, []string{"stage", "status"})

	// StageDuration measures how long each stage takes per job.
	StageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pipeline_stage_duration_seconds",
		Help:    "Time spent processing one job in a pipeline stage.",
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"stage"})

	// RecordsProcessed counts records handled by each stage.
	RecordsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_records_processed_total",
		Help: "Records processed by a pipeline stage.",
	}, []string{"stage"})

	// ErrorsTotal counts pipeline errors reported by each stage.
	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_errors_total",
		Help: "Pipeline errors reported by a stage.",
	}, []string{"stage"})

	// JobsRecovered counts stalled jobs handled by the reaper, by
	// outcome ("requeued" or "failed").
	JobsRecovered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_jobs_recovered_total",
		Help: "Stalled jobs recovered by the reaper, by outcome.",
	}, []string{"stage", "outcome"})

	// JobsInStatus is the number of jobs currently in each active
	// status; a growing count shows a stage falling behind.
	JobsInStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pipeline_jobs_in_status",
		Help: "Jobs currently in each active pipeline status.",
	}, []string{"status"})

	// HTTPRequests counts API requests by route and response code.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests handled, by method, route and status code.",
	}, []string{"method", "route", "code"})

	// HTTPDuration measures API request latency by route.
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency, by method and route.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})
)

// HealthFunc reports whether the service can do its work.
type HealthFunc func(ctx context.Context) error

// Serve runs an HTTP server exposing /metrics and /health on
// METRICS_PORT (default 9090) until ctx is cancelled. It is kept off the
// public API port so metrics are only reachable inside the network.
func Serve(ctx context.Context, health HealthFunc) {
	port := os.Getenv("METRICS_PORT")
	if port == "" {
		port = "9090"
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", HealthHandler(health))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	slog.Info("metrics server listening", "port", port)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server failed", "error", err)
	}
}

// HealthHandler returns 200 when health succeeds and 503 otherwise.
// The failure reason is logged, not returned to the caller.
func HealthHandler(health HealthFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if health != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()

			if err := health(ctx); err != nil {
				slog.Warn("health check failed", "error", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"unavailable"}`))
				return
			}
		}

		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}
