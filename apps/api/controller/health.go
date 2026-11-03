package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"go-assignment/packages/logger"
)

type HealthController struct {
	Ping func(ctx context.Context) error
}

func (c *HealthController) CheckHealth(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set("Content-Type", "application/json")

	if c.Ping != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := c.Ping(ctx); err != nil {
			logger.FromContext(r.Context()).Error("health check failed", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unavailable"})
			return
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
