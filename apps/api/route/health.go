package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterHealthRoutes(
	router *http.ServeMux,
	healthController *controller.HealthController,
) {
	router.HandleFunc("GET /health", healthController.CheckHealth)
}
