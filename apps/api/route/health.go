package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterHealthRoutes(router *http.ServeMux) {
	router.HandleFunc("GET /health", controller.CheckHealth)
}