package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterRoutes(
	router *http.ServeMux,
	controllers *controller.Controllers,
) {
	health := http.NewServeMux()
	RegisterHealthRoutes(health)

	router.Handle(
		"/health/",
		http.StripPrefix("/health", health),
	)

	RegisterUploadRoutes(router, controllers.Upload)
}
