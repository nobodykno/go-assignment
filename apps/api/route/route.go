package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterRoutes(
	router *http.ServeMux,
	controllers *controller.Controllers,
) {
	jobs := http.NewServeMux()
	RegisterJobRoutes(jobs, controllers.Job)
	router.Handle("/jobs/", http.StripPrefix("/jobs", jobs))

	health := http.NewServeMux()
	RegisterHealthRoutes(health)
	router.Handle("/health/", http.StripPrefix("/health", health))

	upload := http.NewServeMux()
	RegisterUploadRoutes(upload, controllers.Upload)
	router.Handle("/upload/", http.StripPrefix("/upload", upload))
}