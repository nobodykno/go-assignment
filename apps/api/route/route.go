package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterRoutes(
	router *http.ServeMux,
	jobController *controller.JobController,
) {
	// Jobs parent route
	jobs := http.NewServeMux()
	RegisterJobRoutes(jobs, jobController)
	router.Handle("/jobs/", http.StripPrefix("/jobs", jobs))

	// Health parent route
	health := http.NewServeMux()
	RegisterHealthRoutes(health)
	router.Handle("/health/", http.StripPrefix("/health", health))
}