package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterJobRoutes(
	router *http.ServeMux,
	jobController *controller.JobController,
) {
	router.HandleFunc("POST /submit", jobController.CreateJob)
}