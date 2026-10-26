
package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterJobsRoutes(
	router *http.ServeMux,
	jobsController *controller.JobsController,
) {
	router.HandleFunc(
		"GET /jobs",
		jobsController.ListJobs,
	)

	router.HandleFunc(
		"GET /jobs/{id}",
		jobsController.GetJob,
	)
}
