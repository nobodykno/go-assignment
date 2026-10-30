package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterPipelineErrorsRoutes(
	router *http.ServeMux,
	errorsController *controller.PipelineErrorsController,
) {
	router.HandleFunc(
		"GET /jobs/{id}/errors",
		errorsController.GetByJobID,
	)
}
