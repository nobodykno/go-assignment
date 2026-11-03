package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterRoutes(
	router *http.ServeMux,
	controllers *controller.Controllers,
) {
	RegisterHealthRoutes(router, controllers.Health)
	RegisterUploadRoutes(router, controllers.Upload)
	RegisterSourceRoutes(router, controllers.Source)
	RegisterJobsRoutes(router, controllers.Jobs)
	RegisterPipelineErrorsRoutes(router, controllers.PipelineErrors)
}
