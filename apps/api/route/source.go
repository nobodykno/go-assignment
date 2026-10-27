package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterSourceRoutes(
	router *http.ServeMux,
	sourceController *controller.SourceController,

) {

	router.HandleFunc(
		"POST /jobs/mixed",
		sourceController.CreateJob,
	)

	router.HandleFunc(
		"POST /jobs/mixed/upload",
		sourceController.UploadFilesAndCreateJob,
	)
}
