package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
)

func RegisterUploadRoutes(
	router *http.ServeMux,
	uploadController *controller.UploadController,
) {
	router.HandleFunc("POST /upload", uploadController.UploadFile)
}