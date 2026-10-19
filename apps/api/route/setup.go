
package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
	"go-assignment/apps/api/service"

	"gorm.io/gorm"
)

func NewRouter(db *gorm.DB) *http.ServeMux {
	router := http.NewServeMux()

	jobService := &service.JobService{
		DB: db,
	}


	uploadController := &controller.UploadController{
		JobService: jobService,
	}

	controllers := &controller.Controllers{
		Upload: uploadController,
	}

	RegisterRoutes(router, controllers)

	return router
}