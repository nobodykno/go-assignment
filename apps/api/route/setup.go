
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

	jobController := &controller.JobController{
		JobService: jobService,
	}

	uploadController := &controller.UploadController{
		JobService: jobService,
	}

	controllers := &controller.Controllers{
		Job:    jobController,
		Upload: uploadController,
	}

	RegisterRoutes(router, controllers)

	return router
}