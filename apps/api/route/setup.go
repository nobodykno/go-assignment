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

	sourceController := &controller.SourceController{
		DB: db,
	}

	jobsController := controller.NewJobsController(jobService)

	pipelineErrorsController := &controller.PipelineErrorsController{
		DB: db,
	}

	controllers := &controller.Controllers{
		Upload:         uploadController,
		Source:         sourceController,
		Jobs:           jobsController,
		PipelineErrors: pipelineErrorsController,
	}

	RegisterRoutes(router, controllers)

	return router
}
