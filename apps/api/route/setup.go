package route

import (
	"context"
	"net/http"

	"go-assignment/apps/api/controller"
	"go-assignment/apps/api/service"

	"gorm.io/gorm"
)

// NewRouter registers the v1 routes. ping checks database health.
func NewRouter(db *gorm.DB, ping func(ctx context.Context) error) *http.ServeMux {
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
		Health:         &controller.HealthController{Ping: ping},
		Upload:         uploadController,
		Source:         sourceController,
		Jobs:           jobsController,
		PipelineErrors: pipelineErrorsController,
	}

	RegisterRoutes(router, controllers)

	return router
}
