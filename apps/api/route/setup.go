
package route

import (
	"net/http"

	"go-assignment/apps/api/controller"
	"go-assignment/apps/api/service"

	"gorm.io/gorm"
)

func NewRouter(db *gorm.DB) *http.ServeMux {
	router := http.NewServeMux()

	// Create the job service using the existing DB handle.
	jobService := &service.JobService{
		DB: db,
	}

	// Inject the service into the controller.
	jobController := &controller.JobController{
		JobService: jobService,
	}

	// Register job and health endpoints.
	RegisterRoutes(router, jobController)

	return router
}
