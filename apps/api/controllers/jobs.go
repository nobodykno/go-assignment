package controller

import (
	"net/http"

	"go-assignment/apps/api/service"

	"github.com/gin-gonic/gin"
)

type JobController struct {
	JobService *service.JobService
}

type CreateJobRequest struct {
	Name string `json:"name" binding:"required"`
}

func (ctrl *JobController) CreateJob(c *gin.Context) {
	var request CreateJobRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Name is required",
		})
		return
	}

	job, err := ctrl.JobService.CreateJob(request.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to create job",
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Job created successfully",
		"data":    job,
	})
}
