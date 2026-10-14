package route

import (
	"go-assignment/apps/api/controllers"

	"github.com/gin-gonic/gin"
)

func createNewJob(router *gin.RouterGroup) {
	router.POST("/jobs", gin.WrapF(controller.CheckHealth))
}