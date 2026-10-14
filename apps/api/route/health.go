package route

import (
	"go-assignment/apps/api/controllers"

	"github.com/gin-gonic/gin"
)

func RegisterHealthRoutes(router *gin.RouterGroup) {
	router.GET("/health", gin.WrapF(controller.CheckHealth))
}