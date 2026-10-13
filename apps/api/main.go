package main

import (
	"go-assignment/apps/api/middleware"
	"go-assignment/apps/api/route"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	router.Use(middleware.RateLimit())
	route.RegisterRoutes(router)

	router.Run(":8080")
}
