
package main

import (
	"log"

	"go-assignment/apps/api/middleware"
	"go-assignment/apps/api/route"
	sharedDB "go-assignment/packages/database"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	log.Println("Environment variables loaded")
	db, err := sharedDB.Connect()
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}

	log.Println("Connected to PostgreSQL successfully")

	// We'll inject db into services that need database access.
	_ = db

	router := gin.Default()
	router.Use(middleware.RateLimit())
	route.RegisterRoutes(router)

	if err := router.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
