package controllers

import (
	"fmt"
	"net/http"
    "go-assignment/apps/api/service"
)

func CheckHealth(
	w http.ResponseWriter,
	r *http.Request,
) {
	message := service.CheckHealth()
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, message)
}