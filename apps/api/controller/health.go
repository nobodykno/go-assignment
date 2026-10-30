package controller

import (
	"fmt"
	"go-assignment/apps/api/service"
	"net/http"
)

func CheckHealth(
	w http.ResponseWriter,
	r *http.Request,
) {
	message := service.CheckHealth()
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, message)
}
