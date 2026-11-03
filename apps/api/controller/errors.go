package controller

import (
	"net/http"
	"os"
	"strings"

	"go-assignment/packages/logger"
)

// logInternalError records the cause of a 500 response with the request
// ID. Clients only receive a generic message, so internal details such
// as database errors or file paths are never exposed.
func logInternalError(r *http.Request, message string, err error) {
	logger.FromContext(r.Context()).Error(message, "error", err)
}

// uploadsDirectory is where uploaded input files are stored. It must
// match the ingestion worker's UPLOADS_DIR.
func uploadsDirectory() string {
	if dir := os.Getenv("UPLOADS_DIR"); strings.TrimSpace(dir) != "" {
		return strings.TrimSpace(dir)
	}
	return "uploads"
}
