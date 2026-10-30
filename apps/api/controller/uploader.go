package controller

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"go-assignment/apps/api/dto/response"
	"go-assignment/apps/api/middleware"
	"go-assignment/apps/api/service"
)

const uploadDir = "uploads"

type UploadController struct {
	JobService *service.JobService
}

func (ctrl *UploadController) UploadFile(
	w http.ResponseWriter,
	r *http.Request,
) {
	// 1. Allow only POST requests.
	if r.Method != http.MethodPost {
		http.Error(
			w,
			"Method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	// 2. Limit the entire request to 10 MiB.
	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		middleware.MaxUploadSize,
	)

	// 3. Parse multipart form data.
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		http.Error(
			w,
			"Invalid upload or request exceeds 10 MiB",
			http.StatusBadRequest,
		)
		return
	}

	// Clean up temporary files created by multipart parsing.
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	// 4. Read the uploaded file.
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(
			w,
			"File is required",
			http.StatusBadRequest,
		)
		return
	}
	defer file.Close()

	// 5. Validate the file extension.
	if err := middleware.ValidateFileExtension(
		header.Filename,
	); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	// 6. Create the upload directory if necessary.
	if err := os.MkdirAll(uploadDir, 0750); err != nil {
		http.Error(
			w,
			"Failed to prepare upload directory",
			http.StatusInternalServerError,
		)
		return
	}

	// 7. Generate a unique storage filename.
	ext := strings.ToLower(filepath.Ext(header.Filename))

	dst, err := os.CreateTemp(
		uploadDir,
		"upload-*"+ext,
	)
	if err != nil {
		http.Error(
			w,
			"Failed to create stored file",
			http.StatusInternalServerError,
		)
		return
	}

	filePath := dst.Name()

	// Remove the saved file if a later step fails.
	keepFile := false
	defer func() {
		if !keepFile {
			os.Remove(filePath)
		}
	}()

	// Restrict file permissions to the owner.
	if err := dst.Chmod(0600); err != nil {
		dst.Close()
		http.Error(
			w,
			"Failed to secure uploaded file",
			http.StatusInternalServerError,
		)
		return
	}

	// 8. Copy the uploaded content to disk.
	size, copyErr := io.Copy(dst, file)
	closeErr := dst.Close()

	if copyErr != nil || closeErr != nil {
		http.Error(
			w,
			"Failed to save uploaded file",
			http.StatusInternalServerError,
		)
		return
	}

	// 9. Reject empty files.
	if size == 0 {
		http.Error(
			w,
			"Uploaded file is empty",
			http.StatusBadRequest,
		)
		return
	}

	// 10. Create a job after the file is saved.
	jobName := filepath.Base(header.Filename)

	job, err := ctrl.JobService.CreateJob(jobName, filePath)
	if err != nil {
		http.Error(
			w,
			"Failed to create job",
			http.StatusInternalServerError,
		)
		return
	}

	// Keep the file because job creation succeeded.
	keepFile = true

	// 11. Build the response DTO.
	res := response.UploadResponse{
		Message:  "File uploaded and job created successfully",
		FileName: jobName,
		FileSize: size,
		Job: response.JobResponseData{
			ID:     job.ID,
			Name:   job.Name,
			Status: job.Status,
		},
	}

	// 12. Return the JSON response.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(res)
}
