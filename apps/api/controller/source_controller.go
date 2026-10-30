package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type SourceController struct {
	DB *gorm.DB
}

type CreateSourceJobRequest struct {
	Name    string                    `json:"name"`
	Sources []models.SourceDefinition `json:"sources"`
}

func (c *SourceController) UploadFilesAndCreateJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit the complete multipart request to 25 MB.
	r.Body = http.MaxBytesReader(w, r.Body, 25<<20)

	if err := r.ParseMultipartForm(8 << 20); err != nil {
		http.Error(w, "invalid multipart request or upload too large", http.StatusBadRequest)
		return
	}

	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	jobName := strings.TrimSpace(r.FormValue("name"))
	if jobName == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	uploadsDir := strings.TrimSpace(os.Getenv("UPLOADS_DIR"))
	if uploadsDir == "" {
		uploadsDir = "uploads"
	}

	if err := os.MkdirAll(uploadsDir, 0750); err != nil {
		http.Error(w, "failed to prepare upload directory", http.StatusInternalServerError)
		return
	}

	// Postman must send each file using the field name "files".
	headers := r.MultipartForm.File["files"]
	if len(headers) == 0 {
		http.Error(w, "at least one file is required under the files field", http.StatusBadRequest)
		return
	}

	var savedPaths []string
	jobCreated := false

	// Remove uploaded files if the job cannot be created.
	defer func() {
		if !jobCreated {
			for _, path := range savedPaths {
				_ = os.Remove(path)
			}
		}
	}()

	sources := make([]models.SourceDefinition, 0, len(headers))
	seenNames := make(map[string]int)

	for i, header := range headers {
		originalName := filepath.Base(header.Filename)
		ext := strings.ToLower(filepath.Ext(originalName))

		var sourceType string
		switch ext {
		case ".csv":
			sourceType = "csv"
		case ".json":
			sourceType = "json"
		default:
			http.Error(
				w,
				"only CSV and JSON files are supported",
				http.StatusBadRequest,
			)
			return
		}

		sourceName := strings.TrimSuffix(originalName, filepath.Ext(originalName))
		sourceName = strings.TrimSpace(sourceName)
		if sourceName == "" || sourceName == "." {
			sourceName = fmt.Sprintf("source-%d", i+1)
		}

		seenNames[sourceName]++
		if seenNames[sourceName] > 1 {
			sourceName = fmt.Sprintf("%s-%d", sourceName, seenNames[sourceName])
		}

		input, err := header.Open()
		if err != nil {
			http.Error(w, "failed to read uploaded file", http.StatusBadRequest)
			return
		}

		output, err := os.CreateTemp(
			uploadsDir,
			"mixed-*"+ext,
		)
		if err != nil {
			_ = input.Close()
			http.Error(w, "failed to save uploaded file", http.StatusInternalServerError)
			return
		}

		// Track the path immediately so error cleanup can remove it.
		savedPaths = append(savedPaths, output.Name())

		_, copyErr := io.Copy(output, input)
		inputCloseErr := input.Close()
		outputCloseErr := output.Close()

		if copyErr != nil || inputCloseErr != nil || outputCloseErr != nil {
			http.Error(w, "failed to save uploaded file", http.StatusInternalServerError)
			return
		}

		sources = append(sources, models.SourceDefinition{
			Name:     sourceName,
			Type:     sourceType,
			FilePath: filepath.Base(output.Name()),
		})
	}

	job := models.Job{
		Name:         jobName,
		Status:       "pending",
		SourceType:   "mixed",
		SourceConfig: models.SourceConfig{Sources: sources},
	}

	if err := c.DB.WithContext(r.Context()).Create(&job).Error; err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	jobCreated = true

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Files uploaded and job created successfully",
		"job":     job,
		"sources": sources,
	})
}

func (c *SourceController) CreateJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req CreateSourceJobRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid JSON request body", http.StatusBadRequest)
		return
	}

	// Reject additional JSON values after the first object.
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "request must contain one JSON object", http.StatusBadRequest)
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if len(req.Sources) == 0 {
		http.Error(w, "at least one source is required", http.StatusBadRequest)
		return
	}

	for i := range req.Sources {
		source := &req.Sources[i]
		source.Name = strings.TrimSpace(source.Name)
		source.Type = strings.ToLower(strings.TrimSpace(source.Type))

		if source.Name == "" {
			http.Error(w, "each source must have a name", http.StatusBadRequest)
			return
		}

		switch source.Type {
		case "api":
			if strings.TrimSpace(source.URL) == "" {
				http.Error(w, "API sources must have a URL", http.StatusBadRequest)
				return
			}
		case "csv", "json":
			if strings.TrimSpace(source.FilePath) == "" {
				http.Error(w, "file sources must have a file path", http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "source type must be api, csv, or json", http.StatusBadRequest)
			return
		}
	}

	job := models.Job{
		Name:         req.Name,
		Status:       "pending",
		SourceType:   "mixed",
		SourceConfig: models.SourceConfig{Sources: req.Sources},
	}

	if err := c.DB.WithContext(r.Context()).Create(&job).Error; err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Job created successfully",
		"job":     job,
	})
}
