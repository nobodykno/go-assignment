package test

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"go-assignment/apps/api/controller"
	"go-assignment/apps/api/dto/response"
	"go-assignment/apps/api/service"
	"go-assignment/packages/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// createMultipartUpload creates a multipart HTTP request containing a file.
func createMultipartUpload(
	t *testing.T,
	filename string,
	content []byte,
) *http.Request {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if filename != "" {
		part, err := writer.CreateFormFile("file", filename)
		if err != nil {
			t.Fatalf("create multipart file: %v", err)
		}

		if _, err := part.Write(content); err != nil {
			t.Fatalf("write multipart file: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return req
}

// openUploadTestDB connects to the dedicated PostgreSQL test database.
func openUploadTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not configured; skipping integration test")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get underlying SQL database: %v", err)
	}

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		t.Fatalf("ping test database: %v", err)
	}

	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})

	return db
}

func TestUploadFileRejectsUnsupportedMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/upload", nil)
	recorder := httptest.NewRecorder()

	ctrl := &controller.UploadController{}
	ctrl.UploadFile(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", recorder.Code)
	}
}

func TestUploadFileRejectsMissingFile(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()
	ctrl := &controller.UploadController{}
	ctrl.UploadFile(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status 400, got %d: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}
}

func TestUploadFileRejectsInvalidExtension(t *testing.T) {
	req := createMultipartUpload(
		t,
		"malware.exe",
		[]byte("test content"),
	)

	recorder := httptest.NewRecorder()
	ctrl := &controller.UploadController{}
	ctrl.UploadFile(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status 400, got %d: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}
}

func TestUploadFileRejectsEmptyFile(t *testing.T) {
	req := createMultipartUpload(t, "empty.csv", []byte{})

	recorder := httptest.NewRecorder()
	ctrl := &controller.UploadController{}
	ctrl.UploadFile(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status 400, got %d: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	if !strings.Contains(recorder.Body.String(), "Uploaded file is empty") {
		t.Fatalf("unexpected response: %q", recorder.Body.String())
	}
}

func TestUploadFileRejectsInvalidMultipartRequest(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost,
		"/upload",
		strings.NewReader("this is not multipart data"),
	)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=invalid")

	recorder := httptest.NewRecorder()
	ctrl := &controller.UploadController{}
	ctrl.UploadFile(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status 400, got %d: %s",
			http.StatusBadRequest,
			recorder.Body.String(),
		)
	}
}

func TestUploadFileSuccessfulUpload(t *testing.T) {
	db := openUploadTestDB(t)

	// UploadController writes files relative to the current directory.
	// Use a temporary directory to avoid repository artifacts.
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("change working directory: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	req := createMultipartUpload(
		t,
		"sample.csv",
		[]byte("name,value\nexample,123\n"),
	)

	recorder := httptest.NewRecorder()
	ctrl := &controller.UploadController{
		JobService: &service.JobService{DB: db},
	}

	ctrl.UploadFile(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf(
			"expected status 201, got %d: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var result response.UploadResponse
	if err := json.NewDecoder(
		io.LimitReader(recorder.Body, 1<<20),
	).Decode(&result); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	if result.FileName != "sample.csv" {
		t.Errorf("expected filename sample.csv, got %q", result.FileName)
	}

	expectedSize := int64(len("name,value\nexample,123\n"))
	if result.FileSize != expectedSize {
		t.Errorf("expected file size %d, got %d", expectedSize, result.FileSize)
	}

	if result.Job.ID == 0 {
		t.Fatal("expected a created job ID")
	}

	if result.Job.Status != "pending" {
		t.Errorf("expected job status pending, got %q", result.Job.Status)
	}

	// Remove the test job and uploaded file after the test.
	t.Cleanup(func() {
		if err := db.Delete(&models.Job{}, result.Job.ID).Error; err != nil {
			t.Errorf("delete test job: %v", err)
		}
	})
}
