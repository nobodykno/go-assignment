
package test

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go-assignment/apps/api/controller"
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

func newSourceControllerRequest(
	method string,
	body []byte,
	contentType string,
) (*httptest.ResponseRecorder, *http.Request) {
	req := httptest.NewRequest(method, "/sources", bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	return httptest.NewRecorder(), req
}

func TestSourceCreateJobRejectsUnsupportedMethod(t *testing.T) {
	recorder, req := newSourceControllerRequest(
		http.MethodGet,
		nil,
		"",
	)

	ctrl := &controller.SourceController{}
	ctrl.CreateJob(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", recorder.Code)
	}

	if recorder.Header().Get("Allow") != http.MethodPost {
		t.Errorf("expected Allow: POST, got %q", recorder.Header().Get("Allow"))
	}
}

func TestSourceCreateJobRejectsInvalidJSON(t *testing.T) {
	recorder, req := newSourceControllerRequest(
		http.MethodPost,
		[]byte(`{"name":`),
		"application/json",
	)

	ctrl := &controller.SourceController{}
	ctrl.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestSourceCreateJobRejectsUnknownJSONField(t *testing.T) {
	recorder, req := newSourceControllerRequest(
		http.MethodPost,
		[]byte(`{"name":"test","sources":[{"name":"weather","type":"api","url":"https://example.com","unexpected":true}]}`),
		"application/json",
	)

	ctrl := &controller.SourceController{}
	ctrl.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestSourceCreateJobRejectsEmptyName(t *testing.T) {
	recorder, req := newSourceControllerRequest(
		http.MethodPost,
		[]byte(`{"name":"   ","sources":[{"name":"weather","type":"api","url":"https://example.com"}]}`),
		"application/json",
	)

	ctrl := &controller.SourceController{}
	ctrl.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestSourceCreateJobRejectsEmptySources(t *testing.T) {
	recorder, req := newSourceControllerRequest(
		http.MethodPost,
		[]byte(`{"name":"test-job","sources":[]}`),
		"application/json",
	)

	ctrl := &controller.SourceController{}
	ctrl.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestSourceCreateJobRejectsInvalidSourceDefinitions(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing source name",
			body: `{"name":"job","sources":[{"name":" ","type":"api","url":"https://example.com"}]}`,
		},
		{
			name: "API source missing URL",
			body: `{"name":"job","sources":[{"name":"weather","type":"api","url":" "}]}`,
		},
		{
			name: "CSV source missing file path",
			body: `{"name":"job","sources":[{"name":"records","type":"csv","file_path":" "}]}`,
		},
		{
			name: "JSON source missing file path",
			body: `{"name":"job","sources":[{"name":"records","type":"json","file_path":""}]}`,
		},
		{
			name: "unsupported source type",
			body: `{"name":"job","sources":[{"name":"records","type":"xml","file_path":"records.xml"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder, req := newSourceControllerRequest(
				http.MethodPost,
				[]byte(tt.body),
				"application/json",
			)

			ctrl := &controller.SourceController{}
			ctrl.CreateJob(recorder, req)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf(
					"expected 400, got %d: %s",
					recorder.Code,
					recorder.Body.String(),
				)
			}
		})
	}
}

func TestSourceUploadFilesRejectsUnsupportedMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/sources/upload", nil)
	recorder := httptest.NewRecorder()

	ctrl := &controller.SourceController{}
	ctrl.UploadFilesAndCreateJob(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", recorder.Code)
	}

	if recorder.Header().Get("Allow") != http.MethodPost {
		t.Errorf("expected Allow: POST, got %q", recorder.Header().Get("Allow"))
	}
}

func TestSourceUploadFilesRejectsInvalidMultipartRequest(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost,
		"/sources/upload",
		strings.NewReader("not multipart"),
	)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=invalid")

	recorder := httptest.NewRecorder()
	ctrl := &controller.SourceController{}
	ctrl.UploadFilesAndCreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestSourceUploadFilesRequiresName(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("files", "records.csv")
	if err != nil {
		t.Fatalf("create file field: %v", err)
	}
	_, _ = part.Write([]byte("id,value\n1,hello\n"))
	_ = writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/sources/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()
	ctrl := &controller.SourceController{}
	ctrl.UploadFilesAndCreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}

	if !strings.Contains(recorder.Body.String(), "name is required") {
		t.Errorf("unexpected response: %s", recorder.Body.String())
	}
}

func TestSourceUploadFilesRequiresFiles(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("name", "test-job")
	_ = writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/sources/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()
	ctrl := &controller.SourceController{}
	ctrl.UploadFilesAndCreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestSourceUploadFilesRejectsUnsupportedExtension(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("name", "test-job")

	part, err := writer.CreateFormFile("files", "records.xml")
	if err != nil {
		t.Fatalf("create file field: %v", err)
	}
	_, _ = part.Write([]byte("<records/>"))
	_ = writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/sources/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()
	ctrl := &controller.SourceController{}
	ctrl.UploadFilesAndCreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

// TestSourceCreateJobSuccess requires a migrated PostgreSQL test database.
func TestSourceCreateJobSuccess(t *testing.T) {
	db := openUploadTestDB(t)

	ctrl := &controller.SourceController{DB: db}

	body := []byte(`{
		"name": "source-controller-test",
		"sources": [
			{
				"name": "weather",
				"type": "api",
				"url": "https://example.com/weather"
			}
		]
	}`)

	recorder, req := newSourceControllerRequest(
		http.MethodPost,
		body,
		"application/json",
	)

	ctrl.CreateJob(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var result struct {
		Message string       `json:"message"`
		Job      models.Job  `json:"job"`
	}

	if err := json.NewDecoder(recorder.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.Job.ID == 0 {
		t.Fatal("expected created job ID")
	}

	if result.Job.Status != "pending" {
		t.Errorf("expected pending status, got %q", result.Job.Status)
	}

	t.Cleanup(func() {
		if err := db.Delete(&models.Job{}, result.Job.ID).Error; err != nil {
			t.Errorf("delete test job: %v", err)
		}
	})
}

// TestSourceUploadFilesSuccess verifies multipart uploads and job creation.
func TestSourceUploadFilesSuccess(t *testing.T) {
	db := openUploadTestDB(t)

	uploadsDir := t.TempDir()
	t.Setenv("UPLOADS_DIR", uploadsDir)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("name", "mixed-source-test")

	csvPart, err := writer.CreateFormFile("files", "records.csv")
	if err != nil {
		t.Fatalf("create CSV file field: %v", err)
	}
	_, _ = csvPart.Write([]byte("id,value\n1,hello\n"))

	jsonPart, err := writer.CreateFormFile("files", "records.json")
	if err != nil {
		t.Fatalf("create JSON file field: %v", err)
	}
	_, _ = jsonPart.Write([]byte(`[{"id":1,"value":"hello"}]`))

	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/sources/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()
	ctrl := &controller.SourceController{DB: db}
	ctrl.UploadFilesAndCreateJob(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var result struct {
		Message string                   `json:"message"`
		Job     models.Job               `json:"job"`
		Sources []models.SourceDefinition `json:"sources"`
	}

	if err := json.NewDecoder(recorder.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.Job.ID == 0 {
		t.Fatal("expected created job ID")
	}

	if len(result.Sources) != 2 {
		t.Fatalf("expected two sources, got %d", len(result.Sources))
	}

	t.Cleanup(func() {
		if err := db.Delete(&models.Job{}, result.Job.ID).Error; err != nil {
			t.Errorf("delete test job: %v", err)
		}

		for _, source := range result.Sources {
			if source.FilePath == "" {
				continue
			}

			if err := os.Remove(filepath.Join(uploadsDir, source.FilePath)); err != nil &&
				!os.IsNotExist(err) {
				t.Errorf("remove uploaded test file: %v", err)
			}
		}
	})
}

// Compile-time check that the test helper returns a valid GORM database.
var _ *gorm.DB
