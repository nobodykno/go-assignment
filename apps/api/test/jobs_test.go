package test

import (
	"encoding/json"
	"fmt"
	"go-assignment/apps/api/controller"
	"go-assignment/apps/api/service"
	"go-assignment/packages/models"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestJobsGetJobRejectsUnsupportedMethod(t *testing.T) {
	ctrl := controller.NewJobsController(nil)
	req := httptest.NewRequest(http.MethodPost, "/jobs/1", nil)
	req.SetPathValue("id", "1")
	rec := httptest.NewRecorder()

	ctrl.GetJob(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}

	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("expected Allow header %q, got %q", http.MethodGet, got)
	}
}

func TestJobsGetJobRejectsInvalidID(t *testing.T) {
	ctrl := controller.NewJobsController(nil)

	tests := []struct {
		name string
		id   string
	}{
		{name: "empty ID", id: ""},
		{name: "non-numeric ID", id: "abc"},
		{name: "zero ID", id: "0"},
		{name: "negative ID", id: "-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/jobs/"+tt.id, nil)
			req.SetPathValue("id", tt.id)
			rec := httptest.NewRecorder()

			ctrl.GetJob(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
			}

			assertJobsErrorResponse(t, rec, "job ID must be a positive integer")
		})
	}
}

func TestJobsGetJobReturnsNotFound(t *testing.T) {
	db := openUploadTestDB(t)
	ctrl := controller.NewJobsController(&service.JobService{DB: db})

	req := httptest.NewRequest(http.MethodGet, "/jobs/999999999", nil)
	req.SetPathValue("id", "999999999")
	rec := httptest.NewRecorder()

	ctrl.GetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}

	assertJobsErrorResponse(t, rec, "job not found")
}

func TestJobsGetJobReturnsJobAndProgress(t *testing.T) {
	db := openUploadTestDB(t)

	job := models.Job{
		Name:                "jobs-controller-test",
		Status:              "transformed",
		SourceType:          "upload",
		TransformedFilePath: "transformed/output.json",
		TotalRecords:        100,
		ProcessedRecords:    80,
		ValidRecords:        70,
		InvalidRecords:      10,
	}

	if err := db.Create(&job).Error; err != nil {
		t.Fatalf("create test job: %v", err)
	}
	t.Cleanup(func() {
		db.Delete(&models.Job{}, job.ID)
	})

	ctrl := controller.NewJobsController(&service.JobService{DB: db})

	req := httptest.NewRequest(http.MethodGet, "/jobs/1", nil)
	req.SetPathValue("id", fmt.Sprint(job.ID))
	rec := httptest.NewRecorder()

	ctrl.GetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d; body: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var result controller.JobProgressResponse
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.ID != job.ID {
		t.Errorf("expected job ID %d, got %d", job.ID, result.ID)
	}
	if result.Name != job.Name {
		t.Errorf("expected job name %q, got %q", job.Name, result.Name)
	}
	if result.Status != "transformed" {
		t.Errorf("expected status %q, got %q", "transformed", result.Status)
	}
	if result.ProgressPercentage != 50 {
		t.Errorf("expected progress 50, got %v", result.ProgressPercentage)
	}
	if result.TotalRecords != 100 {
		t.Errorf("expected total records 100, got %d", result.TotalRecords)
	}
	if result.ProcessedRecords != 80 {
		t.Errorf("expected processed records 80, got %d", result.ProcessedRecords)
	}
	if result.ValidRecords != 70 {
		t.Errorf("expected valid records 70, got %d", result.ValidRecords)
	}
	if result.InvalidRecords != 10 {
		t.Errorf("expected invalid records 10, got %d", result.InvalidRecords)
	}
	if result.TransformedFilePath != "transformed/output.json" {
		t.Errorf("unexpected transformed file path %q", result.TransformedFilePath)
	}
	if result.CreatedAt == "" || result.UpdatedAt == "" {
		t.Error("expected created_at and updated_at to be populated")
	}
}

func TestJobsListRejectsUnsupportedMethod(t *testing.T) {
	ctrl := controller.NewJobsController(nil)
	req := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	rec := httptest.NewRecorder()

	ctrl.ListJobs(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}

	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("expected Allow header %q, got %q", http.MethodGet, got)
	}
}

func TestJobsListReturnsJobsAndCount(t *testing.T) {
	db := openUploadTestDB(t)

	createdJobs := []models.Job{
		{
			Name:       "jobs-list-test-pending",
			Status:     "pending",
			SourceType: "upload",
		},
		{
			Name:             "jobs-list-test-exported",
			Status:           "exported",
			SourceType:       "upload",
			ExportedFilePath: "exports/result.json",
		},
	}

	for i := range createdJobs {
		if err := db.Create(&createdJobs[i]).Error; err != nil {
			t.Fatalf("create test job: %v", err)
		}
	}

	t.Cleanup(func() {
		for i := range createdJobs {
			db.Delete(&models.Job{}, createdJobs[i].ID)
		}
	})

	ctrl := controller.NewJobsController(&service.JobService{DB: db})
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()

	ctrl.ListJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d; body: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var result controller.JobsListResponse
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.Count != 2 {
		t.Fatalf("expected count 2, got %d", result.Count)
	}
	if len(result.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(result.Jobs))
	}

	if result.Jobs[0].Name != "jobs-list-test-exported" {
		t.Errorf("expected newest job first, got %q", result.Jobs[0].Name)
	}
	if result.Jobs[0].ProgressPercentage != 100 {
		t.Errorf("expected exported job progress 100, got %v", result.Jobs[0].ProgressPercentage)
	}
	if result.Jobs[1].ProgressPercentage != 0 {
		t.Errorf("expected pending job progress 0, got %v", result.Jobs[1].ProgressPercentage)
	}
}

func TestJobProgressPercentageByStatus(t *testing.T) {
	db := openUploadTestDB(t)

	tests := []struct {
		name            string
		status          string
		total           int
		processed       int
		transformedPath string
		aggregatedPath  string
		exportedPath    string
		expected        float64
	}{
		{name: "pending", status: "pending", expected: 0},
		{name: "processing without records", status: "processing", expected: 5},
		{name: "processing with records", status: "processing", total: 100, processed: 40, expected: 10},
		{name: "processing capped at 25", status: "processing", total: 100, processed: 150, expected: 25},
		{name: "validated", status: "validated", expected: 25},
		{name: "transforming", status: "transforming", expected: 35},
		{name: "transformed", status: "transformed", expected: 50},
		{name: "aggregating", status: "aggregating", expected: 65},
		{name: "aggregated", status: "aggregated", expected: 75},
		{name: "exporting", status: "exporting", expected: 90},
		{name: "exported", status: "exported", expected: 100},
		{
			name:         "failed after export",
			status:       "failed",
			exportedPath: "exports/result.json",
			expected:     100,
		},
		{
			name:           "failed after aggregation",
			status:         "failed",
			aggregatedPath: "aggregated/result.json",
			expected:       75,
		},
		{
			name:            "failed after transformation",
			status:          "failed",
			transformedPath: "transformed/result.json",
			expected:        50,
		},
		{
			name:      "failed during processing",
			status:    "failed",
			total:     100,
			processed: 20,
			expected:  5,
		},
		{name: "unknown status", status: "unknown", expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := models.Job{
				Name:                "progress-test-" + tt.name,
				Status:              tt.status,
				SourceType:          "upload",
				TotalRecords:        tt.total,
				ProcessedRecords:    tt.processed,
				TransformedFilePath: tt.transformedPath,
				AggregatedFilePath:  tt.aggregatedPath,
				ExportedFilePath:    tt.exportedPath,
			}

			if err := db.Create(&job).Error; err != nil {
				t.Fatalf("create job: %v", err)
			}
			t.Cleanup(func() {
				db.Delete(&models.Job{}, job.ID)
			})

			ctrl := controller.NewJobsController(&service.JobService{DB: db})
			req := httptest.NewRequest(
				http.MethodGet,
				"/jobs/"+strconv.FormatUint(uint64(job.ID), 10),
				nil,
			)
			req.SetPathValue("id", strconv.FormatUint(uint64(job.ID), 10))

			rec := httptest.NewRecorder()
			ctrl.GetJob(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusOK,
					rec.Code,
					rec.Body.String(),
				)
			}

			var result controller.JobProgressResponse
			if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
				t.Fatalf("decode response: %v", err)
			}

			if result.ProgressPercentage != tt.expected {
				t.Errorf(
					"expected progress %.2f, got %.2f",
					tt.expected,
					result.ProgressPercentage,
				)
			}
		})
	}
}

func assertJobsErrorResponse(t *testing.T, rec *httptest.ResponseRecorder, expected string) {
	t.Helper()

	var result controller.JobsErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode error response: %v", err)
	}

	if result.Error != expected {
		t.Errorf("expected error %q, got %q", expected, result.Error)
	}
}
