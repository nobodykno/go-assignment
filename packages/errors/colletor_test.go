package errors

import (
	"context"
	"strings"
	"testing"

	"go-assignment/packages/models"
)

func validEvent() ErrorEvent {
	return ErrorEvent{
		JobID:   1,
		Stage:   "ingestion",
		Message: "Failed to read input file",
	}
}

func TestReportRejectsInvalidEvents(t *testing.T) {
	tests := []struct {
		name    string
		event   ErrorEvent
		wantErr string
	}{
		{
			name:    "missing job ID",
			event:   ErrorEvent{Stage: "ingestion", Message: "failed"},
			wantErr: "job ID is required",
		},
		{
			name:    "missing stage",
			event:   ErrorEvent{JobID: 1, Message: "failed"},
			wantErr: "pipeline stage is required",
		},
		{
			name:    "missing message",
			event:   ErrorEvent{JobID: 1, Stage: "ingestion"},
			wantErr: "error message is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := NewCollector(nil, 1)

			err := collector.Report(context.Background(), tt.event)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}

			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf(
					"expected error containing %q, got %q",
					tt.wantErr,
					err.Error(),
				)
			}
		})
	}
}

func TestReportQueuesEvent(t *testing.T) {
	collector := NewCollector(nil, 1)
	event := validEvent()

	if err := collector.Report(context.Background(), event); err != nil {
		t.Fatalf("Report() returned unexpected error: %v", err)
	}

	select {
	case got := <-collector.Events:
		if got != event {
			t.Fatalf("expected event %+v, got %+v", event, got)
		}
	default:
		t.Fatal("expected event to be queued")
	}
}

func TestReportRejectsAfterClose(t *testing.T) {
	collector := NewCollector(nil, 1)
	collector.Close()

	err := collector.Report(context.Background(), validEvent())
	if err == nil {
		t.Fatal("expected an error after closing the collector")
	}

	if !strings.Contains(err.Error(), "error collector is closed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloseCanBeCalledMoreThanOnce(t *testing.T) {
	collector := NewCollector(nil, 1)

	collector.Close()
	collector.Close()
}

func TestCollectorPersistsReportedEvent(t *testing.T) {
	db := openTestDB(t)

	// Create a job for the pipeline_errors foreign-key relationship.
	job := models.Job{
		Name:   "collector-test",
		Status: "failed",
	}

	if err := db.Create(&job).Error; err != nil {
		t.Fatalf("create test job: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Where("job_id = ?", job.ID).
			Delete(&models.PipelineError{}).Error; err != nil {
			t.Errorf("delete test pipeline errors: %v", err)
		}

		if err := db.Delete(&models.Job{}, job.ID).Error; err != nil {
			t.Errorf("delete test job: %v", err)
		}
	})

	collector := NewCollector(db, 10)
	ctx := context.Background()

	// Start the collector before reporting an event.
	go collector.Run(ctx)

	event := ErrorEvent{
		JobID:   job.ID,
		Stage:   "ingestion",
		Message: "test ingestion failure",
	}

	if err := collector.Report(ctx, event); err != nil {
		t.Fatalf("report event: %v", err)
	}

	// Closing the channel lets Run process queued events before exiting.
	collector.Close()
	collector.Wait()

	var saved models.PipelineError
	if err := db.Where(
		"job_id = ? AND stage = ? AND message = ?",
		job.ID,
		event.Stage,
		event.Message,
	).First(&saved).Error; err != nil {
		t.Fatalf("expected event to be persisted: %v", err)
	}

	if saved.JobID != event.JobID {
		t.Errorf("expected job ID %d, got %d", event.JobID, saved.JobID)
	}

	if saved.Stage != event.Stage {
		t.Errorf("expected stage %q, got %q", event.Stage, saved.Stage)
	}

	if saved.Message != event.Message {
		t.Errorf("expected message %q, got %q", event.Message, saved.Message)
	}
}
