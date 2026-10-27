package models

import "time"

// PipelineError stores an error reported by a pipeline stage.
type PipelineError struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	JobID     uint      `gorm:"not null;index" json:"job_id"`
	Stage     string    `gorm:"type:varchar(50);not null" json:"stage"`
	Message   string    `gorm:"type:text;not null" json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

func (PipelineError) TableName() string {
	return "pipeline_errors"
}
