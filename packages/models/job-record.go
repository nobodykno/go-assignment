package models

import "time"

type JobRecord struct {
	ID               uint              `gorm:"primaryKey" json:"id"`
	JobID            uint              `gorm:"not null;uniqueIndex:uq_job_records_job_record" json:"job_id"`
	RecordNumber     int               `gorm:"not null;uniqueIndex:uq_job_records_job_record" json:"record_number"`
	RecordData       map[string]string `gorm:"type:jsonb;serializer:json;not null" json:"record_data"`
	IsValid          bool              `gorm:"not null;default:false;index" json:"is_valid"`
	ValidationErrors []string          `gorm:"type:jsonb;serializer:json;not null" json:"validation_errors"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}
