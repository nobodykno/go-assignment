package models

import "time"

type Job struct {
	ID                  uint         `gorm:"primaryKey" json:"id"`
	Name                string       `gorm:"not null" json:"name"`
	Status              string       `gorm:"not null;default:pending" json:"status"`
	FilePath            string       `gorm:"column:file_path;type:text;not null;default:''" json:"file_path"`
	SourceType          string       `gorm:"column:source_type;type:varchar(30);not null;default:'upload'" json:"source_type"`
	SourceConfig        SourceConfig `gorm:"column:source_config;type:jsonb;serializer:json;not null" json:"source_config"`
	TotalRecords        int          `gorm:"not null;default:0" json:"total_records"`
	ProcessedRecords    int          `gorm:"not null;default:0" json:"processed_records"`
	ErrorMessage        *string      `json:"error_message"`
	CreatedAt           time.Time    `json:"created_at"`
	UpdatedAt           time.Time    `json:"updated_at"`
	DeletedAt           *time.Time   `json:"deleted_at,omitempty"`
	ValidRecords        int          `gorm:"not null;default:0" json:"valid_records"`
	InvalidRecords      int          `gorm:"not null;default:0" json:"invalid_records"`
	TransformedFilePath string       `gorm:"column:transformed_file_path;type:text;not null;default:''" json:"transformed_file_path"`
	AggregatedFilePath  string       `gorm:"column:aggregated_file_path;type:text;not null;default:''" json:"aggregated_file_path"`
	ExportedFilePath    string       `gorm:"column:exported_file_path;type:text;not null;default:''" json:"exported_file_path"`
	RetryCount          int          `gorm:"not null;default:0" json:"retry_count"`
	HeartbeatAt         *time.Time   `gorm:"column:heartbeat_at" json:"heartbeat_at,omitempty"`
}

func (Job) TableName() string {
	return "jobs"
}
