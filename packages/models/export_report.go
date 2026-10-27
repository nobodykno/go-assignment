package models

import "time"

type ExportedReport struct {
	ID uint `gorm:"primaryKey" json:"id"`

	JobID uint `gorm:"not null;uniqueIndex:uq_exported_reports_job_date_group" json:"job_id"`

	ReportDate time.Time `gorm:"type:date;not null;uniqueIndex:uq_exported_reports_job_date_group" json:"report_date"`

	GroupKey string `gorm:"type:text;not null;uniqueIndex:uq_exported_reports_job_date_group" json:"group_key"`

	ReportData map[string]interface{} `gorm:"type:jsonb;serializer:json;not null" json:"report_data"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (ExportedReport) TableName() string {
	return "exported_reports"
}
