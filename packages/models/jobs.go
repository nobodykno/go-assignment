package models

import "time"


type Job struct {
    ID               uint       `gorm:"primaryKey" json:"id"`
    Name             string     `gorm:"not null" json:"name"`
    Status           string     `gorm:"not null;default:pending" json:"status"`
    FilePath         string     `gorm:"column:file_path;type:text;not null;default:''" json:"file_path"`
    TotalRecords     int        `gorm:"not null;default:0" json:"total_records"`
    ProcessedRecords int        `gorm:"not null;default:0" json:"processed_records"`
    ErrorMessage     *string    `json:"error_message"`
    CreatedAt        time.Time  `json:"created_at"`
    UpdatedAt        time.Time  `json:"updated_at"`
    DeletedAt        *time.Time `json:"deleted_at,omitempty"`
    ValidRecords     int        `gorm:"not null;default:0" json:"valid_records"`
    InvalidRecords   int        `gorm:"not null;default:0" json:"invalid_records"`
}


func (Job) TableName() string {
    return "jobs"
}