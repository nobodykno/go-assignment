
package service

import (
	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type JobService struct {
	DB *gorm.DB
}

func (s *JobService) CreateJob(name string, filePath string) (*models.Job, error) {
	job := models.Job{
		Name:     name,
		Status:   "pending",
		FilePath: filePath,
	}

	if err := s.DB.Create(&job).Error; err != nil {
		return nil, err
	}

	return &job, nil
}
