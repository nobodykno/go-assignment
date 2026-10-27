package service

import (
	"errors"

	"go-assignment/packages/models"

	"gorm.io/gorm"
)

type JobService struct {
	DB *gorm.DB
}

func (s *JobService) CreateJob(
	name string,
	filePath string,
) (*models.Job, error) {
	job := models.Job{
		Name:       name,
		Status:     "pending",
		FilePath:   filePath,
		SourceType: "upload",
		SourceConfig: models.SourceConfig{
			Sources: []models.SourceDefinition{},
		},
	}

	if err := s.DB.Create(&job).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

// GetJobByID retrieves a job using its ID.
func (s *JobService) GetJobByID(
	jobID uint,
) (*models.Job, error) {
	var job models.Job

	err := s.DB.First(&job, jobID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}

		return nil, err
	}

	return &job, nil
}

// GetAllJobs retrieves all jobs, newest first.
func (s *JobService) GetAllJobs() ([]models.Job, error) {
	var jobs []models.Job

	err := s.DB.Order("created_at DESC").Find(&jobs).Error
	if err != nil {
		return nil, err
	}

	return jobs, nil
}
