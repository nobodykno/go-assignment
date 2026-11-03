package controller

type Controllers struct {
	Health         *HealthController
	Upload         *UploadController
	Source         *SourceController
	Jobs           *JobsController
	PipelineErrors *PipelineErrorsController
}
