
package controller

type Controllers struct {
	Upload *UploadController
	Source *SourceController
	Jobs   *JobsController
	PipelineErrors *PipelineErrorsController
}
