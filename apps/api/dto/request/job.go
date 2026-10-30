package request

type CreateJobRequest struct {
	Name string `json:"name" validate:"required"`
}
