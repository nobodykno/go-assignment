package response

type UploadResponse struct {
	Message  string          `json:"message"`
	FileName string          `json:"fileName"`
	FileSize int64           `json:"fileSize"`
	Job      JobResponseData `json:"job"`
}
