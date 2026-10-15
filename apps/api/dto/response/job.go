package response




type JobResponseData struct {
    ID     uint   `json:"ID"`
    Name   string `json:"Name"`
    Status string `json:"Status"`
}

type CreateJobResponse struct {
    Message string          `json:"message"`
    Data    JobResponseData `json:"data"`
}