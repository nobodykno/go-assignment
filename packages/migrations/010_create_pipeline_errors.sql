
-- +goose Up
CREATE TABLE IF NOT EXISTS pipeline_errors (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    stage VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_errors_job_id
    ON pipeline_errors(job_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_errors_created_at
    ON pipeline_errors(created_at);

-- +goose Down
DROP TABLE IF EXISTS pipeline_errors;
