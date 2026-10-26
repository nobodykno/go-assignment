
-- +goose Up
ALTER TABLE jobs
ADD COLUMN exported_file_path TEXT NOT NULL DEFAULT '';

CREATE TABLE exported_reports (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    report_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_exported_reports_job_date
        UNIQUE (job_id, report_date)
);

-- +goose Down
DROP TABLE IF EXISTS exported_reports;

ALTER TABLE jobs
DROP COLUMN IF EXISTS exported_file_path;
