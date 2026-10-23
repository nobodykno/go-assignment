
-- +goose Up

CREATE TABLE job_records (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    record_number INTEGER NOT NULL,
    record_data JSONB NOT NULL,
    is_valid BOOLEAN NOT NULL DEFAULT FALSE,
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_job_records_job_record
        UNIQUE (job_id, record_number)
);

CREATE INDEX idx_job_records_job_valid
    ON job_records (job_id, is_valid);

-- +goose Down

DROP TABLE job_records;
