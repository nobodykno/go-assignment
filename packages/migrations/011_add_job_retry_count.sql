
-- +goose Up

ALTER TABLE jobs
ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE jobs
ADD CONSTRAINT chk_jobs_retry_count_nonnegative
CHECK (retry_count >= 0);

-- +goose Down

ALTER TABLE jobs
DROP CONSTRAINT IF EXISTS chk_jobs_retry_count_nonnegative;

ALTER TABLE jobs
DROP COLUMN IF EXISTS retry_count;
