
-- +goose Up

ALTER TABLE jobs
ADD COLUMN heartbeat_at TIMESTAMPTZ;

CREATE INDEX idx_jobs_active_heartbeat
ON jobs (heartbeat_at)
WHERE status IN (
    'processing',
    'transforming',
    'aggregating',
    'exporting'
);

-- +goose Down

DROP INDEX IF EXISTS idx_jobs_active_heartbeat;

ALTER TABLE jobs
DROP COLUMN IF EXISTS heartbeat_at;
