
-- +goose Up

ALTER TABLE jobs
ADD COLUMN source_type VARCHAR(30) NOT NULL DEFAULT 'upload';

ALTER TABLE jobs
ADD COLUMN source_config JSONB NOT NULL
DEFAULT '{"sources":[]}'::jsonb;

-- +goose Down

ALTER TABLE jobs
DROP COLUMN source_config;

ALTER TABLE jobs
DROP COLUMN source_type;
