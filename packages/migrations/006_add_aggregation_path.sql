-- +goose Up

ALTER TABLE jobs
ADD COLUMN aggregated_file_path TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE jobs
DROP COLUMN aggregated_file_path;