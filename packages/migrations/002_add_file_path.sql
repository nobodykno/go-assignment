-- +goose Up
ALTER TABLE jobs
ADD COLUMN file_path TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE jobs
DROP COLUMN file_path;