
-- +goose Up
ALTER TABLE jobs
ADD COLUMN transformed_file_path TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE jobs
DROP COLUMN transformed_file_path;
