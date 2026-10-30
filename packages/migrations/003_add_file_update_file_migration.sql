-- +goose Up

ALTER TABLE jobs
ADD COLUMN valid_records INTEGER NOT NULL DEFAULT 0;

ALTER TABLE jobs
ADD COLUMN invalid_records INTEGER NOT NULL DEFAULT 0;


-- +goose Down

ALTER TABLE jobs
DROP COLUMN invalid_records;

ALTER TABLE jobs
DROP COLUMN valid_records;