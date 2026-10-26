
-- +goose Up

ALTER TABLE exported_reports
    ADD COLUMN group_key TEXT NOT NULL DEFAULT '';

ALTER TABLE exported_reports
    DROP CONSTRAINT uq_exported_reports_job_date;

ALTER TABLE exported_reports
    ADD CONSTRAINT uq_exported_reports_job_date_group
    UNIQUE (job_id, report_date, group_key);


-- +goose Down

ALTER TABLE exported_reports
    DROP CONSTRAINT uq_exported_reports_job_date_group;

ALTER TABLE exported_reports
    DROP COLUMN group_key;

ALTER TABLE exported_reports
    ADD CONSTRAINT uq_exported_reports_job_date
    UNIQUE (job_id, report_date);
