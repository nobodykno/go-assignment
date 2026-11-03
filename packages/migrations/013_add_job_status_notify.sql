-- +goose Up

-- Notify listeners on the job_status_changed channel with the new status
-- whenever a job is created or its status changes. Workers LISTEN on it
-- instead of polling the jobs table every few seconds. Notifications are
-- delivered when the transaction commits.

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_job_status_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('job_status_changed', NEW.status);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER trg_jobs_status_insert_notify
AFTER INSERT ON jobs
FOR EACH ROW
EXECUTE FUNCTION notify_job_status_changed();

CREATE TRIGGER trg_jobs_status_update_notify
AFTER UPDATE OF status ON jobs
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION notify_job_status_changed();

-- +goose Down

DROP TRIGGER IF EXISTS trg_jobs_status_update_notify ON jobs;
DROP TRIGGER IF EXISTS trg_jobs_status_insert_notify ON jobs;
DROP FUNCTION IF EXISTS notify_job_status_changed();
