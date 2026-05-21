ALTER TABLE notification_logs
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_next_retry_ready
    ON notification_logs(next_retry_at)
    WHERE status = 'RETRYING';

CREATE INDEX IF NOT EXISTS idx_failure_code_status
    ON notification_logs(failure_code, status)
    WHERE status IN ('RETRYING', 'FAILED');

COMMENT ON COLUMN notification_logs.next_retry_at IS
    'Timestamp when retry should execute';