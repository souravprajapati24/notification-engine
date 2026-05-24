ALTER TABLE notification_events
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(36);

CREATE UNIQUE INDEX IF NOT EXISTS uk_idempotency_key
    ON notification_events(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_idempotency_key
    ON notification_events(idempotency_key);

COMMENT ON COLUMN notification_events.idempotency_key IS
'PHASE 4: Unique idempotency key for deduplication. Uses eventId (UUID) to ensure deterministic behavior across Kafka replays, retries, and consumer restarts. Format: UUID string (e.g., 550e8400-e29b-41d4-a716-446655440000)';