
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS notification_events (
                                                   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,


    channels JSONB NOT NULL DEFAULT '[]',

    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',

    metadata JSONB,


    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,


    CONSTRAINT chk_event_status CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'PARTIAL', 'FAILED')),
    CONSTRAINT chk_event_not_empty CHECK (message IS NOT NULL AND message != '')
    );


CREATE INDEX idx_notification_events_user_created
    ON notification_events(user_id, created_at DESC);

CREATE INDEX idx_notification_events_status
    ON notification_events(status);

CREATE INDEX idx_notification_events_created
    ON notification_events(created_at DESC);

-- Comment for documentation
COMMENT ON TABLE notification_events IS
    'Core table storing notification events received from Kafka. Each event can have multiple delivery channels. Status aggregates results from all channels.';

CREATE TABLE IF NOT EXISTS notification_event_channels (
                                                           event_id UUID NOT NULL REFERENCES notification_events(id) ON DELETE CASCADE,
    channel VARCHAR(20) NOT NULL,

    CONSTRAINT chk_channel CHECK (channel IN ('EMAIL', 'SMS', 'WEBSOCKET', 'PUSH_NOTIFICATION', 'SLACK')),
    CONSTRAINT pk_event_channels PRIMARY KEY (event_id, channel)
    );


CREATE TABLE IF NOT EXISTS notification_logs (
                                                 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),


    event_id UUID NOT NULL REFERENCES notification_events(id) ON DELETE CASCADE,


    user_id VARCHAR(100) NOT NULL,
    channel VARCHAR(20) NOT NULL,

    recipient VARCHAR(255),


    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',

    retry_count INTEGER NOT NULL DEFAULT 0,
    last_retry_at TIMESTAMP,

    failure_reason TEXT,
    failure_code VARCHAR(50),

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_channel CHECK (channel IN ('EMAIL', 'SMS', 'WEBSOCKET', 'PUSH_NOTIFICATION', 'SLACK')),
    CONSTRAINT chk_delivery_status CHECK (status IN ('PENDING', 'RETRYING', 'SENT', 'FAILED')),
    CONSTRAINT chk_retry_count CHECK (retry_count >= 0),

    CONSTRAINT uk_event_channel UNIQUE (event_id, channel)
    );


CREATE INDEX idx_notification_logs_event_id
    ON notification_logs(event_id);

CREATE INDEX idx_notification_logs_user_channel_status
    ON notification_logs(user_id, channel, status);

CREATE INDEX idx_notification_logs_status_created
    ON notification_logs(status, created_at DESC);

CREATE INDEX idx_notification_logs_failed
    ON notification_logs(created_at DESC)
    WHERE status = 'FAILED';

CREATE INDEX idx_notification_logs_pending
    ON notification_logs(created_at)
    WHERE status IN ('PENDING', 'RETRYING');

CREATE INDEX idx_notification_logs_user_id
    ON notification_logs(user_id);

-- Comment for documentation
COMMENT ON TABLE notification_logs IS
    'Tracks delivery status for each (event, channel) combination. One row per channel per event. Updated as delivery progresses from PENDING → RETRYING/SENT/FAILED.';

CREATE MATERIALIZED VIEW IF NOT EXISTS notification_delivery_summary AS
SELECT
    DATE(nl.created_at) AS delivery_date,
    nl.channel,
    nl.status,
    COUNT(*) AS count,
    ROUND(
    AVG(EXTRACT(EPOCH FROM (COALESCE(nl.sent_at, CURRENT_TIMESTAMP) - nl.created_at)))::numeric,
    2
    ) AS avg_delivery_seconds,
    MAX(EXTRACT(EPOCH FROM (COALESCE(nl.sent_at, CURRENT_TIMESTAMP) - nl.created_at)))::numeric
    AS max_delivery_seconds
FROM notification_logs nl
GROUP BY DATE(nl.created_at), nl.channel, nl.status;

CREATE INDEX idx_summary_date_channel
    ON notification_delivery_summary(delivery_date, channel);


COMMENT ON MATERIALIZED VIEW notification_delivery_summary IS
    'Pre-computed delivery statistics by date, channel, and status. Used for fast dashboard queries. Refresh periodically (Phase 8).';

CREATE SEQUENCE IF NOT EXISTS notification_batch_id_seq;

INSERT INTO notification_events
(id, user_id, event_type, message, channels, status, metadata, created_at, updated_at)
VALUES
    (
        'f47ac10b-58cc-4372-a567-0e02b2c3d479',
        'test-user-123',
        'TEST_EVENT',
        'This is a test notification',
        '["EMAIL", "SMS", "WEBSOCKET"]',
        'PENDING',
        '{"orderId": "TEST-001", "amount": 999.99}',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT DO NOTHING;


INSERT INTO notification_event_channels (event_id, channel)
VALUES
    ('f47ac10b-58cc-4372-a567-0e02b2c3d479', 'EMAIL'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d479', 'SMS'),
    ('f47ac10b-58cc-4372-a567-0e02b2c3d479', 'WEBSOCKET')
    ON CONFLICT DO NOTHING;

