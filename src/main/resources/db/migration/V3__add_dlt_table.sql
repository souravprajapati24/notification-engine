
CREATE TABLE IF NOT EXISTS dlt_messages (
                                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    topic VARCHAR(100) NOT NULL,
    partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    message_key TEXT,
    message_payload TEXT NOT NULL,

    event_id UUID,
    user_id VARCHAR(100),
    channel VARCHAR(50),


    failure_code VARCHAR(50),
    error_reason TEXT,

    processed BOOLEAN NOT NULL DEFAULT FALSE,
    replay_result VARCHAR(255),


    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_dlt_topic_partition_offset UNIQUE (topic, partition, kafka_offset)
    );

CREATE INDEX IF NOT EXISTS idx_dlt_created ON dlt_messages(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlt_topic ON dlt_messages(topic);
CREATE INDEX IF NOT EXISTS idx_dlt_event_id ON dlt_messages(event_id);
CREATE INDEX IF NOT EXISTS idx_dlt_user_id ON dlt_messages(user_id);
CREATE INDEX IF NOT EXISTS idx_dlt_channel ON dlt_messages(channel);
CREATE INDEX IF NOT EXISTS idx_dlt_unprocessed ON dlt_messages(processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_dlt_failure_code ON dlt_messages(failure_code);
CREATE INDEX IF NOT EXISTS idx_dlt_kafka_offset ON dlt_messages(topic, kafka_offset);

COMMENT ON TABLE dlt_messages IS
'Dead Letter Topic messages. Stores messages that permanently failed (all retries exhausted). Used for inspection and manual replay by operators.';

COMMENT ON COLUMN dlt_messages.processed IS
'TRUE = operator has reviewed and replayed, FALSE = waiting for operator action';

COMMENT ON COLUMN dlt_messages.replay_result IS
'Result of replay attempt (e.g., "REPLAYED_BY_OPERATOR_1716374400000")';

COMMENT ON COLUMN dlt_messages.kafka_offset IS
'Kafka partition offset where message came from (used for tracking source)';

COMMENT ON COLUMN dlt_messages.event_id IS
'Event ID extracted from payload (for quick querying)';

COMMENT ON COLUMN dlt_messages.user_id IS
'User ID extracted from payload (for filtering by user)';

COMMENT ON COLUMN dlt_messages.channel IS
'Notification channel that failed (EMAIL, SMS, WEBSOCKET)';