ALTER TABLE dlt_messages
DROP CONSTRAINT IF EXISTS uk_dlt_topic_partition_offset;
ALTER TABLE dlt_messages
DROP COLUMN IF EXISTS topic,
DROP COLUMN IF EXISTS partition,
DROP COLUMN IF EXISTS kafka_offset;