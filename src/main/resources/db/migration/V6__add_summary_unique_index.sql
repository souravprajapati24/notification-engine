CREATE UNIQUE INDEX IF NOT EXISTS idx_summary_unique
    ON notification_delivery_summary(delivery_date, channel, status);