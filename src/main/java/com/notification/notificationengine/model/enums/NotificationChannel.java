package com.notification.notificationengine.model.enums;

public enum NotificationChannel {
    EMAIL("email", "Send via SMTP/Gmail"),
    SMS("sms", "Send via Twilio/SMS API"),
    WEBSOCKET("websocket", "Send via WebSocket for real-time browser push"),
    PUSH_NOTIFICATION("push", "Send via mobile push (future)"),
    SLACK("slack", "Send via Slack API (future)");

    private final String code;
    private final String description;

    NotificationChannel(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}