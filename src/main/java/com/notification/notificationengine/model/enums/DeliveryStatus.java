package com.notification.notificationengine.model.enums;

public enum DeliveryStatus {
    PENDING,
    RETRYING,
    SENT,
    FAILED;

    public boolean isTerminal() {
        return this == SENT || this == FAILED;
    }

    public boolean isRetryable() {
        return this == PENDING || this == RETRYING;
    }
}