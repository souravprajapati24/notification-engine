package com.notification.notificationengine.model.enums;

public enum EventStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    PARTIAL,
    FAILED;

    public boolean isComplete() {
        return this == COMPLETED || this == PARTIAL || this == FAILED;
    }

    public boolean isSuccess() {
        return this == COMPLETED || this == PARTIAL;
    }
}