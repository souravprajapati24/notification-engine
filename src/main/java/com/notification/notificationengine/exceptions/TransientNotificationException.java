package com.notification.notificationengine.exceptions;

public class TransientNotificationException extends NotificationException {
    public TransientNotificationException(String message, Throwable cause) {
        super(message, cause);
    }
}
