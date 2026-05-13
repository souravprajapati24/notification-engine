package com.notification.notificationengine.exceptions;

public class ChannelNotificationException extends NotificationException{
    private final String channel;

    public ChannelNotificationException(String channel, String message, Throwable cause) {
        super("Channel [" + channel + "] failed: " + message, cause);
        this.channel = channel;
    }

    public String getChannel() {
        return channel;
    }
}
