package com.notification.notificationengine.dto.websocket;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
public class NotificationAckDto {

    private UUID eventId;

    private String userId;

    private LocalDateTime acknowledgedAt;
}