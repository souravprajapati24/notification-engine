package com.notification.notificationengine.dto;

import com.notification.notificationengine.enums.NotificationChannels;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class NotificationEventDto {
    private UUID eventId;
    private String email;
    private String userId;
    private String phoneNumber;
    private List<NotificationChannels> channels;
    private String subject;
    private String message;
    private LocalDateTime createdAt;
}
