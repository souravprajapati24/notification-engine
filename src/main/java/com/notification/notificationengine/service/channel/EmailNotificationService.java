package com.notification.notificationengine.service.channel;

import com.notification.notificationengine.dto.NotificationEventDto;

public interface EmailNotificationService {
    void sendEmail(NotificationEventDto eventDto);
}
