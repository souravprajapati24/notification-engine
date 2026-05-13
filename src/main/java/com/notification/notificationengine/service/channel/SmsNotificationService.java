package com.notification.notificationengine.service.channel;

import com.notification.notificationengine.dto.NotificationEventDto;

public interface SmsNotificationService {
    void sendSms(NotificationEventDto eventDto);
}
