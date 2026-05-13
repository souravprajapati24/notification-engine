package com.notification.notificationengine.service.channel;

import com.notification.notificationengine.dto.NotificationEventDto;

public interface WebSocketNotificationService {
    void sendWebSocket(NotificationEventDto eventDto);
}
