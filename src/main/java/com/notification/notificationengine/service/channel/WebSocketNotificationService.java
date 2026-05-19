package com.notification.notificationengine.service.channel;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.model.NotificationEvent;
import org.springframework.scheduling.annotation.Async;

public interface WebSocketNotificationService {

    @Async("taskExecutor")
    void deliver(NotificationEvent event);
}
