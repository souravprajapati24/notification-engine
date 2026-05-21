package com.notification.notificationengine.service.channel;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.model.NotificationEvent;
import org.springframework.scheduling.annotation.Async;

import java.util.Map;

public interface WebSocketNotificationService {

    @Async("taskExecutor")
    void deliver(NotificationEvent event);

    void sendDirectMessage(String userId, String messageType, Map<String, Object> payload);

    void broadcast(String messageType, Map<String, Object> payload);
}
