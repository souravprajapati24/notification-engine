package com.notification.notificationengine.service.channel;

import com.notification.notificationengine.model.NotificationEvent;
import org.springframework.scheduling.annotation.Async;

public interface SmsNotificationService {

    @Async("taskExecutor")
    void deliver(NotificationEvent event);
}
