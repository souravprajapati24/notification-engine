package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.service.channel.WebSocketNotificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class WebSocketNotificationServiceImpl implements WebSocketNotificationService {
    @Override
    public void sendWebSocket(NotificationEventDto eventDto) {

        log.info(
                "Sending WEBSOCKET notification for user {} and event {}",
                eventDto.getUserId(),
                eventDto.getEventId()
        );
    }
}
