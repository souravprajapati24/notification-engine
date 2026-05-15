package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.service.channel.WebSocketNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class WebSocketNotificationServiceImpl
        implements WebSocketNotificationService {

    private final SimpMessagingTemplate messagingTemplate;

    @Override
    @Async
    public void sendWebSocketNotification(NotificationEventDto eventDto) {

        String userId = eventDto.getUserId();
        if (userId == null || userId.isBlank()) {
            log.warn("Cannot send WebSocket notification: missing userId for event {}",
                    eventDto.getEventId());
            return;
        }

        log.info(
                "Sending websocket notification to user {}",
                eventDto.getUserId()
        );

        messagingTemplate.convertAndSendToUser(
                userId,
                "/queue/notifications",
                eventDto
        );

        log.info(
                "WebSocket notification sent to user {} for event {}",
                userId,
                eventDto.getEventId()
        );
    }
}