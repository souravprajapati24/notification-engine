package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.service.channel.WebSocketNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class WebSocketNotificationServiceImpl implements WebSocketNotificationService {

    private final SimpMessagingTemplate simpMessagingTemplate;

    @Override
    public void sendWebSocketNotification(NotificationEventDto eventDto) {

        if(eventDto==null){
            log.warn("Cannot send Websocket notification: event is null");
            return;
        }

        try {
            log.info("Sending Websocket notification for event {}",eventDto.getEventId());

            simpMessagingTemplate.convertAndSend("/topic/notifications",eventDto);
            log.info("Websocket notification sent successfully for event {}",eventDto.getEventId());
        }
        catch (Exception e){
            log.error("Failed to send Websocket notification for event {}"
                    , eventDto.getEventId(), e);
        }
    }
}
