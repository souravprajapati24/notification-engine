package com.notification.notificationengine.controller.websocket;

import com.notification.notificationengine.dto.websocket.NotificationAckDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;

@Slf4j
@Controller
public class NotificationAckController {

    @MessageMapping("/ack")
    public void acknowledgeNotification(
            NotificationAckDto ackDto
    ) {

        log.info(
                "Notification ACK received: eventId={}, userId={}, acknowledgedAt={}",
                ackDto.getEventId(),
                ackDto.getUserId(),
                ackDto.getAcknowledgedAt()
        );
    }
}