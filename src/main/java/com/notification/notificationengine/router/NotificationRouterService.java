package com.notification.notificationengine.router;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.enums.NotificationChannels;
import com.notification.notificationengine.service.channel.EmailNotificationService;
import com.notification.notificationengine.service.channel.SmsNotificationService;
import com.notification.notificationengine.service.channel.WebSocketNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationRouterService {

    private final EmailNotificationService emailNotificationService;
    private final SmsNotificationService smsNotificationService;
    private final WebSocketNotificationService webSocketNotificationService;

    public void routeNotification(NotificationEventDto eventDto){

        log.info("Routing notification for event {} to channels: {}"
                , eventDto.getEventId(), eventDto.getChannels());

        for(NotificationChannels channel : eventDto.getChannels()){

            switch (channel){
                case EMAIL -> emailNotificationService.sendEmail(eventDto);
                case SMS -> smsNotificationService.sendSms(eventDto);
                case WEBSOCKET -> webSocketNotificationService.sendWebSocketNotification(eventDto);
                default -> log.warn("Unsupported notification channel: {}", channel);
            }
        }
    }

}
























