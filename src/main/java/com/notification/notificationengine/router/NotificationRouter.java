package com.notification.notificationengine.router;


import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.enums.EventStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.service.channel.EmailNotificationService;
import com.notification.notificationengine.service.channel.SmsNotificationService;
import com.notification.notificationengine.service.channel.WebSocketNotificationService;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationRouter {

    private final EmailNotificationService emailService;
    private final SmsNotificationService smsService;
    private final WebSocketNotificationService webSocketService;

    public void route(NotificationEvent event) {
        try {
            log.info("▼ Routing notification - ID: {}, User: {}, Channels: {}",
                    event.getId(),
                    event.getUserId(),
                    event.getChannels()
            );

            event.setStatus(EventStatus.PROCESSING);

            for (NotificationChannel channel : event.getChannels()) {
                switch (channel) {
                    case EMAIL:
                        log.debug("Dispatching to EMAIL channel - Event: {}", event.getId());
                        emailService.deliver(event);
                        break;

                    case SMS:
                        log.debug("Dispatching to SMS channel - Event: {}", event.getId());
                        smsService.deliver(event);
                        break;

                    case WEBSOCKET:
                        log.debug("Dispatching to WEBSOCKET channel - Event: {}", event.getId());
                        webSocketService.deliver(event);
                        break;

                    default:
                        log.warn("⚠ Unsupported notification channel: {}", channel);
                }
            }

            log.info("✓ Routed to all channels - ID: {}", event.getId());

        } catch (Exception e) {
            log.error("✗ Error routing notification - ID: {}, Error: {}",
                    event.getId(),
                    e.getMessage(),
                    e
            );

        }
    }

    public void routeFromDto(Object eventDto, String eventId) {
        log.debug("Routing from DTO - EventId: {}", eventId);
    }
}