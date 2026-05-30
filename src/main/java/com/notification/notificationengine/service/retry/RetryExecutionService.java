package com.notification.notificationengine.service.retry;

import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.repository.NotificationEventRepository;
import com.notification.notificationengine.router.NotificationRouter;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class RetryExecutionService {
    private final NotificationEventRepository eventRepository;
    private final NotificationRouter router;
    private final NotificationPersistenceService persistenceService;


    @Transactional
    public void retryDelivery(NotificationLog retryLog) {
        try {
            UUID eventId = retryLog.getEventId();

            var event = eventRepository.findById(eventId);

            if (event.isEmpty()) {
                log.warn("⚠ Original event not found for retry - EventId: {}", eventId);
                return;
            }

            NotificationEvent originalEvent = event.get();

            log.info(
                    "⟳ Retrying delivery - EventId: {}, Channel: {}, Attempt: {}/3",
                    eventId,
                    retryLog.getChannel(),
                    retryLog.getRetryCount()
            );

            NotificationEvent eventForChannel = getNotificationEvent(retryLog, originalEvent);

            switch (retryLog.getChannel()) {
                case EMAIL:
                    log.debug("Retrying EMAIL delivery for event {}", eventId);
                    break;
                case SMS:
                    log.debug("Retrying SMS delivery for event {}", eventId);
                    break;
                case WEBSOCKET:
                    log.debug("Retrying WEBSOCKET delivery for event {}", eventId);
                    break;
                default:
                    log.warn("⚠ Unknown channel for retry: {}", retryLog.getChannel());
            }

            router.route(eventForChannel);

        } catch (Exception e) {
            log.error(
                    "✗ Failed to retry delivery - LogId: {}, Error: {}",
                    retryLog.getId(),
                    e.getMessage(),
                    e
            );
        }
    }

    private static NotificationEvent getNotificationEvent(NotificationLog retryLog, NotificationEvent originalEvent) {
        NotificationEvent eventForChannel = new NotificationEvent();
        eventForChannel.setId(originalEvent.getId());
        eventForChannel.setUserId(originalEvent.getUserId());
        eventForChannel.setEventType(originalEvent.getEventType());
        eventForChannel.setMessage(originalEvent.getMessage());
        eventForChannel.setChannels(List.of(retryLog.getChannel()));
        eventForChannel.setMetadata(originalEvent.getMetadata());
        eventForChannel.setStatus(originalEvent.getStatus());
        return eventForChannel;
    }

}
