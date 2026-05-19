package com.notification.notificationengine.service.persistenceService;


import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.model.enums.EventStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.repository.NotificationEventRepository;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationPersistenceServiceImpl implements NotificationPersistenceService {

    private final NotificationEventRepository eventRepository;
    private final NotificationLogRepository logRepository;

    @Override
    @Transactional
    public NotificationEvent saveEventFromKafka(NotificationEvent event) {
        try {
            event.setStatus(EventStatus.PENDING);
            NotificationEvent saved = eventRepository.save(event);

            log.info(
                    "✓ Event saved - ID: {}, User: {}, Type: {}, Channels: {}",
                    saved.getId(),
                    saved.getUserId(),
                    saved.getEventType(),
                    saved.getChannels()
            );
            return saved;

        } catch (Exception e) {
            log.error(
                    "✗ Failed to save event - User: {}, Type: {}, Error: {}",
                    event.getUserId(),
                    event.getEventType(),
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to persist notification event", e);
        }
    }

    @Transactional
    @Override
    public NotificationEvent persistEventLogs(NotificationEvent event){

        NotificationEvent savedEvent = saveEventFromKafka(event);
        log.debug("Event persisted - ID: {}", savedEvent.getId());

        createLogEntriesForChannels(
                savedEvent.getId(),
                savedEvent.getUserId(),
                savedEvent.getChannels()
        );

        log.debug("Delivery logs created - Event: {}, Channels: {}",
                savedEvent.getId(),
                savedEvent.getChannels().size()
        );

        return savedEvent;

    }

    @Override
    @Transactional
    public void createLogEntriesForChannels(
            UUID eventId,
            String userId,
            List<NotificationChannel> channels
    ) {
        try {
            for (NotificationChannel channel : channels) {
                NotificationLog logg = NotificationLog.builder()
                        .eventId(eventId)
                        .userId(userId)
                        .channel(channel)
                        .status(DeliveryStatus.PENDING)
                        .retryCount(0)
                        .build();

                logRepository.save(logg);
                log.debug("Created log entry for event {} channel {}", eventId, channel);

            }

            log.info(
                    "✓ Created {} delivery logs for event {}",
                    channels.size(),
                    eventId
            );

        } catch (Exception e) {
            log.error(
                    "✗ Failed to create delivery logs for event {} - Error: {}",
                    eventId,
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to create delivery logs", e);
        }
    }

    @Override
    @Transactional
    public void markChannelDelivered(
            UUID eventId,
            NotificationChannel channel,
            String recipient
    ) {
        try {
            var logEntry = logRepository.findByEventIdAndChannelAndStatus(
                    eventId,
                    channel,
                    DeliveryStatus.PENDING
            );

            if (logEntry.isPresent()) {
                NotificationLog logg = logEntry.get();
                logg.setStatus(DeliveryStatus.SENT);
                logg.setSentAt(LocalDateTime.now());
                logg.setRecipient(recipient);
                logRepository.save(logg);

                log.info(
                        "✓ Delivery successful - Event: {}, Channel: {}, Recipient: {}",
                        eventId,
                        channel,
                        maskSensitiveData(recipient)
                );
                updateEventStatus(eventId);
            } else {

                log.warn(
                        "⚠ No PENDING log entry for event {} channel {} - possibly already processed",
                        eventId,
                        channel
                );
            }

        } catch (Exception e) {
            log.error(
                    "✗ Failed to mark delivery as sent - Event: {}, Channel: {}, Error: {}",
                    eventId,
                    channel,
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to update delivery status", e);
        }
    }


    @Override
    @Transactional
    public void markChannelFailed(
            UUID eventId,
            NotificationChannel channel,
            String failureReason,
            String failureCode
    ) {
        try {

            var logEntry = logRepository.findByEventIdAndChannelAndStatus(
                    eventId,
                    channel,
                    DeliveryStatus.PENDING
            );

            if (logEntry.isPresent()) {
                NotificationLog logg = logEntry.get();
                logg.setStatus(DeliveryStatus.FAILED);
                logg.setFailureReason(failureReason);
                logg.setFailureCode(failureCode);
                logRepository.save(logg);


                log.warn(
                        "✗ Delivery failed - Event: {}, Channel: {}, Code: {}, Reason: {}",
                        eventId,
                        channel,
                        failureCode,
                        failureReason
                );
                updateEventStatus(eventId);
            } else {
                log.warn(
                        "⚠ No PENDING log entry for event {} channel {} - skipping failure update",
                        eventId,
                        channel
                );
            }

        } catch (Exception e) {
            log.error(
                    "✗ Failed to mark delivery as failed - Event: {}, Channel: {}, Error: {}",
                    eventId,
                    channel,
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to update delivery failure status", e);
        }
    }

    @Override
    @Transactional
    public void updateEventStatus(UUID eventId) {
        try {
            List<NotificationLog> logs = logRepository.findByEventIdOrderByCreatedAt(eventId);

            if (logs.isEmpty()) {
                log.warn("No log entries found for event {} - cannot update status", eventId);
                return;
            }

            long sentCount = logs.stream()
                    .filter(l -> l.getStatus() == DeliveryStatus.SENT)
                    .count();

            long failedCount = logs.stream()
                    .filter(l -> l.getStatus() == DeliveryStatus.FAILED)
                    .count();

            long totalCount = logs.size();

            EventStatus newStatus;
            if (sentCount == totalCount) {
                newStatus = EventStatus.COMPLETED;

            } else if (sentCount > 0 && failedCount>0) {
                newStatus = EventStatus.PARTIAL;
            } else if (failedCount == totalCount) {
                newStatus = EventStatus.FAILED;
            } else {
                newStatus = EventStatus.PROCESSING;
            }

            var eventOpt = eventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                NotificationEvent event = eventOpt.get();
                EventStatus oldStatus = event.getStatus();
                event.setStatus(newStatus);
                event.setProcessedAt(LocalDateTime.now());
                eventRepository.save(event);

                log.info(
                        "✓ Event status updated - ID: {}, Old: {}, New: {}, Sent: {}/{}, Failed: {}",
                        eventId,
                        oldStatus,
                        newStatus,
                        sentCount,
                        totalCount,
                        failedCount
                );
            }

        } catch (Exception e) {
            log.error(
                    "✗ Failed to update event status - Event: {}, Error: {}",
                    eventId,
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to update event status", e);
        }
    }

    private String maskSensitiveData(String data) {
        if (data == null || data.length() < 4) {
            return "***";
        }

        if (data.contains("@")) {
            String[] parts = data.split("@");
            return parts[0].substring(0, Math.min(3, parts[0].length())) + "@***";
        } else if (data.contains("-") || data.startsWith("+")) {
            return data.substring(0, Math.min(4, data.length())) + "****" +
                    data.substring(Math.max(0, data.length() - 4));
        }

        return data.substring(0, 3) + "***" + data.substring(Math.max(3, data.length() - 3));
    }
}