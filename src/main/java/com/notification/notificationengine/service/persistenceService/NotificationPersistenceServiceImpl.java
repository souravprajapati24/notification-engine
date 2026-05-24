package com.notification.notificationengine.service.persistenceService;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.notificationengine.dto.DltMessagePayloadDto;
import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.model.enums.EventStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.repository.NotificationEventRepository;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String , String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.notification-events}.DLT")
    private  String dltTopic;

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

    @Transactional
    @Override
    public boolean markChannelForRetry(
            UUID eventId,
            NotificationChannel channel,
            String failureReason,
            String failureCode
    ) {
        try {
            var logEntry = logRepository.findByEventIdAndChannelAndStatus(
                    eventId,
                    channel,
                    DeliveryStatus.RETRYING
            ).or(() -> logRepository.findByEventIdAndChannelAndStatus(
                    eventId,
                    channel,
                    DeliveryStatus.PENDING
            ));

            if (logEntry.isPresent()) {
                NotificationLog logg = logEntry.get();

                logg.setRetryCount(logg.getRetryCount() + 1);
                logg.setFailureReason(failureReason);
                logg.setFailureCode(failureCode);
                logg.setLastRetryAt(LocalDateTime.now());

                Long backoffSeconds = getBackoffInterval(logg.getRetryCount());

                if (backoffSeconds == null) {
                    logg.setStatus(DeliveryStatus.FAILED);
                    log.warn(
                            "✗ Max retries exhausted - Event: {}, Channel: {}, Total attempts: {}",
                            eventId, channel, logg.getRetryCount()
                    );

                    logRepository.save(logg);
                    updateEventStatus(eventId);

                    sendToDlt(
                            eventId,
                            channel,
                            failureReason,
                            failureCode
                    );
                    return false;

                } else {
                    logg.setStatus(DeliveryStatus.RETRYING);
                    LocalDateTime nextRetry = LocalDateTime.now().plusSeconds(backoffSeconds);
                    logg.setNextRetryAt(nextRetry);

                    log.info(
                            "⟳ Scheduled for retry - Event: {}, Channel: {}, Attempt: {}, Wait: {}s, NextRetry: {}",
                            eventId, channel, logg.getRetryCount(), backoffSeconds, nextRetry
                    );
                    logRepository.save(logg);
                    return true;
                }

            }

            return false;

        } catch (Exception e) {
            log.error(
                    "✗ Failed to mark channel for retry - Event: {}, Channel: {}, Error: {}",
                    eventId, channel, e.getMessage(), e
            );
            throw new RuntimeException("Failed to schedule retry", e);
        }
    }


    private void sendToDlt(
            UUID eventId,
            NotificationChannel channel,
            String failureReason,
            String failureCode
    ) {
        try {

            var eventOpt = eventRepository.findById(eventId);
            if (eventOpt.isEmpty()) {
                log.warn("Event not found for DLT - EventId: {}", eventId);
                return;
            }

            NotificationEvent notifEvent = eventOpt.get();

            var logEntry = logRepository.findByEventIdAndChannelAndStatus(
                    eventId,
                    channel,
                    DeliveryStatus.FAILED
            );

            DltMessagePayloadDto payload = DltMessagePayloadDto.builder()
                    .eventId(eventId)
                    .userId(notifEvent.getUserId())
                    .channel(channel.toString())
                    .failureCode(failureCode)
                    .failureReason(failureReason)
                    .retryCount(logEntry.map(NotificationLog::getRetryCount).orElse(0))
                    .failedAt(LocalDateTime.now())
                    .eventType(notifEvent.getEventType())
                    .message(notifEvent.getMessage())
                    .build();

            String dltMessage = objectMapper.writeValueAsString(payload);

            kafkaTemplate.send(dltTopic, eventId.toString(), dltMessage).get();

            log.error(
                    "→ Message sent to DLT - Topic: {}, EventId: {}, Channel: {}, Code: {}",
                    dltTopic, eventId, channel, failureCode
            );

        } catch (JsonProcessingException e) {
            log.error(
                    "⚠ Failed to serialize DLT message - EventId: {}, Error: {}",
                    eventId, e.getMessage()
            );
        } catch (Exception e) {
            log.error(
                    "⚠ Failed to send message to DLT - EventId: {}, Error: {}",
                    eventId, e.getMessage()
            );
        }
    }

private Long getBackoffInterval(int retryCount) {
        return switch (retryCount) {
            case 0 -> 5L;
            case 1 -> 30L;
            case 2 -> 120L;
            default -> null;
        };
    }

    @Override
    public boolean isRetriable(String failureCode) {

        if (failureCode == null) {
            return false;
        }

        return switch (failureCode) {

            case "EMAIL_TIMEOUT",
                 "EMAIL_CONNECTION_ERROR",
                 "EMAIL_TEMPORARY_FAILURE",
                 "EMAIL_RATE_LIMITED" -> true;

            case "EMAIL_INVALID_RECIPIENT",
                 "EMAIL_INVALID_FORMAT",
                 "EMAIL_AUTH_FAILED",
                 "EMAIL_UNKNOWN_ERROR" -> false;

            case "SMS_TIMEOUT",
                 "SMS_CONNECTION_ERROR",
                 "SMS_NETWORK_ERROR",
                 "SMS_RATE_LIMITED",
                 "SMS_TWILIO_INTERNAL_ERROR" -> true;

            case "SMS_INVALID_RECIPIENT",
                 "SMS_INVALID_MESSAGE",
                 "SMS_AUTH_FAILED",
                 "SMS_CREDENTIALS_ERROR",
                 "SMS_UNKNOWN_ERROR" -> false;

            case "WS_TIMEOUT",
                 "WS_CONNECTION_ERROR",
                 "WS_CONNECTION_CLOSED" -> true;

            case "WS_NO_SESSION",
                 "WS_INVALID_USER",
                 "WS_MESSAGE_TOO_LARGE",
                 "WS_UNKNOWN_ERROR" -> false;

            default -> false;
        };
    }

    @Transactional(readOnly = true)
    @Override
    public Page<NotificationLog> findReadyForRetry(Pageable pageable) {
        return logRepository.findReadyForRetry(pageable);
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