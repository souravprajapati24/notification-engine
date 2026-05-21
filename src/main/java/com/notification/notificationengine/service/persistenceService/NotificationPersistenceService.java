package com.notification.notificationengine.service.persistenceService;

import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.model.enums.NotificationChannel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

public interface NotificationPersistenceService {

    NotificationEvent saveEventFromKafka(NotificationEvent event);

    @Transactional
    NotificationEvent persistEventLogs(NotificationEvent event);

    void createLogEntriesForChannels(
            UUID eventId,
            String userId,
            List<NotificationChannel> channels
    );

    void markChannelDelivered(
            UUID eventId,
            NotificationChannel channel,
            String recipient
    );

    void markChannelFailed(
            UUID eventId,
            NotificationChannel channel,
            String reason,
            String errorCode
    );

    @Transactional
    boolean markChannelForRetry(
            UUID eventId,
            NotificationChannel channel,
            String failureReason,
            String failureCode
    );

    boolean isRetriable(String failureCode);

    @Transactional(readOnly = true)
    Page<NotificationLog> findReadyForRetry(Pageable pageable);

    void updateEventStatus(UUID eventId);
}