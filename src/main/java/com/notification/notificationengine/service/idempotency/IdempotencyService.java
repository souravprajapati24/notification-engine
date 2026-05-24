package com.notification.notificationengine.service.idempotency;

import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.repository.NotificationEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;


@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final NotificationEventRepository eventRepository;

    @Transactional(readOnly = true)
    public boolean isAlreadyProcessed(String idempotencyKey) {
        boolean exists = eventRepository.existsByIdempotencyKey(idempotencyKey);

        if (exists) {
            log.warn(
                    "⚠ Duplicate message detected (already processed) - Key: {}",
                    idempotencyKey
            );
        }

        return exists;
    }

    @Transactional(readOnly = true)
    public Optional<NotificationEvent> getPreviouslyProcessed(String idempotencyKey) {
        return eventRepository.findByIdempotencyKey(idempotencyKey);
    }


    public String generateKeyFromEventId(UUID eventId) {
        return NotificationEvent.generateIdempotencyKey(eventId);
    }

    @Transactional(readOnly = true)
    public long getProcessedCount() {
        return eventRepository.count();
    }
}