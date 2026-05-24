package com.notification.notificationengine.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.model.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.notification-events}")
    private String topic;

    public void publishEvent(NotificationEventDto eventDto) {
        try {
            log.info("▼ Publishing notification event - User: {}, Type: {}, Channels: {}",
                    eventDto.getUserId(),
                    eventDto.getEventType(),
                    eventDto.getChannels()
            );

            NotificationEvent event = NotificationEvent.builder()
                    .id(UUID.randomUUID())
                    .userId(eventDto.getUserId())
                    .eventType(eventDto.getEventType())
                    .message(eventDto.getMessage())
                    .channels(eventDto.getChannels())
                    .metadata(eventDto.getMetadata())
                    .build();

            String eventJson = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(topic, event.getUserId(), eventJson);

            log.info("✓ Event published to Kafka - Topic: {}, User: {}", topic, eventDto.getUserId());

        } catch (Exception e) {
            log.error("✗ Failed to publish event - User: {}, Error: {}",
                    eventDto.getUserId(),
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to publish notification event", e);
        }
    }
}