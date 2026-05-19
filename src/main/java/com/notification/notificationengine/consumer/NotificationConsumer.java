package com.notification.notificationengine.consumer;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.router.NotificationRouter;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final NotificationRouter router;
    private final NotificationPersistenceService persistenceService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = {"${app.kafka.topics.notification-events}"},
            groupId = "notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack
    ) {

        try {
            log.info("Kafka Message Received - Topic: {}, Partition: {}, Offset: {}",
                    topic, partition, offset);

            NotificationEvent event = objectMapper.readValue(message, NotificationEvent.class);

            log.debug("Parsed event - User: {}, Type: {}, Channels: {}",
                    event.getUserId(),
                    event.getEventType(),
                    event.getChannels()
            );


            NotificationEvent savedEvent = persistenceService.persistEventLogs(event);
            router.route(savedEvent);

            log.debug("Event routed to delivery services - ID: {}", savedEvent.getId());
            ack.acknowledge();

            log.info("Event processed successfully - ID: {}, Topic: {}, Offset: {}",
                    savedEvent.getId(),
                    topic,
                    offset
            );

        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            log.error("JSON parsing failed for message - Reason: {}", e.getMessage());
            throw new RuntimeException("Failed to parse notification event from Kafka", e);

        } catch (Exception e) {
            log.error("Error processing Kafka message - Topic: {}, Offset: {}, Error: {}",
                    topic,
                    offset,
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to process notification event", e);
        }
    }

}