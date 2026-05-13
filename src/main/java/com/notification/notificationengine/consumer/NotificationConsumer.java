package com.notification.notificationengine.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.exceptions.InvalidNotificationEventException;
import com.notification.notificationengine.exceptions.TransientNotificationException;
import com.notification.notificationengine.router.NotificationRouterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final ObjectMapper objectMapper;
    private final NotificationRouterService notificationRouterService;

    @KafkaListener(topics = "${app.kafka.topics.notification-events}")
    public void consumeEvent(
            @Payload String message ,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition ,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack){
        try{
            log.info(
                    "Received message from partition {} with offset {}",
                    partition, offset
            );

            NotificationEventDto eventDto;

            try{

                eventDto = objectMapper.readValue(message, NotificationEventDto.class);
                log.info("Deserialized event {}",eventDto.getEventId());
            }
            catch (Exception e){
                log.error("Failed to deserialize message from partition {} offset {} : {}"
                        , partition,offset,e.getMessage());
                ack.acknowledge();
                return;
            }
            try {
                validateEvent(eventDto);
                notificationRouterService.routeNotification(eventDto);
                log.info("Notification routed successfully for event {}"
                        ,eventDto.getEventId());
                ack.acknowledge();
                log.debug("Message for event {} (partition {} offset {}) acknowledged"
                        ,eventDto.getEventId(), partition, offset);
            }
            catch (TransientNotificationException e){
                log.warn(
                        "Transient error routing event {}: {}. Will retry later.",
                        eventDto.getEventId(), e.getMessage()
                );

                throw e;
            }
            catch (InvalidNotificationEventException e){
                log.error(
                        "Invalid notification event {}: {}",
                        eventDto.getEventId(), e.getMessage()
                );
                ack.acknowledge();
            }
            catch (Exception e){
                log.error(
                        "Unexpected error routing event {}: {}",
                        eventDto.getEventId(), e.getMessage(), e
                );
                throw new TransientNotificationException(
                        "Unknown error for event: " + eventDto.getEventId(), e
                );
            }

        }
        catch (Exception e){
            log.error(
                    "Critical error in message consumer at partition {} offset {}: {}",
                    partition, offset, e.getMessage(), e
            );
            throw new TransientNotificationException(
                    "Consumer error at partition " + partition + " offset " + offset, e
            );
        }

    }

    private void validateEvent(NotificationEventDto eventDto){
        if (eventDto == null) {
            throw new InvalidNotificationEventException("Event data is null");
        }
        if (eventDto.getEventId() == null) {
            throw new InvalidNotificationEventException("Event ID is missing");
        }
        if (eventDto.getChannels() == null || eventDto.getChannels().isEmpty()) {
            throw new InvalidNotificationEventException("Channels not specified for event: " + eventDto.getEventId());
        }
    }
}
