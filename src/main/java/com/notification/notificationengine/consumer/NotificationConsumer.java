package com.notification.notificationengine.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.notificationengine.dto.NotificationEventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topics.notification-events}")
    public void consumeEvent(String message , Acknowledgment ack){
        try{
            log.info("Received raw message {}",message);
            NotificationEventDto eventDto = objectMapper.readValue(message, NotificationEventDto.class);
            log.info("Deserialized event {}",eventDto);
            ack.acknowledge();
            log.info("Message acknowledged for event {}",eventDto.getEventId());
        }
        catch (Exception e){
            log.error("Error processing kafka message" ,e);
        }
    }
}
