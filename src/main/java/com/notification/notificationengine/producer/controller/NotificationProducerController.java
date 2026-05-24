package com.notification.notificationengine.producer.controller;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.producer.service.NotificationProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
@Slf4j
public class NotificationProducerController {

    private final NotificationProducer notificationProducer;

    @PostMapping("/publish")
    public ResponseEntity<Map<String, Object>> publishNotification(
            @Valid @RequestBody NotificationEventDto eventDto
    ) {
        try {
            log.info("▼ API Request - Publish notification - User: {}, Type: {}",
                    eventDto.getUserId(),
                    eventDto.getEventType()
            );

            notificationProducer.publishEvent(eventDto);

            return ResponseEntity.accepted().body(Map.of(
                    "status", "SUCCESS",
                    "message", "Notification published successfully",
                    "userId", eventDto.getUserId(),
                    "eventType", eventDto.getEventType(),
                    "channels", eventDto.getChannels()
            ));

        } catch (Exception e) {
            log.error("✗ Failed to publish notification - Error: {}", e.getMessage(), e);

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to publish notification",
                    "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/publish/test")
    public ResponseEntity<Map<String, Object>> testPublish() {
        try {
            NotificationEventDto testEvent = NotificationEventDto.builder()
                    .userId("test-user")
                    .eventType("TEST_EVENT")
                    .message("This is a test notification")
                    .channels(java.util.List.of(
                            com.notification.notificationengine.model.enums.NotificationChannel.EMAIL,
                            com.notification.notificationengine.model.enums.NotificationChannel.SMS,
                            com.notification.notificationengine.model.enums.NotificationChannel.WEBSOCKET
                    ))
                    .build();

            notificationProducer.publishEvent(testEvent);

            return ResponseEntity.accepted().body(Map.of(
                    "status", "SUCCESS",
                    "message", "Test notification published"
            ));

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "error", e.getMessage()
            ));
        }
    }
}