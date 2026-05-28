package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.service.channel.WebSocketNotificationService;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class WebSocketNotificationServiceImpl implements WebSocketNotificationService {

    private final SimpMessagingTemplate messagingTemplate;
    private final NotificationPersistenceService persistenceService;

    @Async("deliveryExecutor")
    @Override
    public void deliver(NotificationEvent event) {

        try {
           String userId = event.getUserId();

            if (userId == null || userId.isBlank()) {
                throw new IllegalArgumentException("Missing userId for event: " + event.getId());
            }

            log.debug("Preparing WebSocket delivery - Event: {}, User: {}", event.getId(), userId);

            Map<String, Object> wsPayload = Map.of(
                    "eventId", event.getId().toString(),
                    "userId", event.getUserId(),
                    "type", event.getEventType(),
                    "message", event.getMessage(),
                    "timestamp", System.currentTimeMillis()
            );

            messagingTemplate.convertAndSendToUser(
                    userId,
                    "/queue/notifications",
                    wsPayload
            );

            log.info("✓ WebSocket message sent - Event: {}, User: {}", event.getId(), userId);

            persistenceService.markChannelDelivered(
                    event.getId(),
                    NotificationChannel.WEBSOCKET,
                    "/queue/notifications"
            );

        }  catch (Exception e) {

            String errorCode = categorizeError(e);
            if (persistenceService.isRetriable(errorCode)) {

                boolean retryScheduled = persistenceService.markChannelForRetry(
                        event.getId(),
                        NotificationChannel.WEBSOCKET,
                        e.getMessage(),
                        errorCode
                );

                if (retryScheduled) {
                    log.warn(
                            "⟳ WebSocket delivery retriable error - Event: {}, Error: {}, Will retry",
                            event.getId(),
                            errorCode
                    );

                } else {
                    log.error(
                            "✗ WebSocket delivery permanently failed after retries exhausted - Event: {}, Error: {}",
                            event.getId(),
                            errorCode
                    );
                }

            } else {
                persistenceService.markChannelFailed(
                        event.getId(),
                        NotificationChannel.WEBSOCKET,
                        e.getMessage(),
                        errorCode
                );
                persistenceService.sendToDlt(
                        event.getId(),
                        NotificationChannel.WEBSOCKET,
                        e.getMessage(),
                        errorCode
                );
                log.error(
                        "✗ WebSocket delivery failed permanently - Event: {}, Error: {}",
                        event.getId(),
                        errorCode
                );
            }
        }
    }

    @Override
    public void sendDirectMessage(String userId, String messageType, Map<String, Object> payload) {
        try {
            Map<String, Object> wsMessage = Map.of(
                    "type", messageType,
                    "data", payload,
                    "timestamp", System.currentTimeMillis()
            );

            messagingTemplate.convertAndSendToUser(userId, "/queue/notifications", wsMessage);
            log.debug("Direct WebSocket message sent - User: {}, Type: {}", userId, messageType);

        } catch (Exception e) {
            log.warn("Failed to send direct message to user {} - Error: {}", userId, e.getMessage());
        }
    }

    @Override
    public void broadcast(String messageType, Map<String, Object> payload) {
        try {
            Map<String, Object> wsMessage = Map.of(
                    "type", messageType,
                    "data", payload,
                    "timestamp", System.currentTimeMillis()
            );

            messagingTemplate.convertAndSend("/topic/broadcast", wsMessage);
            log.info("Broadcast WebSocket message sent - Type: {}", messageType);

        } catch (Exception e) {
            log.error(
                    "Failed to broadcast WebSocket message - Error: {}",
                    e.getMessage(),
                    e
            );
        }
    }

    private String categorizeError(Exception e) {
        String message = e.getMessage();
        if (message == null) return "WS_UNKNOWN_ERROR";

        String msg = message.toLowerCase();

        if (msg.contains("not connected") || msg.contains("offline")) {
            return "WS_NO_SESSION";
        } else if (msg.contains("connection closed") || msg.contains("session closed")) {
            return "WS_CONNECTION_CLOSED";
        } else if (msg.contains("size") || msg.contains("large")) {
            return "WS_MESSAGE_TOO_LARGE";
        } else if (msg.contains("timeout")) {
            return "WS_TIMEOUT";
        } else if (msg.contains("user not found") || msg.contains("invalid user") || msg.contains("userid")) {
            return "WS_INVALID_USER";
        } else {
            return "WS_UNKNOWN_ERROR";
        }
    }
}