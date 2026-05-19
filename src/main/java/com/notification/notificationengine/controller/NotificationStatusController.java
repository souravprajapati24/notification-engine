package com.notification.notificationengine.controller;

import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.repository.NotificationEventRepository;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
@Slf4j
public class NotificationStatusController {

    private final NotificationEventRepository eventRepository;
    private final NotificationLogRepository logRepository;

    @GetMapping("/{userId}")
    public ResponseEntity<Map<String, Object>> getUserNotificationHistory(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        log.debug("Fetching notification history for user: {}, page: {}, size: {}", userId, page, size);

        try {
            Pageable pageable = PageRequest.of(page, size);
            Page<NotificationLog> logs = logRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);


            return ResponseEntity.ok(Map.of(
                    "userId", userId,
                    "totalNotifications", logs.getTotalElements(),
                    "totalPages", logs.getTotalPages(),
                    "currentPage", page,
                    "pageSize", size,
                    "logs", logs.getContent()
            ));

        } catch (Exception e) {
            log.error("Error fetching notification history for user {}: {}", userId, e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch notification history"));
        }
    }

    @GetMapping("/{eventId}/details")
    public ResponseEntity<Map<String, Object>> getNotificationDetails(
            @PathVariable String eventId
    ) {
        log.debug("Fetching details for event: {}", eventId);

        try {
            UUID uuid = UUID.fromString(eventId);
            Optional<NotificationEvent> event = eventRepository.findById(uuid);

            if (event.isEmpty()) {
                log.warn("Event not found: {}", eventId);
                return ResponseEntity.notFound().build();
            }

            List<NotificationLog> logs = logRepository.findByEventIdOrderByCreatedAt(uuid);

            long successCount = logs.stream()
                    .filter(l -> l.getStatus() == DeliveryStatus.SENT)
                    .count();

            long failureCount = logs.stream()
                    .filter(l -> l.getStatus() == DeliveryStatus.FAILED)
                    .count();

            return ResponseEntity.ok(Map.of(
                    "event", event.get(),
                    "deliveryLogs", logs,
                    "summary", Map.of(
                            "totalChannels", logs.size(),
                            "successfulDeliveries", successCount,
                            "failedDeliveries", failureCount,
                            "deliveryLatencies", calculateLatencies(logs)
                    )
            ));

        } catch (IllegalArgumentException e) {
            log.warn("Invalid event ID format: {}", eventId);
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error fetching event details for {}: {}", eventId, e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch event details"));
        }
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getDeliveryStatistics() {
        log.debug("Fetching delivery statistics");

        try {
            long pendingCount = logRepository.countByStatus(DeliveryStatus.PENDING);
            long sentCount = logRepository.countByStatus(DeliveryStatus.SENT);
            long failedCount = logRepository.countByStatus(DeliveryStatus.FAILED);
            long retiringCount = logRepository.countByStatus(DeliveryStatus.RETRYING);
            long totalCount = pendingCount + sentCount + failedCount + retiringCount;

            double successRate = totalCount > 0 ? (sentCount * 100.0 / totalCount) : 0;

            return ResponseEntity.ok(Map.of(
                    "timestamp", LocalDateTime.now(),
                    "total", totalCount,
                    "delivered", sentCount,
                    "pending", pendingCount,
                    "retrying", retiringCount,
                    "failed", failedCount,
                    "successRate", String.format("%.2f%%", successRate)
            ));

        } catch (Exception e) {
            log.error("Error fetching statistics: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch statistics"));
        }
    }

    @GetMapping("/failed")
    public ResponseEntity<Map<String, Object>> getFailedDeliveries(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        log.debug("Fetching failed deliveries, page: {}, size: {}", page, size);

        try {
            Pageable pageable = PageRequest.of(page, size);
            Page<NotificationLog> failed = logRepository.findByStatusOrderByCreatedAtDesc(
                    DeliveryStatus.FAILED,
                    pageable
            );

            return ResponseEntity.ok(Map.of(
                    "totalFailed", failed.getTotalElements(),
                    "totalPages", failed.getTotalPages(),
                    "currentPage", page,
                    "pageSize", size,
                    "failedDeliveries", failed.getContent()
            ));

        } catch (Exception e) {
            log.error("Error fetching failed deliveries: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch failed deliveries"));
        }
    }

    @GetMapping("/by-channel/{channel}")
    public ResponseEntity<Map<String, Object>> getByChannel(
            @PathVariable String channel,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        log.debug("Fetching deliveries for channel: {}", channel);

        try {
            com.notification.notificationengine.model.enums.NotificationChannel notifChannel =
                    com.notification.notificationengine.model.enums.NotificationChannel.valueOf(channel.toUpperCase());

            Pageable pageable = PageRequest.of(page, size);
            Page<NotificationLog> logs = logRepository.findFailedByChannel(notifChannel, pageable);

            return ResponseEntity.ok(Map.of(
                    "channel", channel,
                    "totalFailed", logs.getTotalElements(),
                    "totalPages", logs.getTotalPages(),
                    "currentPage", page,
                    "failedLogs", logs.getContent()
            ));

        } catch (IllegalArgumentException e) {
            log.warn("Invalid channel: {}", channel);
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid channel: " + channel));
        } catch (Exception e) {
            log.error("Error fetching deliveries for channel {}: {}", channel, e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch channel statistics"));
        }
    }

    @GetMapping("/history/{userId}")
    public ResponseEntity<Map<String, Object>> getUserHistoryByDateRange(
            @PathVariable String userId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        log.debug("Fetching history for user: {} from: {} to: {}", userId, from, to);

        try {
            Pageable pageable = PageRequest.of(page, size);
            Page<NotificationLog> logs;

            if (from != null && to != null) {
                logs = logRepository.findByTimeRange(from, to, pageable);
            } else {
                logs = logRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
            }

            return ResponseEntity.ok(Map.of(
                    "userId", userId,
                    "from", from,
                    "to", to,
                    "totalNotifications", logs.getTotalElements(),
                    "logs", logs.getContent()
            ));

        } catch (Exception e) {
            log.error("Error fetching user history: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch user history"));
        }
    }

    private Map<String, Long> calculateLatencies(List<NotificationLog> logs) {
        Map<String, Long> latencies = new HashMap<>();
        for (NotificationLog log : logs) {
            Long latency = log.getDeliveryLatencySeconds();
            if (latency != null) {
                latencies.put(log.getChannel().toString(), latency);
            }
        }
        return latencies;
    }
}