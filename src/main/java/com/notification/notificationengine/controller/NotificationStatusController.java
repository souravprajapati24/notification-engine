package com.notification.notificationengine.controller;

import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.service.idempotency.IdempotencyService;
import com.notification.notificationengine.service.notification.NotificationQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
@Slf4j
public class NotificationStatusController {

    private final NotificationQueryService notificationQueryService;
    private final IdempotencyService idempotencyService;

    @GetMapping("/{userId}")
    public ResponseEntity<Map<String, Object>> getUserNotificationHistory(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return ResponseEntity.ok(
                notificationQueryService.getUserNotificationHistory(userId, page, size));
    }

    @GetMapping("/{eventId}/details")
    public ResponseEntity<Map<String, Object>> getNotificationDetails(
            @PathVariable String eventId
    ) {
        return ResponseEntity.ok(
                notificationQueryService.getNotificationDetails(eventId));
    }

    @GetMapping("/dashboard/overview")
    public ResponseEntity<Map<String, Object>> getDashboardOverview() {
        return ResponseEntity.ok(
                notificationQueryService.getDashboardOverview());
    }

    @GetMapping("/dashboard/trend")
    public ResponseEntity<List<Map<String, Object>>> getDeliveryTrend(
            @RequestParam(defaultValue = "7") int days
    ) {
        return ResponseEntity.ok(
                notificationQueryService.getDeliveryTrend(days));
    }

    @GetMapping("/dashboard/retry-analytics")
    public ResponseEntity<Map<String, Object>> getRetryAnalytics(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return ResponseEntity.ok(
                notificationQueryService.getRetryAnalytics(page, size));
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getDeliveryStatistics() {
        return ResponseEntity.ok(
                notificationQueryService.getDeliveryStatistics());
    }

    @GetMapping("/failed")
    public ResponseEntity<Map<String, Object>> getFailedDeliveries(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        return ResponseEntity.ok(
                notificationQueryService.getFailedDeliveries(page, size));
    }

    @GetMapping("/by-channel/{channel}")
    public ResponseEntity<Map<String, Object>> getByChannel(
            @PathVariable String channel,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        return ResponseEntity.ok(
                notificationQueryService.getByChannel(channel, page, size));
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
        return ResponseEntity.ok(
                notificationQueryService.getUserHistoryByDateRange(userId, from, to, page, size));
    }

    @GetMapping("/check-duplicate/{idempotencyKey}")
    public ResponseEntity<Map<String, Object>> checkDuplicate(
            @PathVariable String idempotencyKey
    ) {
        boolean isProcessed = idempotencyService.isAlreadyProcessed(idempotencyKey);

        if (isProcessed) {
            var previousEvent = idempotencyService.getPreviouslyProcessed(idempotencyKey);
            return ResponseEntity.ok(Map.of(
                    "isDuplicate",      true,
                    "alreadyProcessed", true,
                    "previousEventId", Objects.requireNonNull(previousEvent.map(NotificationEvent::getId).orElse(null)),
                    "idempotencyKey",   idempotencyKey,
                    "message",          "This message (eventId) was already processed before"
            ));
        }

        return ResponseEntity.ok(Map.of(
                "isDuplicate",      false,
                "alreadyProcessed", false,
                "idempotencyKey",   idempotencyKey,
                "message",          "This is a new message (not yet processed)"
        ));
    }

    @GetMapping("/idempotency-stats")
    public ResponseEntity<Map<String, Object>> getIdempotencyStats() {
        long totalProcessed = idempotencyService.getProcessedCount();
        return ResponseEntity.ok(
                notificationQueryService.getIdempotencyStats(totalProcessed));
    }
}