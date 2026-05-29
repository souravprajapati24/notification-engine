package com.notification.notificationengine.service.notification;

import com.notification.notificationengine.exception.InvalidInputException;
import com.notification.notificationengine.exception.ResourceNotFoundException;
import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.model.enums.EventStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.repository.DltMessageRepository;
import com.notification.notificationengine.repository.NotificationEventRepository;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class NotificationQueryService {

    private final NotificationEventRepository eventRepository;
    private final NotificationLogRepository logRepository;
    private final DltMessageRepository dltRepository;


    public Map<String, Object> getUserNotificationHistory(String userId, int page, int size) {
        log.debug("Fetching notification history for user: {}, page: {}, size: {}", userId, page, size);

        Pageable pageable = PageRequest.of(page, size);
        Page<NotificationLog> logs = logRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);

        if (logs.isEmpty()) {
            throw new ResourceNotFoundException(
                    "No notifications found for user or user does not exist for Id: " + userId);
        }

        return Map.of(
                "userId", userId,
                "totalNotifications", logs.getTotalElements(),
                "totalPages", logs.getTotalPages(),
                "currentPage", page,
                "pageSize", size,
                "logs", logs.getContent()
        );
    }


    public Map<String, Object> getNotificationDetails(String eventId) {
        log.debug("Fetching details for event: {}", eventId);

        UUID uuid;
        try {
            uuid = UUID.fromString(eventId);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid UUID format for event ID: {}", eventId);
            throw new InvalidInputException("Invalid event ID format: " + eventId);
        }

        NotificationEvent event = eventRepository.findById(uuid)
                .orElseThrow(() -> new ResourceNotFoundException("Event not found for ID: " + eventId));

        List<NotificationLog> logs = logRepository.findByEventIdOrderByCreatedAt(uuid);

        long successCount = logs.stream()
                .filter(l -> l.getStatus() == DeliveryStatus.SENT)
                .count();

        long failureCount = logs.stream()
                .filter(l -> l.getStatus() == DeliveryStatus.FAILED)
                .count();

        return Map.of(
                "event", event,
                "deliveryLogs", logs,
                "summary", Map.of(
                        "totalChannels", logs.size(),
                        "successfulDeliveries", successCount,
                        "failedDeliveries", failureCount,
                        "deliveryLatencies", calculateLatencies(logs)
                )
        );
    }


    public Map<String, Object> getDashboardOverview() {
        log.debug("Building dashboard overview");

        long totalSent     = logRepository.countByStatus(DeliveryStatus.SENT);
        long totalFailed   = logRepository.countByStatus(DeliveryStatus.FAILED);
        long totalRetrying = logRepository.countByStatus(DeliveryStatus.RETRYING);
        long totalPending  = logRepository.countByStatus(DeliveryStatus.PENDING);
        long totalLogs     = totalSent + totalFailed + totalRetrying + totalPending;

        long totalEvents     = eventRepository.count();
        long completedEvents = eventRepository.countByStatus(EventStatus.COMPLETED);
        long failedEvents    = eventRepository.countByStatus(EventStatus.FAILED);
        long partialEvents   = eventRepository.countByStatus(EventStatus.PARTIAL);

        long dltUnprocessed = dltRepository.countByProcessedFalse();
        long dltTotal       = dltRepository.count();

        double successRate = totalLogs > 0 ? (totalSent * 100.0 / totalLogs) : 0;

        List<Object[]> channelFailures = logRepository.countFailuresByChannel();
        Map<String, Long> failuresByChannel = new LinkedHashMap<>();
        for (Object[] row : channelFailures) {
            failuresByChannel.put(row[0].toString(), (Long) row[1]);
        }

        return Map.of(
                "generatedAt", LocalDateTime.now(),
                "deliveries", Map.of(
                        "total",       totalLogs,
                        "sent",        totalSent,
                        "failed",      totalFailed,
                        "retrying",    totalRetrying,
                        "pending",     totalPending,
                        "successRate", String.format("%.1f", successRate)
                ),
                "events", Map.of(
                        "total",     totalEvents,
                        "completed", completedEvents,
                        "partial",   partialEvents,
                        "failed",    failedEvents
                ),
                "dlt", Map.of(
                        "total",       dltTotal,
                        "unprocessed", dltUnprocessed
                ),
                "channelFailures", failuresByChannel
        );
    }


    public List<Map<String, Object>> getDeliveryTrend(int days) {
        log.debug("Fetching delivery trend for last {} days", days);

        List<Object[]> rows = logRepository.getDailyDeliveryTrend(days);
        List<Map<String, Object>> result = new ArrayList<>();

        for (Object[] row : rows) {
            result.add(Map.of(
                    "date",    row[0].toString(),
                    "channel", row[1].toString(),
                    "status",  row[2].toString(),
                    "count",   ((Number) row[3]).longValue()
            ));
        }

        return result;
    }


    public Map<String, Object> getRetryAnalytics(int page, int size) {
        log.debug("Fetching retry analytics, page: {}, size: {}", page, size);

        List<Object[]> distribution = logRepository.getRetryCountDistribution();
        Map<String, Long> retryDist = new LinkedHashMap<>();
        for (Object[] row : distribution) {
            retryDist.put("attempt_" + row[0].toString(), (Long) row[1]);
        }

        Page<NotificationLog> retrying = logRepository.findByStatus(
                DeliveryStatus.RETRYING, PageRequest.of(page, size));

        long readyNow = logRepository.findReadyForRetry(PageRequest.of(0, 1)).getTotalElements();

        return Map.of(
                "totalRetrying",     retrying.getTotalElements(),
                "readyForRetryNow",  readyNow,
                "retryDistribution", retryDist,
                "currentPage",       page,
                "totalPages",        retrying.getTotalPages(),
                "entries",           retrying.getContent()
        );
    }


    public Map<String, Object> getDeliveryStatistics() {
        log.debug("Fetching delivery statistics");

        long pendingCount  = logRepository.countByStatus(DeliveryStatus.PENDING);
        long sentCount     = logRepository.countByStatus(DeliveryStatus.SENT);
        long failedCount   = logRepository.countByStatus(DeliveryStatus.FAILED);
        long retiringCount = logRepository.countByStatus(DeliveryStatus.RETRYING);
        long totalCount    = pendingCount + sentCount + failedCount + retiringCount;

        double successRate = totalCount > 0 ? (sentCount * 100.0 / totalCount) : 0;

        return Map.of(
                "timestamp",   LocalDateTime.now(),
                "total",       totalCount,
                "delivered",   sentCount,
                "pending",     pendingCount,
                "retrying",    retiringCount,
                "failed",      failedCount,
                "successRate", String.format("%.2f%%", successRate)
        );
    }


    public Map<String, Object> getFailedDeliveries(int page, int size) {
        log.debug("Fetching failed deliveries, page: {}, size: {}", page, size);

        Pageable pageable = PageRequest.of(page, size);
        Page<NotificationLog> failed = logRepository.findByStatusOrderByCreatedAtDesc(
                DeliveryStatus.FAILED, pageable);

        return Map.of(
                "totalFailed",      failed.getTotalElements(),
                "totalPages",       failed.getTotalPages(),
                "currentPage",      page,
                "pageSize",         size,
                "failedDeliveries", failed.getContent()
        );
    }


    public Map<String, Object> getByChannel(String channel, int page, int size) {
        log.debug("Fetching deliveries for channel: {}", channel);

        NotificationChannel notifChannel;
        try {
            notifChannel = NotificationChannel.valueOf(channel.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid channel requested: {}", channel);
            throw new InvalidInputException("Invalid channel: " + channel);
        }

        Pageable pageable = PageRequest.of(page, size);
        Page<NotificationLog> logs = logRepository.findFailedByChannel(notifChannel, pageable);

        return Map.of(
                "channel",    channel,
                "total",      logs.getTotalElements(),
                "totalPages", logs.getTotalPages(),
                "currentPage", page,
                "Logs",       logs.getContent()
        );
    }


    public Map<String, Object> getUserHistoryByDateRange(
            String userId, LocalDateTime from, LocalDateTime to, int page, int size) {

        log.debug("Fetching history for user: {} from: {} to: {}", userId, from, to);

        Pageable pageable = PageRequest.of(page, size);
        Page<NotificationLog> logs =
                (from != null && to != null)
                        ? logRepository.findByUserIdAndCreatedAtBetween(
                        userId,
                        from,
                        to,
                        pageable
                )
                        : logRepository.findByUserIdOrderByCreatedAtDesc(
                        userId,
                        pageable
                );

        return Map.of(
                "userId",             userId,
                "from",               from,
                "to",                 to,
                "totalNotifications", logs.getTotalElements(),
                "logs",               logs.getContent()
        );
    }


    public Map<String, Object> getIdempotencyStats(long totalProcessed) {
        long uniqueKeys = eventRepository.count();

        return Map.of(
                "totalProcessed",          totalProcessed,
                "uniqueIdempotencyKeys",   uniqueKeys,
                "duplicateDetectionStatus", "ACTIVE (using eventId-based keys)",
                "keyFormat",               "UUID (stable, deterministic)"
        );
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