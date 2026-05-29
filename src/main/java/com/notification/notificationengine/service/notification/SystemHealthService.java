package com.notification.notificationengine.service.notification;

import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.repository.DltMessageRepository;
import com.notification.notificationengine.repository.NotificationLogRepository;
import com.notification.notificationengine.service.throttle.RetryWorkerThrottle;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class SystemHealthService {

    private final NotificationLogRepository logRepository;
    private final DltMessageRepository dltRepository;
    private final RetryWorkerThrottle retryWorkerThrottle;

    public Map<String, Object> getSystemHealth() {

        long pendingLogs =
                logRepository.countByStatus(DeliveryStatus.PENDING);

        long dltUnprocessed =
                dltRepository.countByProcessedFalse();

        String retryWorkerStatus = "RUNNING";

        String overallStatus;

        if (dltUnprocessed > 50) {
            overallStatus = "DEGRADED";
        } else if (dltUnprocessed > 0) {
            overallStatus = "WARNING";
        } else {
            overallStatus = "HEALTHY";
        }

        log.debug(
                "System health checked. Status: {}, DLT: {}",
                overallStatus,
                dltUnprocessed
        );

        return Map.of(
                "status", overallStatus,
                "checkedAt", LocalDateTime.now(),
                "database", "UP",
                "retryWorker", retryWorkerStatus,
                "pendingDeliveries", pendingLogs,
                "dltUnprocessed", dltUnprocessed,
                "actuatorHealth", "/actuator/health"
        );
    }
}
