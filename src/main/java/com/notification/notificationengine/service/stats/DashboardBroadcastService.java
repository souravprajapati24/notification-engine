package com.notification.notificationengine.service.stats;

import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.repository.DltMessageRepository;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DashboardBroadcastService {

    private final SimpMessagingTemplate messagingTemplate;
    private final NotificationLogRepository logRepository;
    private final DltMessageRepository dltRepository;


    @Scheduled(fixedDelay = 15_000, initialDelay = 5_000)
    public void broadcastStats() {
        try {
            Map<String, Object> snapshot = Map.of(
                    "ts",       System.currentTimeMillis(),
                    "sent",     logRepository.countByStatus(DeliveryStatus.SENT),
                    "failed",   logRepository.countByStatus(DeliveryStatus.FAILED),
                    "retrying", logRepository.countByStatus(DeliveryStatus.RETRYING),
                    "pending",  logRepository.countByStatus(DeliveryStatus.PENDING),
                    "dltUnprocessed", dltRepository.countByProcessedFalse()
            );
            messagingTemplate.convertAndSend("/topic/dashboard/stats", snapshot);
        } catch (Exception e) {
            log.debug("Broadcast failed (dashboard may not be connected): {}", e.getMessage());
        }
    }
}
