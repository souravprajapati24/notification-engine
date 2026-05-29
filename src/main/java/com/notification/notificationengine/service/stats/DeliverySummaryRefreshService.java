package com.notification.notificationengine.service.stats;

import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeliverySummaryRefreshService {

    private final EntityManager entityManager;

    @Scheduled(fixedDelay = 300_000, initialDelay = 60_000)
    @Transactional
    public void refreshDeliverySummary() {
        try {
            entityManager.createNativeQuery(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY notification_delivery_summary"
            ).executeUpdate();
            log.debug("✓ Refreshed notification_delivery_summary");
        } catch (Exception e) {
            log.warn("Failed to refresh materialized view: {}", e.getMessage());
        }
    }
}
