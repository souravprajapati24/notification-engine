package com.notification.notificationengine.service.retry;

import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


/**
 * PHASE 2: Retry Worker Service
 *
 * Periodically scans for RETRYING messages that are ready to retry
 * (where next_retry_at <= current_time)
 *
 * Runs every 10 seconds to find and reattempt failed deliveries
 * Uses exponential backoff: 5s → 30s → 120s
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RetryWorker {

    private final NotificationLogRepository logRepository;
    private final RetryExecutionService retryExecutionService;
    /**
     * Run every 10 seconds to retry failed messages
     * Batch size: 100 messages at a time to avoid overload
     */
    @Scheduled(fixedDelay = 10000, initialDelay = 10000)
    public void retryFailedMessages() {
        try {
            log.debug("▼ Retry worker started - scanning for messages ready to retry...");

            // Find all RETRYING messages where next_retry_at <= now
            Page<NotificationLog> readyForRetry = logRepository.findReadyForRetry(
                    PageRequest.of(0, 100)
            );

            if (readyForRetry.isEmpty()) {
                log.debug("✓ No messages ready for retry at this time");
                return;
            }

            log.info(
                    "⟳ Found {} messages ready for retry (page 1 of {})",
                    readyForRetry.getNumberOfElements(),
                    readyForRetry.getTotalPages()
            );

            // Process each message ready for retry
            for (NotificationLog logg : readyForRetry.getContent()) {

                try {
                    retryExecutionService.retryDelivery(logg);
                }
                catch (Exception e) {
                    log.error(
                            " Error retrying message - LogId: {}, Error: {}",
                            logg.getId(),
                            e.getMessage(),
                            e
                    );
                }
            }

            // If there are more pages, schedule them for next cycle
            if (readyForRetry.hasNext()) {
                log.info(
                        "⟳ {} more messages queued for retry in next cycle",
                        readyForRetry.getTotalElements() - readyForRetry.getNumberOfElements()
                );
            }

        } catch (Exception e) {
            log.error(" Retry worker error: {}", e.getMessage(), e);
        }
    }



}