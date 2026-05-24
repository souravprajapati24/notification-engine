package com.notification.notificationengine.service.retry;

import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.repository.NotificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RetryWorker {

    private final NotificationLogRepository logRepository;
    private final RetryExecutionService retryExecutionService;

    @Scheduled(fixedDelay = 30000, initialDelay = 5000)
    public void retryFailedMessages() {
        try {
            log.debug("▼ Retry worker started - scanning for messages ready to retry...");

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