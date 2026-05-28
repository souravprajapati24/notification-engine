package com.notification.notificationengine.service.retry;

import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.repository.NotificationLogRepository;
import com.notification.notificationengine.service.throttle.RetryWorkerThrottle;
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
    private final RetryWorkerThrottle retryWorkerThrottle;

    @Scheduled(fixedDelay = 30000, initialDelay = 5000)
    public void retryFailedMessages() {
        try {
            log.debug("▼ Retry worker started - scanning for messages ready to retry...");

            int totalProcessed = 0;
            int pageNum = 0;
            int maxBatchSize = retryWorkerThrottle.getMaxBatchSize();

            while(true){
                Page<NotificationLog> readyForRetry = logRepository.findReadyForRetry(
                        PageRequest.of(pageNum, maxBatchSize)
                );

                if (readyForRetry.isEmpty()) {
                    log.debug("✓ No more messages ready for retry");
                    break;
                }

                log.info(
                        "⟳ Found {} messages ready for retry (page {} of {})",
                        readyForRetry.getNumberOfElements(),
                        pageNum + 1,
                        readyForRetry.getTotalPages()
                );

                int batchProcessed = 0;
                for (NotificationLog logg : readyForRetry.getContent()) {

                    try {
                        retryExecutionService.retryDelivery(logg);
                        batchProcessed++;
                        totalProcessed++;
                    }
                    catch (Exception e) {
                        log.error(
                                " Error retrying message - LogId: {}, Error: {}",
                                logg.getId(),
                                e.getMessage()
                        );
                    }
                }

                log.info("✓ Processed batch {}: {} messages", pageNum + 1, batchProcessed);

                retryWorkerThrottle.delayBetweenBatches(readyForRetry.getNumberOfElements());

                if (!readyForRetry.hasNext()) {
                    log.debug("✓ No more pages to process");
                    break;
                }
                pageNum++;
            }
            log.info("✓ Retry worker cycle complete - Total processed: {}", totalProcessed);

        } catch (Exception e) {
            log.error(" Retry worker error: {}", e.getMessage(), e);
        }
    }



}