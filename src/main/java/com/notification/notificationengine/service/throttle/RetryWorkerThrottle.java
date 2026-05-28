package com.notification.notificationengine.service.throttle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * PHASE 5: Retry worker throttling
 *
 * Prevents retry worker from creating traffic spikes.
 * Instead of retrying 100 messages at once every 30 seconds,
 * we spread them out: process 20 at a time with small delays.
 */
@Service
@Slf4j
public class RetryWorkerThrottle {


    private final int maxRetryBatchSize;
    private final long delayBetweenBatchesMs;

    public RetryWorkerThrottle(
            @Value("${app.throttle.retry.max-batch-size}") int maxRetryBatchSize,
            @Value("${app.throttle.retry.batch-delay-ms}") long delayBetweenBatchesMs
    ) {
        this.maxRetryBatchSize = maxRetryBatchSize;
        this.delayBetweenBatchesMs = delayBetweenBatchesMs;

        log.info("✓ Retry worker throttle initialized - Batch: {}, Delay: {}ms",
                maxRetryBatchSize, delayBetweenBatchesMs);
    }

    public int getMaxBatchSize() {
        return maxRetryBatchSize;
    }

    /**
     * Sleep between batches (unless last batch was empty)
     */
    public void delayBetweenBatches(int itemsProcessed) {
        if (itemsProcessed > 0) {
            try {
                Thread.sleep(delayBetweenBatchesMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}