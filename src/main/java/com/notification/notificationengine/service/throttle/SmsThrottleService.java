package com.notification.notificationengine.service.throttle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.Semaphore;

/**
 * PHASE 5: Simple SMS throttling using Semaphore
 *
 * Limits concurrent Twilio API calls to prevent rate limiting.
 * Twilio: ~100-200 SMS/sec depending on account
 * We limit to 20 concurrent calls for safety
 */
@Service
@Slf4j
public class SmsThrottleService {

    private final Semaphore smsSemaphore;

    public SmsThrottleService(
            @Value("${app.throttle.sms-max-concurrent}") int maxConcurrentSms
    ) {
        this.smsSemaphore = new Semaphore(maxConcurrentSms);
        log.info("✓ SMS throttle initialized - Max concurrent: {}", maxConcurrentSms);
    }

    /**
     * Acquire permit before calling Twilio API
     * Blocks if all permits are in use
     */
    public void acquire() throws InterruptedException {
        smsSemaphore.acquire();
    }

    /**
     * Release permit after API call completes (always do this in finally!)
     */
    public void release() {
        smsSemaphore.release();
    }

    /**
     * Try to acquire without blocking
     * Returns true if successful, false if no permits available
     */
    public boolean tryAcquire() {
        return smsSemaphore.tryAcquire();
    }

    /**
     * Get available permits (for monitoring)
     */
    public int getAvailablePermits() {
        return smsSemaphore.availablePermits();
    }
}