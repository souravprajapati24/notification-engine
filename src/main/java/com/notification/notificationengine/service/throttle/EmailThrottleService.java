package com.notification.notificationengine.service.throttle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.Semaphore;

/**
 * PHASE 5: Email throttling using Semaphore
 *
 * Limits concurrent SMTP connections to prevent connection pool exhaustion.
 * JavaMailSender pool: typically 5-10 connections
 * We limit to 50 concurrent attempts (connection reuse)
 */
@Service
@Slf4j
public class EmailThrottleService {

    private final Semaphore emailSemaphore;

    public EmailThrottleService(
            @Value("${app.throttle.email-max-concurrent}") int maxConcurrentEmail
    ) {
        this.emailSemaphore = new Semaphore(maxConcurrentEmail);
        log.info("✓ Email throttle initialized - Max concurrent: {}", maxConcurrentEmail);
    }

    public void acquire() throws InterruptedException {
        emailSemaphore.acquire();
    }

    public void release() {
        emailSemaphore.release();
    }

    public boolean tryAcquire() {
        return emailSemaphore.tryAcquire();
    }

    public int getAvailablePermits() {
        return emailSemaphore.availablePermits();
    }
}