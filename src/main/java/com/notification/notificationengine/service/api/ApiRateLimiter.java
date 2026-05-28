package com.notification.notificationengine.service.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * PHASE 5: Simple in-memory API rate limiter
 *
 * Prevents single user from spamming API endpoint.
 * Uses sliding window counter with in-memory map.
 *
 * NOT for distributed systems: if you have multiple API servers,
 * use Redis instead (but for single-server, this is fine).
 *
 * Example: Max 1000 requests per minute per user
 */
@Service
@Slf4j
public class ApiRateLimiter {

    private final int requestsPerMinute;
    private final long timeWindowMs;

    // Key: userId, Value: list of request timestamps
    private final ConcurrentMap<String, RequestWindow> windows = new ConcurrentHashMap<>();

    public ApiRateLimiter(
            @Value("${app.throttle.api-rate-limit.requests-per-minute}") int requestsPerMinute
    ) {
        this.requestsPerMinute = requestsPerMinute;
        this.timeWindowMs = TimeUnit.MINUTES.toMillis(1);

        log.info("✓ API rate limiter initialized - {} requests/min per user",
                requestsPerMinute);
    }

    /**
     * Check if user is allowed to make request
     * Returns true if allowed, false if rate limited
     */
    public boolean isAllowed(String userId) {
        long now = System.currentTimeMillis();

        RequestWindow window = windows.computeIfAbsent(
                userId,
                k -> new RequestWindow(now)
        );

        synchronized (window) {
            // Check if window has expired
            if (now - window.windowStart > timeWindowMs) {
                // Start new window
                window.windowStart = now;
                window.count = 1;
                return true;
            }

            // Window still active
            if (window.count < requestsPerMinute) {
                window.count++;
                return true;
            }

            // Rate limit exceeded
            return false;
        }
    }

    /**
     * Get remaining requests for user (for dashboard)
     */
    public int getRemainingRequests(String userId) {
        RequestWindow window = windows.get(userId);
        if (window == null) {
            return requestsPerMinute;
        }

        long now = System.currentTimeMillis();
        synchronized (window) {
            if (now - window.windowStart > timeWindowMs) {
                return requestsPerMinute;
            }
            return Math.max(0, requestsPerMinute - window.count);
        }
    }

    /**
     * Cleanup old entries (optional maintenance task)
     */
    public void cleanup() {
        long now = System.currentTimeMillis();
        windows.entrySet().removeIf(entry ->
                now - entry.getValue().windowStart > timeWindowMs * 2
        );
    }

    /**
     * Inner class to track requests in current window
     */
    private static class RequestWindow {
        long windowStart;
        int count;

        RequestWindow(long windowStart) {
            this.windowStart = windowStart;
            this.count = 0;
        }
    }
}