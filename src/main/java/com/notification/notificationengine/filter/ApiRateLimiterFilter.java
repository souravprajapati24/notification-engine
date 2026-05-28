package com.notification.notificationengine.filter;

import com.notification.notificationengine.service.api.ApiRateLimiter;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * PHASE 5: Rate limit filter for API endpoints
 *
 * Intercepts all POST /api/notifications/publish requests
 * and checks rate limits before processing.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ApiRateLimiterFilter implements Filter {

    private final ApiRateLimiter rateLimiter;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (!(request instanceof HttpServletRequest)) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        // Only rate limit POST /api/notifications/publish
        if (httpRequest.getMethod().equals("POST") &&
                httpRequest.getRequestURI().contains("/api/notifications/publish")) {

            // Extract user ID from request
            // Option 1: From header (if you have authentication)
            String userId = httpRequest.getHeader("X-User-Id");

            // Option 2: From request parameter (fallback)
            if (userId == null || userId.isEmpty()) {
                userId = request.getParameter("userId");
            }

            // Option 3: From client IP (if no user ID)
            if (userId == null || userId.isEmpty()) {
                userId = httpRequest.getRemoteAddr();
            }

            // Check rate limit
            if (!rateLimiter.isAllowed(userId)) {
                log.warn("⚠ Rate limit exceeded for user: {}", userId);

                httpResponse.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
                httpResponse.setContentType("application/json");
                httpResponse.setHeader("Retry-After", "60");

                httpResponse.getWriter().write(
                        "{\"status\":\"RATE_LIMIT_EXCEEDED\",\"message\":\"Too many requests. Max 1000 per minute.\",\"retryAfterSeconds\":60}"
                );
                return;
            }

            log.debug("✓ Rate limit check passed for user: {} - Remaining: {}",
                    userId, rateLimiter.getRemainingRequests(userId));
        }

        chain.doFilter(request, response);
    }
}