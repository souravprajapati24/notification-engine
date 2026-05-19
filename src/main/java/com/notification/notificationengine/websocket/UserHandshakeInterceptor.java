package com.notification.notificationengine.websocket;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

@Slf4j
public class UserHandshakeInterceptor implements HandshakeInterceptor {

    @Override
    public boolean beforeHandshake(
            @NonNull ServerHttpRequest request,
            @NonNull ServerHttpResponse response,
            @NonNull WebSocketHandler wsHandler,
            @NonNull Map<String, Object> attributes
    ) {
        MultiValueMap<String, String> params = UriComponentsBuilder
                .fromUri(request.getURI())
                .build()
                .getQueryParams();

        String userId = params.getFirst("userId");

        if (userId == null || userId.isBlank()) {
            log.warn("WebSocket connection rejected: missing userId");
            return false;
        }

        attributes.put("userId", userId);
        log.info("✓ WebSocket handshake initiated - userId: {}", userId);
        return true;
    }

    @Override
    public void afterHandshake(
            @NonNull ServerHttpRequest request,
            @NonNull ServerHttpResponse response,
            @NonNull WebSocketHandler wsHandler,
            Exception exception
    ) {
        if (exception != null) {
            log.error("✗ WebSocket handshake failed: {}", exception.getMessage(), exception);
        } else {
            log.info("✓ WebSocket handshake completed successfully");
        }
    }
}