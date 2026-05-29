package com.notification.notificationengine.controller;

import com.notification.notificationengine.service.notification.SystemHealthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/admin/health")
@RequiredArgsConstructor
@Slf4j
public class SystemHealthController {

    private final SystemHealthService systemHealthService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> getSystemHealth() {

        return ResponseEntity.ok(
                systemHealthService.getSystemHealth()
        );
    }
}