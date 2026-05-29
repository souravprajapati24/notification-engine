package com.notification.notificationengine.controller;

import com.notification.notificationengine.service.dlt.DltService;
import com.notification.notificationengine.service.notification.DltQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/dlt")
@RequiredArgsConstructor
@Slf4j
public class DltController {

    private final DltQueryService dltQueryService;
    private final DltService dltService;

    @GetMapping("/messages")
    public ResponseEntity<Map<String, Object>> getUnprocessedMessages(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return ResponseEntity.ok(dltQueryService.getUnprocessedMessages(page, size));
    }

    @GetMapping("/messages/{id}")
    public ResponseEntity<Map<String, Object>> getDltMessageDetails(@PathVariable UUID id) {
        return ResponseEntity.ok(dltQueryService.getDltMessageDetails(id));
    }

    @PostMapping("/messages/{id}/replay")
    public ResponseEntity<Map<String, Object>> replayMessage(@PathVariable UUID id) {
        dltService.replayMessage(id);
        return ResponseEntity.ok(Map.of(
                "status",    "REPLAYED",
                "messageId", id,
                "message",   "Message queued for replay"
        ));
    }

    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getDltStatistics() {
        return ResponseEntity.ok(dltQueryService.getDltStatistics());
    }

    @GetMapping("/by-failure-code/{failureCode}")
    public ResponseEntity<Map<String, Object>> getByFailureCode(
            @PathVariable String failureCode,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return ResponseEntity.ok(dltQueryService.getByFailureCode(failureCode, page, size));
    }
}