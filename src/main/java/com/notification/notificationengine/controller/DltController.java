package com.notification.notificationengine.controller;

import com.notification.notificationengine.model.DltMessage;
import com.notification.notificationengine.repository.DltMessageRepository;
import com.notification.notificationengine.service.dlt.DltService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/dlt")
@RequiredArgsConstructor
@Slf4j
public class DltController {

    private final DltMessageRepository dltRepository;
    private final DltService dltService;

    @GetMapping("/messages")
    public ResponseEntity<Map<String, Object>> getUnprocessedMessages(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        try {
            Page<DltMessage> messages = dltRepository.findByProcessedFalseOrderByCreatedAtDesc(
                    PageRequest.of(page, size)
            );

            return ResponseEntity.ok(Map.of(
                    "totalUnprocessed", dltRepository.countByProcessedFalse(),
                    "totalPages", messages.getTotalPages(),
                    "currentPage", page,
                    "messages", messages.getContent()
            ));

        } catch (Exception e) {
            log.error("Error fetching DLT messages: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch DLT messages"));
        }
    }

    @GetMapping("/messages/{id}")
    public ResponseEntity<Map<String, Object>> getDltMessageDetails(
            @PathVariable UUID id
    ) {
        try {
            var message = dltRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("DLT message not found"));

            return ResponseEntity.ok(Map.of(
                    "message", message,
                    "canReplay", !message.getProcessed(),
                    "messagePayload", message.getMessagePayload()
            ));

        } catch (Exception e) {
            log.error("Error fetching DLT message {}: {}", id, e.getMessage(), e);
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/messages/{id}/replay")
    public ResponseEntity<Map<String, Object>> replayMessage(
            @PathVariable UUID id
    ) {
        try {
            var message = dltRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("DLT message not found"));

            if (message.getProcessed()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "Message already processed/replayed"
                ));
            }

            dltService.replayMessage(id);

            return ResponseEntity.ok(Map.of(
                    "status", "REPLAYED",
                    "messageId", id,
                    "message", "Message queued for replay"
            ));

        } catch (Exception e) {
            log.error("Error replaying DLT message {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to replay message: " + e.getMessage()));
        }
    }

    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getDltStatistics() {
        try {
            long totalUnprocessed = dltRepository.countByProcessedFalse();
            long total = dltRepository.count();

            var failureStats = dltRepository.countByFailureCode();

            return ResponseEntity.ok(Map.of(
                    "totalMessages", total,
                    "unprocessedMessages", totalUnprocessed,
                    "processedMessages", total - totalUnprocessed,
                    "unprocessedPercentage", total > 0 ? (totalUnprocessed * 100.0 / total) : 0,
                    "failureCodeBreakdown", failureStats
            ));

        } catch (Exception e) {
            log.error("Error fetching DLT statistics: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch statistics"));
        }
    }

    @GetMapping("/by-failure-code/{failureCode}")
    public ResponseEntity<Map<String, Object>> getByFailureCode(
            @PathVariable String failureCode,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        try {
            Page<DltMessage> messages = dltRepository.findByFailureCodeOrderByCreatedAtDesc(
                    failureCode,
                    PageRequest.of(page, size)
            );

            return ResponseEntity.ok(Map.of(
                    "failureCode", failureCode,
                    "total", messages.getTotalElements(),
                    "totalPages", messages.getTotalPages(),
                    "messages", messages.getContent()
            ));

        } catch (Exception e) {
            log.error("Error fetching DLT by failure code: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch messages"));
        }
    }
}