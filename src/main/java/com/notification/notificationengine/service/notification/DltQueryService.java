package com.notification.notificationengine.service.notification;

import com.notification.notificationengine.model.DltMessage;
import com.notification.notificationengine.repository.DltMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class DltQueryService {

    private final DltMessageRepository dltRepository;

    public Map<String, Object> getUnprocessedMessages(int page, int size) {
        log.debug("Fetching unprocessed DLT messages, page: {}, size: {}", page, size);

        Page<DltMessage> messages = dltRepository.findByProcessedFalseOrderByCreatedAtDesc(
                PageRequest.of(page, size));

        return Map.of(
                "totalUnprocessed", dltRepository.countByProcessedFalse(),
                "totalPages",       messages.getTotalPages(),
                "currentPage",      page,
                "messages",         messages.getContent()
        );
    }

    public Map<String, Object> getDltMessageDetails(UUID id) {
        log.debug("Fetching DLT message details for id: {}", id);

        DltMessage message = dltRepository.findById(id)
                .orElseThrow(() -> new com.notification.notificationengine.exception
                        .ResourceNotFoundException("DLT message not found for ID: " + id));

        return Map.of(
                "message",        message,
                "canReplay",      !message.getProcessed(),
                "messagePayload", message.getMessagePayload()
        );
    }

    public Map<String, Object> getDltStatistics() {
        log.debug("Fetching DLT statistics");

        long totalUnprocessed = dltRepository.countByProcessedFalse();
        long total            = dltRepository.count();
        var  failureStats     = dltRepository.countByFailureCode();

        return Map.of(
                "totalMessages",        total,
                "unprocessedMessages",  totalUnprocessed,
                "processedMessages",    total - totalUnprocessed,
                "unprocessedPercentage", total > 0 ? (totalUnprocessed * 100.0 / total) : 0,
                "failureCodeBreakdown", failureStats
        );
    }

    public Map<String, Object> getByFailureCode(String failureCode, int page, int size) {
        log.debug("Fetching DLT messages by failure code: {}", failureCode);

        Page<DltMessage> messages = dltRepository.findByFailureCodeOrderByCreatedAtDesc(
                failureCode, PageRequest.of(page, size));

        return Map.of(
                "failureCode", failureCode,
                "total",       messages.getTotalElements(),
                "totalPages",  messages.getTotalPages(),
                "messages",    messages.getContent()
        );
    }
}