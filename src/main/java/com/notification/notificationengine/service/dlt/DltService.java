package com.notification.notificationengine.service.dlt;

import com.notification.notificationengine.dto.DltMessagePayloadDto;
import com.notification.notificationengine.model.DltMessage;
import com.notification.notificationengine.repository.DltMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DltService {

    private final DltMessageRepository dltRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;


    @Transactional
    public void logDltMessage(DltMessagePayloadDto payload) {

        try {

            boolean alreadyExists =
                    dltRepository.existsByEventIdAndChannel(
                            payload.getEventId(),
                            payload.getChannel()
                    );

            if (alreadyExists) {
                log.warn(
                        "⚠ DLT entry already exists - EventId: {}, Channel: {}",
                        payload.getEventId(),
                        payload.getChannel()
                );

                return;
            }

            DltMessage dltMsg = DltMessage.builder()
                    .messageKey(payload.getEventId().toString())
                    .messagePayload(payload.getMessage())
                    .failureCode(payload.getFailureCode())
                    .errorReason(payload.getFailureReason())
                    .eventId(payload.getEventId())
                    .userId(payload.getUserId())
                    .channel(payload.getChannel())
                    .processed(false)
                    .build();

            dltRepository.save(dltMsg);

            log.error(
                    "☠ DLT persistence successful - EventId: {}, Channel: {}, FailureCode: {}",
                    payload.getEventId(),
                    payload.getChannel(),
                    payload.getFailureCode()
            );

            alertOperationsTeam(
                    payload,
                    payload.getFailureCode(),
                    payload.getFailureReason()
            );
        } catch (Exception e) {

            log.error(
                    "⚠ Failed direct DLT persistence - EventId: {}, Error: {}",
                    payload.getEventId(),
                    e.getMessage(),
                    e
            );

            throw e;
        }
    }

    @Transactional
    public void replayMessage(UUID dltId) {
        try {
            var dltMsg = dltRepository.findById(dltId)
                    .orElseThrow(() -> new RuntimeException("DLT message not found: " + dltId));

            log.info(
                    "↻ Replaying DLT message - Id: {}",
                    dltId
            );

            kafkaTemplate.send(
                    dltMsg.getMessageKey(),
                    dltMsg.getMessagePayload()
            ).get();

            dltMsg.setProcessed(true);
            dltMsg.setReplayResult("REPLAYED_BY_OPERATOR_" + System.currentTimeMillis());
            dltRepository.save(dltMsg);

            log.info("✓ DLT message replayed successfully - Id: {}", dltId);

        } catch (Exception e) {
            log.error("✗ Failed to replay DLT message - Id: {}, Error: {}", dltId, e.getMessage(), e);
            throw new RuntimeException("Failed to replay DLT message: " + e.getMessage(), e);
        }
    }

    private void alertOperationsTeam(
            DltMessagePayloadDto payload,
            String failureCode,
            String errorReason
    ) {

        log.warn(
                "⚠ DLT Alert - EventId: {}, UserId: {}, Channel: {}, FailureCode: {}, Reason: {}",
                payload.getEventId(),
                payload.getUserId(),
                payload.getChannel(),
                failureCode,
                errorReason
        );
    }

    public long getUnprocessedCount() {
        return dltRepository.countByProcessedFalse();
    }
}