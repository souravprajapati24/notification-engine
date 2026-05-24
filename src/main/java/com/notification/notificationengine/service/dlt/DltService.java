package com.notification.notificationengine.service.dlt;

import com.notification.notificationengine.dto.DltMessagePayloadDto;
import com.notification.notificationengine.model.DltMessage;
import com.notification.notificationengine.repository.DltMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    public void logDltMessage(
            ConsumerRecord<String, String> record,
            DltMessagePayloadDto payload,
            String failureCode,
            String errorReason
    ) {
        try {
            // Prevent duplicate DLT inserts
            boolean alreadyExists =
                    dltRepository.existsByTopicAndPartitionAndKafkaOffset(
                            record.topic(),
                            record.partition(),
                            record.offset()
                    );

            if (alreadyExists) {

                log.warn(
                        "⚠ DLT message already logged - Topic: {}, Partition: {}, Offset: {}",
                        record.topic(),
                        record.partition(),
                        record.offset()
                );

                return;
            }
            DltMessage dltMsg = DltMessage.builder()
                    .topic(record.topic())
                    .partition(record.partition())
                    .kafkaOffset(record.offset())
                    .messageKey(record.key())
                    .messagePayload(record.value())
                    .failureCode(failureCode)
                    .errorReason(errorReason)
                    .eventId(payload.getEventId())
                    .userId(payload.getUserId())
                    .channel(payload.getChannel())
                    .processed(false)
                    .build();

            dltRepository.save(dltMsg);

            log.error(
                    "✗ Message logged to DLT - Topic: {}, Partition: {}, Offset: {}, Id: {}, Code: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    dltMsg.getId(),
                    failureCode
            );

        } catch (Exception e) {
            log.error("✗ Failed to log DLT message: {}", e.getMessage(), e);

        }
    }

    @Transactional
    public void replayMessage(UUID dltId, String originalTopic) {
        try {
            var dltMsg = dltRepository.findById(dltId)
                    .orElseThrow(() -> new RuntimeException("DLT message not found: " + dltId));

            log.info(
                    "↻ Replaying DLT message - Id: {}, Original offset: {}, Topic: {}",
                    dltId,
                    dltMsg.getKafkaOffset(),
                    originalTopic
            );

            kafkaTemplate.send(
                    originalTopic,
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

    public long getUnprocessedCount() {
        return dltRepository.countByProcessedFalse();
    }
}