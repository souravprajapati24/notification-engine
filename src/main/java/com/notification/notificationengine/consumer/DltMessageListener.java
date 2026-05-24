package com.notification.notificationengine.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.notificationengine.dto.DltMessagePayloadDto;
import com.notification.notificationengine.service.dlt.DltService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;


@Service
@RequiredArgsConstructor
@Slf4j
public class DltMessageListener {

    private final DltService dltService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.notification-events}.DLT",
            groupId = "dlt-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleDltMessage(ConsumerRecord<String, String> record) {
        try {
            log.error(
                    "✗ DLT Message received - Topic: {}, Partition: {}, Offset: {}, Key: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key()
            );

            DltMessagePayloadDto payload = objectMapper.readValue(
                    record.value(),
                    DltMessagePayloadDto.class
            );

            log.error(
                    "DLT Payload deserialized - EventId: {}, Channel: {}, Code: {}, UserId: {}",
                    payload.getEventId(),
                    payload.getChannel(),
                    payload.getFailureCode(),
                    payload.getUserId()
            );

            String errorReason = extractErrorReason(record);
            String failureCode = payload.getFailureCode();

            String headerFailureCode = extractFailureCodeFromHeaders(record);
            if (!"UNKNOWN_ERROR".equals(headerFailureCode)) {
                failureCode = headerFailureCode;
            }

            dltService.logDltMessage(
                    record,
                    payload,
                    failureCode,
                    errorReason
            );

            alertOperationsTeam(payload, failureCode, errorReason);
            log.info(
                    "✓ DLT message processed - EventId: {}, LogId will be generated",
                    payload.getEventId()
            );

        } catch (Exception e) {
            log.error(
                    "✗ Error handling DLT message - Offset: {}, Topic: {}, Error: {}",
                    record.offset(),
                    record.topic(),
                    e.getMessage(),
                    e
            );
        }
    }

    private String extractErrorReason(ConsumerRecord<String, String> record) {
        try {
            Header exceptionMessageHeader = record.headers().lastHeader(
                    KafkaHeaders.DLT_EXCEPTION_MESSAGE
            );

            if (exceptionMessageHeader != null) {
                return new String(
                        exceptionMessageHeader.value(),
                        StandardCharsets.UTF_8
                );
            }
        } catch (Exception e) {
            log.debug("Failed to extract error reason from headers: {}", e.getMessage());
        }

        return "Message moved to DLT after max retries exhausted";
    }

    private String extractFailureCodeFromHeaders(ConsumerRecord<String, String> record) {
        try {
            Header exceptionTypeHeader = record.headers().lastHeader(
                    KafkaHeaders.DLT_EXCEPTION_FQCN
            );

            if (exceptionTypeHeader != null) {
                String exceptionClassName = new String(
                        exceptionTypeHeader.value(),
                        StandardCharsets.UTF_8
                );
                return mapExceptionToFailureCode(exceptionClassName);
            }
        } catch (Exception e) {
            log.debug("Failed to extract failure code from headers: {}", e.getMessage());
        }

        return "UNKNOWN_ERROR";
    }

    private String mapExceptionToFailureCode(String exceptionClassName) {
        if (exceptionClassName == null || exceptionClassName.isEmpty()) {
            return "UNKNOWN_ERROR";
        }

        if (exceptionClassName.contains("SocketTimeout")) {
            return "EMAIL_TIMEOUT";
        }
        if (exceptionClassName.contains("ConnectException")) {
            return "CONNECTION_ERROR";
        }
        if (exceptionClassName.contains("TimeoutException")) {
            return "OPERATION_TIMEOUT";
        }

        if (exceptionClassName.contains("JsonParse")) {
            return "INVALID_JSON";
        }
        if (exceptionClassName.contains("DataIntegrityViolation")) {
            return "DATABASE_CONSTRAINT_ERROR";
        }

        if (exceptionClassName.contains("Mail")) {
            return "EMAIL_SERVICE_ERROR";
        }
        if (exceptionClassName.contains("Twilio")) {
            return "SMS_SERVICE_ERROR";
        }
        if (exceptionClassName.contains("WebSocket")) {
            return "WEBSOCKET_ERROR";
        }
        if (exceptionClassName.contains("Authentication")) {
            return "AUTH_FAILED";
        }
        if (exceptionClassName.contains("Authorization")) {
            return "AUTH_FAILED";
        }

        return "UNKNOWN_ERROR";
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


}