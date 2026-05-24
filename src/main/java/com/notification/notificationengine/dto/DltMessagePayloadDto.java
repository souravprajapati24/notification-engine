package com.notification.notificationengine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DltMessagePayloadDto {

    @JsonProperty("event_id")
    private UUID eventId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("channel")
    private String channel;

    @JsonProperty("failure_code")
    private String failureCode;

    @JsonProperty("failure_reason")
    private String failureReason;

    @JsonProperty("retry_count")
    private Integer retryCount;

    @JsonProperty("failed_at")
    private LocalDateTime failedAt;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("message")
    private String message;
}