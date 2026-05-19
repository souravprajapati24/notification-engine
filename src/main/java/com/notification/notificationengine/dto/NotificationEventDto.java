package com.notification.notificationengine.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.notification.notificationengine.model.enums.NotificationChannel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationEventDto {

    @NotBlank(message = "userId cannot be blank")
    private String userId;

    @NotBlank(message = "eventType cannot be blank")
    private String eventType;

    @NotBlank(message = "message cannot be blank")
    private String message;

    @NotEmpty(message = "At least one channel must be specified")
    private List<NotificationChannel> channels;

    private JsonNode metadata;
}