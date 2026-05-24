package com.notification.notificationengine.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "dlt_messages")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DltMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private Integer partition;

    @Column(nullable = false)
    private Long kafkaOffset;

    @Column(columnDefinition = "TEXT")
    private String messageKey;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String messagePayload;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false, length = 100)
    private String userId;

    @Column(length = 50)
    private String channel;

    @Column(columnDefinition = "TEXT")
    private String errorReason;

    @Column(length = 50)
    private String failureCode;

    @Column(nullable = false)
    private Boolean processed;

    @Column(columnDefinition = "TEXT")
    private String replayResult;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
        if (this.processed == null) {
            this.processed = false;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}