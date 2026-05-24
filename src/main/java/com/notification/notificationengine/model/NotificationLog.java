package com.notification.notificationengine.model;

import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;
@Entity
@Table(
        name = "notification_logs",
        indexes = {
                @Index(name = "idx_event_id", columnList = "event_id", unique = false),
                @Index(name = "idx_user_channel_status", columnList = "user_id, channel, status", unique = false),
                @Index(name = "idx_status_created", columnList = "status, created_at DESC", unique = false),
                @Index(name = "idx_failed_notifications", columnList = "created_at DESC", unique = false),
                @Index(name = "idx_pending_notifications", columnList = "created_at", unique = false),
                @Index(name = "idx_user_id", columnList = "user_id", unique = false)
        },
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_event_channel",
                        columnNames = {"event_id", "channel"}
                )
        }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false, length = 100)
    private String userId;


    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private NotificationChannel channel;

    @Column(length = 255)
    private String recipient;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private DeliveryStatus status;

    @Column(nullable = false)
    private Integer retryCount;

    private LocalDateTime lastRetryAt;

    private LocalDateTime nextRetryAt;

    @Column(columnDefinition = "TEXT")
    private String failureReason;

    @Column(length = 50)
    private String failureCode;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    private LocalDateTime sentAt;


    @Column(nullable = false)
    private LocalDateTime updatedAt;


    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
        if (this.status == null) {
            this.status = DeliveryStatus.PENDING;
        }
        if (this.retryCount == null) {
            this.retryCount = 0;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    public Long getDeliveryLatencySeconds() {
        if (this.sentAt != null && this.createdAt != null) {
            return java.time.temporal.ChronoUnit.SECONDS.between(this.createdAt, this.sentAt);
        }
        return null;
    }


    public boolean isTerminal() {
        return status == DeliveryStatus.SENT || status == DeliveryStatus.FAILED;
    }

    public boolean isReadyForRetry() {
        if (this.status != DeliveryStatus.RETRYING) {
            return false;
        }
        if (this.nextRetryAt == null) {
            return true;
        }
        return java.time.LocalDateTime.now().isAfter(this.nextRetryAt);
    }

    public Long getNextBackoffSeconds() {
        return switch (this.retryCount) {
            case 0 -> 5L;
            case 1 -> 30L;
            case 2 -> 120L;
            default -> null;
        };
    }
}