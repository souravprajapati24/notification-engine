package com.notification.notificationengine.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.notification.notificationengine.model.enums.EventStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Table(
        name = "notification_events",
        indexes = {
                @Index(name = "idx_user_created", columnList = "user_id, created_at DESC", unique = false),
                @Index(name = "idx_status", columnList = "status", unique = false),
                @Index(name = "idx_created", columnList = "created_at DESC", unique = false)
        }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false, length = 100)
    private String userId;

    @Column(nullable = false, length = 50)
    private String eventType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "notification_event_channels",
            joinColumns = @JoinColumn(name = "event_id")
    )
    @Enumerated(EnumType.STRING)
    @Column(name = "channel")
    private List<NotificationChannel> channels;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private EventStatus status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode metadata;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    private LocalDateTime processedAt;


    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
        if (this.status == null) {
            this.status = EventStatus.PENDING;
        }
    }


    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}