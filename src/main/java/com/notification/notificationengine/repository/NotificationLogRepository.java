package com.notification.notificationengine.repository;

import com.notification.notificationengine.model.NotificationLog;
import com.notification.notificationengine.model.enums.DeliveryStatus;
import com.notification.notificationengine.model.enums.NotificationChannel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface NotificationLogRepository extends JpaRepository<NotificationLog, UUID> {

    Optional<NotificationLog> findByEventIdAndChannelAndStatus(
            UUID eventId,
            NotificationChannel channel,
            DeliveryStatus status
    );

    Page<NotificationLog> findByUserIdOrderByCreatedAtDesc(
            String userId,
            Pageable pageable
    );


    List<NotificationLog> findByEventIdOrderByCreatedAt(UUID eventId);

    Page<NotificationLog> findByStatusOrderByCreatedAtDesc(
            DeliveryStatus status,
            Pageable pageable
    );

    @Query("""
        SELECT nl FROM NotificationLog nl
        WHERE nl.channel = :channel AND nl.status = 'FAILED'
        ORDER BY nl.createdAt DESC
    """)
    Page<NotificationLog> findFailedByChannel(
            @Param("channel") NotificationChannel channel,
            Pageable pageable
    );

    long countByStatus(DeliveryStatus status);

    @Query("""
        SELECT nl FROM NotificationLog nl
        WHERE nl.createdAt BETWEEN :startTime AND :endTime
        ORDER BY nl.createdAt DESC
    """)
    Page<NotificationLog> findByTimeRange(
            @Param("startTime") LocalDateTime startTime,
            @Param("endTime") LocalDateTime endTime,
            Pageable pageable
    );

    @Query("""
        SELECT nl.channel, COUNT(nl) FROM NotificationLog nl
        WHERE nl.status = 'FAILED'
        GROUP BY nl.channel
    """)
    List<Object[]> countFailuresByChannel();

    @Query("""
    SELECT nl FROM NotificationLog nl
    WHERE nl.status = 'RETRYING'
    AND (nl.nextRetryAt IS NULL OR nl.nextRetryAt <= CURRENT_TIMESTAMP)
    ORDER BY nl.nextRetryAt ASC NULLS FIRST
""")
    Page<NotificationLog> findReadyForRetry(Pageable pageable);

    @Query("""
    SELECT nl.retryCount, COUNT(nl) as count FROM NotificationLog nl
    WHERE nl.status IN ('RETRYING', 'FAILED')
    GROUP BY nl.retryCount
    ORDER BY nl.retryCount ASC
""")
    List<Object[]> getRetryCountDistribution();

    @Modifying
    @Query("""
    UPDATE NotificationLog nl
    SET nl.status = 'PENDING', nl.nextRetryAt = NULL
    WHERE nl.id = :logId
""")
    void resetLogToPending(@Param("logId") UUID logId);

    @Query(
            value = """
        SELECT delivery_date, channel, status, count
        FROM notification_delivery_summary
        WHERE delivery_date >= CURRENT_DATE - CAST(:days AS INTEGER)
        ORDER BY delivery_date ASC, channel ASC
    """,
            nativeQuery = true
    )
    List<Object[]> getDailyDeliveryTrend(@Param("days") int days);


    boolean existsByEventId(UUID eventId);

    Page<NotificationLog> findByStatus(DeliveryStatus status, Pageable pageable);

    Page<NotificationLog> findByUserIdAndCreatedAtBetween(String userId, LocalDateTime from, LocalDateTime to, Pageable pageable);
}