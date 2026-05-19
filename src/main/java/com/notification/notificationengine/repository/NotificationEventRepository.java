package com.notification.notificationengine.repository;


import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.enums.EventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface NotificationEventRepository extends JpaRepository<NotificationEvent, UUID> {

    Page<NotificationEvent> findByUserIdOrderByCreatedAtDesc(
            String userId,
            Pageable pageable
    );


    Page<NotificationEvent> findByStatusOrderByCreatedAtDesc(
            EventStatus status,
            Pageable pageable
    );


    long countByStatus(EventStatus status);

    @Query("""
        SELECT ne FROM NotificationEvent ne 
        WHERE ne.createdAt BETWEEN :startTime AND :endTime
        ORDER BY ne.createdAt DESC
    """)
    Page<NotificationEvent> findByTimeRange(
            @Param("startTime") LocalDateTime startTime,
            @Param("endTime") LocalDateTime endTime,
            Pageable pageable
    );

    Optional<NotificationEvent> findById(UUID id);
}