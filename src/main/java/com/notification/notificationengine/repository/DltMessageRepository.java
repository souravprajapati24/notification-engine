package com.notification.notificationengine.repository;

import com.notification.notificationengine.model.DltMessage;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface DltMessageRepository extends JpaRepository<DltMessage, UUID> {

    Page<DltMessage> findByProcessedFalseOrderByCreatedAtDesc(Pageable pageable);

    Page<DltMessage> findByTopicOrderByCreatedAtDesc(String topic, Pageable pageable);

    long countByProcessedFalse();

    Page<DltMessage> findByFailureCodeOrderByCreatedAtDesc(String failureCode, Pageable pageable);

    @Query("""
        SELECT dm.topic, COUNT(dm) as count FROM DltMessage dm
        GROUP BY dm.topic
        ORDER BY count DESC
    """)
    List<Object[]> countByTopic();

    @Query("""
        SELECT dm.failureCode, COUNT(dm) as count FROM DltMessage dm
        WHERE dm.failureCode IS NOT NULL
        GROUP BY dm.failureCode
        ORDER BY count DESC
    """)
    List<Object[]> countByFailureCode();

    boolean existsByTopicAndPartitionAndKafkaOffset(String topic, int partition, long offset);
}