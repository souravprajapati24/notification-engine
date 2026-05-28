package com.notification.notificationengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * PHASE 5: Improved async configuration
 *
 * Uses separate executors for different types of tasks:
 * - deliveryExecutor: For channel delivery (SMS, Email, WebSocket)
 * - retryExecutor: For retry worker
 * - defaultExecutor: Fallback for other @Async tasks
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    /**
     * Executor for delivery services (Email, SMS, WebSocket)
     * Core: 5, Max: 10, Queue: 100
     * Settings: Keep as is (good balance)
     */
    @Bean(name = "deliveryExecutor")
    public Executor deliveryExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("notification-delivery-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

    /**
     * Default executor for other async tasks
     * Smaller because less frequently used
     */
    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("notification-task-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }
}