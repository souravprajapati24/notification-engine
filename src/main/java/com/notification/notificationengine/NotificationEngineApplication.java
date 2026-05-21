package com.notification.notificationengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NotificationEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationEngineApplication.class, args);
    }

}
