package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.service.channel.SmsNotificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SmsNotificationServiceImpl implements SmsNotificationService {
    @Override
    public void sendSms(NotificationEventDto eventDto) {

        log.info(
                "Sending SMS notification to {} for event {}",
                eventDto.getPhoneNumber(),
                eventDto.getEventId()
        );
    }
}
