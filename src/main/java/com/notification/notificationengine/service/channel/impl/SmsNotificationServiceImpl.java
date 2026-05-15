package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.config.TwilioConfig;
import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.service.channel.SmsNotificationService;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SmsNotificationServiceImpl implements SmsNotificationService {

    private final TwilioConfig twilioConfig;

    @Override
    @Async
    public void sendSms(NotificationEventDto eventDto) {

        log.info("Sending Sms for event {} to phone number {}",
                eventDto.getEventId(),
                eventDto.getPhoneNumber()
        );

        try {

            Message.creator(
                    new PhoneNumber(eventDto.getPhoneNumber()),
                    new PhoneNumber(twilioConfig.getFromNumber()),
                    eventDto.getMessage()
            ).create();

            log.info(
                    "SMS sent successfully to {} for event {}",
                    eventDto.getPhoneNumber(),
                    eventDto.getEventId()
            );

        } catch (Exception e) {

            log.error(
                    "Failed to send SMS to {} for event {}",
                    eventDto.getPhoneNumber(),
                    eventDto.getEventId(),
                    e
            );

            throw new RuntimeException("SMS delivery failed");
        }
    }
}