package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.config.TwilioConfig;
import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.service.channel.SmsNotificationService;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceServiceImpl;
import com.twilio.exception.ApiException;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
@Service
@RequiredArgsConstructor
@Slf4j
public class SmsNotificationServiceImpl implements SmsNotificationService {

    private final NotificationPersistenceServiceImpl persistenceService;
    private final TwilioConfig twilioConfig;

    @Async
    @Override
    public void deliver(NotificationEvent event) {
        String recipientPhone = null;

        try {

            recipientPhone = extractPhoneFromEvent(event);

            if (recipientPhone == null || recipientPhone.isEmpty()) {
                throw new IllegalArgumentException("No phone number found for user: " + event.getUserId());
            }

            log.debug("Preparing SMS delivery - Event: {}, Recipient: {}",
                    event.getId(), maskPhone(recipientPhone));

            if (twilioConfig.getAccountSid() == null || twilioConfig.getAuthToken() == null || twilioConfig.getFromNumber() == null) {
                throw new IllegalStateException("Twilio credentials not configured");
            }

            Message message = Message.creator(
                    new PhoneNumber(recipientPhone),
                    new PhoneNumber(twilioConfig.getFromNumber()),
                    event.getMessage()
            ).create();

            log.info("✓ SMS sent successfully - Event: {}, Recipient: {}, MessageSid: {}",
                    event.getId(),
                    maskPhone(recipientPhone),
                    message.getSid()
            );
            persistenceService.markChannelDelivered(
                    event.getId(),
                    NotificationChannel.SMS,
                    recipientPhone
            );

        } catch (ApiException e) {
            log.error("✗ Twilio API error - Event: {}, Recipient: {}, Code: {}, Message: {}",
                    event.getId(),
                    maskPhone(recipientPhone),
                    e.getCode().toString(),
                    e.getMessage(),
                    e
            );

            String errorCode = categorizeTwilioError(e.getCode().toString());
            persistenceService.markChannelFailed(
                    event.getId(),
                    NotificationChannel.SMS,
                    e.getMessage(),
                    errorCode
            );

        } catch (Exception e) {
            log.error("✗ SMS delivery failed - Event: {}, Recipient: {}, Error: {}",
                    event.getId(),
                    maskPhone(recipientPhone),
                    e.getMessage(),
                    e
            );

            String errorCode = categorizeError(e);
            persistenceService.markChannelFailed(
                    event.getId(),
                    NotificationChannel.SMS,
                    e.getMessage(),
                    errorCode
            );
        }
    }

    private String extractPhoneFromEvent(NotificationEvent event) {


        if (event.getMetadata() != null && event.getMetadata().has("phone")) {
            return event.getMetadata().get("phone").asText();
        }

        return null;
    }

    private String categorizeTwilioError(String twilioErrorMessage) {
        if (twilioErrorMessage == null) {
            return "SMS_TWILIO_ERROR";
        }

        String msg = twilioErrorMessage.toLowerCase();

        if (msg.contains("21211") || msg.contains("invalid phone")) {
            return "SMS_INVALID_RECIPIENT";
        } else if (msg.contains("20003") || msg.contains("authentication")) {
            return "SMS_AUTH_FAILED";
        } else if (msg.contains("20005") || msg.contains("rate")) {
            return "SMS_RATE_LIMITED";
        } else if (msg.contains("21601") || msg.contains("message body")) {
            return "SMS_INVALID_MESSAGE";
        } else if (msg.contains("50003") || msg.contains("internal")) {
            return "SMS_TWILIO_INTERNAL_ERROR";
        } else {
            return "SMS_TWILIO_ERROR";
        }
    }


    private String categorizeError(Exception e) {
        String message = e.getMessage().toLowerCase();

        if (message.contains("timeout") || message.contains("timed out")) {
            return "SMS_TIMEOUT";
        } else if (message.contains("connection")) {
            return "SMS_CONNECTION_ERROR";
        } else if (message.contains("credentials")) {
            return "SMS_CREDENTIALS_ERROR";
        } else {
            return "SMS_UNKNOWN_ERROR";
        }
    }

    private String maskPhone(String phone) {
        if (phone == null || phone.length() < 4) {
            return "***";
        }
        return phone.substring(0, Math.min(4, phone.length())) +
                "****" +
                phone.substring(Math.max(0, phone.length() - 4));
    }
}