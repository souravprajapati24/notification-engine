package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.config.TwilioConfig;
import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.service.channel.SmsNotificationService;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceService;
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

    private final NotificationPersistenceService persistenceService;
    private final TwilioConfig twilioConfig;

    @Async
    @Override
    public void deliver(NotificationEvent event) {

        try {

            String recipientPhone = extractPhoneFromEvent(event);

            if (recipientPhone == null || recipientPhone.isEmpty()) {
                throw new IllegalArgumentException("No phone number found for user: " + event.getUserId());
            }

            log.debug("Preparing SMS delivery - Event: {}, Recipient: {}",
                    event.getId(),
                    maskPhone(recipientPhone)
            );

            validateTwilioConfiguration();
            Message message = Message.creator(
                    new PhoneNumber(recipientPhone),
                    new PhoneNumber(twilioConfig.getFromNumber()),
                    event.getMessage()
            ).create();

            log.info(
                    "✓ SMS sent successfully - Event: {}, Recipient: {}, MessageSid: {}",
                    event.getId(),
                    maskPhone(recipientPhone),
                    message.getSid()
            );

            persistenceService.markChannelDelivered(
                    event.getId(),
                    NotificationChannel.SMS,
                    recipientPhone
            );

        } catch (Exception e) {

            String errorCode = determineErrorCode(e);
            if (persistenceService.isRetriable(errorCode)) {

                boolean retryScheduled = persistenceService.markChannelForRetry(
                        event.getId(),
                        NotificationChannel.SMS,
                        e.getMessage(),
                        errorCode
                );
                if (retryScheduled) {
                    log.warn(
                            "⟳ SMS delivery retriable error - Event: {}, Error: {}, Will retry",
                            event.getId(),
                            errorCode
                    );

                } else {
                    log.error(
                            "✗ SMS delivery permanently failed after retries exhausted - Event: {}, Error: {}",
                            event.getId(),
                            errorCode
                    );
                }

            } else {
                persistenceService.markChannelFailed(
                        event.getId(),
                        NotificationChannel.SMS,
                        e.getMessage(),
                        errorCode
                );
                persistenceService.sendToDlt(
                        event.getId(),
                        NotificationChannel.SMS,
                        e.getMessage(),
                        errorCode
                );
                log.error(
                        "✗ SMS delivery failed permanently - Event: {}, Error: {}",
                        event.getId(),
                        errorCode
                );
            }
        }
    }

    private String extractPhoneFromEvent(NotificationEvent event) {

        if (event.getMetadata() != null
                && event.getMetadata().has("phone")) {

            return event.getMetadata()
                    .get("phone")
                    .asText();
        }

        return null;
    }

    private void validateTwilioConfiguration() {

        if (twilioConfig.getAccountSid() == null
                || twilioConfig.getAuthToken() == null
                || twilioConfig.getFromNumber() == null) {

            throw new IllegalStateException(
                    "Twilio credentials not configured"
            );
        }
    }

    private String determineErrorCode(Exception e) {

        if (e instanceof ApiException apiException) {
            return categorizeTwilioError(apiException);
        }

        return categorizeGenericError(e);
    }

    private String categorizeTwilioError(ApiException e) {

        Integer code = e.getCode();

        if (code == null) {
            return "SMS_TWILIO_ERROR";
        }

        return switch (code) {

            case 21211 -> "SMS_INVALID_RECIPIENT";
            case 20003 -> "SMS_AUTH_FAILED";
            case 20005 -> "SMS_RATE_LIMITED";
            case 21601 -> "SMS_INVALID_MESSAGE";
            case 50003 -> "SMS_TWILIO_INTERNAL_ERROR";
            default -> "SMS_TWILIO_ERROR";
        };
    }

    private String categorizeGenericError(Exception e) {

        String message = e.getMessage();

        if (message == null) {
            return "SMS_UNKNOWN_ERROR";
        }

        message = message.toLowerCase();

        if (message.contains("timeout")|| message.contains("timed out")) {
            return "SMS_TIMEOUT";

        } else if (message.contains("connection")) {
            return "SMS_CONNECTION_ERROR";

        } else if (message.contains("credentials")) {
            return "SMS_CREDENTIALS_ERROR";

        } else if (message.contains("network")) {
            return "SMS_NETWORK_ERROR";

        } else {
            return "SMS_UNKNOWN_ERROR";
        }
    }

    private String maskPhone(String phone) {

        if (phone == null || phone.length() < 4) {
            return "***";
        }
        return phone.substring(0, Math.min(4, phone.length()))
                + "****"
                + phone.substring(Math.max(0, phone.length() - 4));
    }
}