package com.notification.notificationengine.service.channel.impl;
import com.notification.notificationengine.model.NotificationEvent;
import com.notification.notificationengine.model.enums.NotificationChannel;
import com.notification.notificationengine.service.channel.EmailNotificationService;
import com.notification.notificationengine.service.persistenceService.NotificationPersistenceService;
import com.notification.notificationengine.service.throttle.EmailThrottleService;
import com.resend.Resend;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.resend.services.emails.model.CreateEmailOptions;
import com.resend.services.emails.model.CreateEmailResponse;


@Service
@RequiredArgsConstructor
@Slf4j
public class EmailNotificationServiceImpl implements EmailNotificationService {

    private final NotificationPersistenceService persistenceService;
    private final EmailThrottleService emailThrottleService;
    @Value("${resend.api-key}")
    private String apiKey;
    @Value("${resend.from-email}")
    private String fromEmail;

    @Override
    @Async("deliveryExecutor")
    public void deliver(NotificationEvent event) {
        boolean permitted = false;

        try {

            emailThrottleService.acquire();
            permitted = true;

            String recipientEmail = extractEmailFromEvent(event);

            if (recipientEmail.isEmpty()) {
                throw new IllegalArgumentException("No email address found for user: " + event.getUserId());
            }

            log.debug("Preparing email delivery - Event: {}, Recipient: {},AvailableSlots: {}",
                    event.getId(), maskEmail(recipientEmail),emailThrottleService.getAvailablePermits());


            Resend resend = new Resend(apiKey);

            CreateEmailOptions createEmailRequest = CreateEmailOptions.builder()
                    .from(fromEmail)
                    .to(recipientEmail)
                    .subject("[" + event.getEventType() + "] " + buildSubject(event))
                    .text(event.getMessage())
                    .build();
            CreateEmailResponse response = resend.emails().send(createEmailRequest);



            log.info("Email sent successfully - Event: {}, Recipient: {}, ResendId: {}",
                    event.getId(), maskEmail(recipientEmail) , response.getId());

            persistenceService.markChannelDelivered(
                    event.getId(),
                    NotificationChannel.EMAIL,
                    recipientEmail
            );

        }catch (InterruptedException e){
            log.warn("Email delivery interrupted while waiting for throttle - Event: {}",
                    event.getId());
            Thread.currentThread().interrupt();
        }

        catch (Exception e) {
            String errorCode = categorizeError(e);

            if (persistenceService.isRetriable(errorCode)) {

                boolean retrySchedule = persistenceService.markChannelForRetry(
                        event.getId(),
                        NotificationChannel.EMAIL,
                        e.getMessage(),
                        errorCode
                );

                if (retrySchedule) {
                    log.warn("⟳ Email delivery retriable error - Event: {}, Error: {}, Will retry",
                            event.getId(), errorCode);

                } else {

                    log.error(
                            "✗ Email delivery permanently failed after retries exhausted - Event: {}, Error: {}",
                            event.getId(),
                            errorCode
                    );
                }

            } else {

                persistenceService.markChannelFailed(
                        event.getId(),
                        NotificationChannel.EMAIL,
                        e.getMessage(),
                        errorCode
                );

                persistenceService.sendToDlt(
                        event.getId(),
                        NotificationChannel.EMAIL,
                        e.getMessage(),
                        errorCode);


                log.error(
                        "✗ Email delivery failed permanently - Event: {}, Error: {}",
                        event.getId(),
                        errorCode
                );
            }
        }
        finally {
            if (permitted) {
                emailThrottleService.release();
                log.debug("Released email throttle permit - Event: {}, AvailableSlots: {}",
                        event.getId(),
                        emailThrottleService.getAvailablePermits()
                );
            }
        }
    }

    private String extractEmailFromEvent(NotificationEvent event) {
        if (event.getMetadata() != null && event.getMetadata().has("email")) {

            String email = event.getMetadata()
                    .get("email")
                    .asText();

            if (email != null && !email.isBlank()) {
                return email;
            }
        }

        throw new IllegalArgumentException(
                "Recipient email missing for event: " + event.getId()
        );
    }

    private String buildSubject(NotificationEvent event) {
        return switch (event.getEventType()) {
            case "ORDER_CONFIRMED" -> "Order Confirmed";
            case "ORDER_SHIPPED" -> "Order Shipped";
            case "DELIVERY_COMPLETE" -> "Delivery Complete";
            case "PAYMENT_RECEIVED" -> "Payment Received";
            case "PAYMENT_FAILED" -> "Payment Failed";
            case "NOTIFICATION_EVENT"->"Notification Received";
            default -> "Notification";
        };
    }

    private String categorizeError(Exception e) {
        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";

        if (message.contains("timeout") || message.contains("timed out"))          return "EMAIL_TIMEOUT";
        if (message.contains("connection"))                                          return "EMAIL_CONNECTION_ERROR";
        if (message.contains("rate limit") || message.contains("429"))              return "EMAIL_RATE_LIMITED";
        if (message.contains("unauthorized") || message.contains("401"))            return "EMAIL_AUTH_FAILED";
        if (message.contains("500") || message.contains("internal server"))         return "EMAIL_SERVER_ERROR";

        if (message.contains("domain is not verified") || message.contains("validation_error")
                || message.contains("you can only send testing emails"))         return "EMAIL_INVALID_SENDER";
        if (message.contains("invalid") || message.contains("400"))                return "EMAIL_INVALID_FORMAT";

        return "EMAIL_UNKNOWN_ERROR";
    }

    private String maskEmail(String email) {
        if (email == null || !email.contains("@")) {
            return "***";
        }
        String[] parts = email.split("@");
        return parts[0].substring(0, Math.min(3, parts[0].length())) + "@***";
    }
}