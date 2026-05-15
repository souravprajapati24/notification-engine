package com.notification.notificationengine.service.channel.impl;

import com.notification.notificationengine.dto.NotificationEventDto;
import com.notification.notificationengine.exceptions.InvalidNotificationEventException;
import com.notification.notificationengine.exceptions.TransientNotificationException;
import com.notification.notificationengine.service.channel.EmailNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class EmailNotificationServiceImpl
        implements EmailNotificationService {

    private final JavaMailSender javaMailSender;

    @Value("${spring.mail.username}")
    private String fromEmail;

    @Override
    @Async
    public void sendEmail(NotificationEventDto eventDto) {

        validateEmailEvent(eventDto);

        try {
            log.info(
                    "Sending EMAIL notification for event {} to recipient {}",
                    eventDto.getEventId(),
                    eventDto.getEmail()
            );

            SimpleMailMessage mailMessage = buildMailMessage(eventDto);
            javaMailSender.send(mailMessage);
            log.info(
                    "EMAIL notification sent successfully for event {}",
                    eventDto.getEventId()
            );

        } catch (MailException e) {
            log.error(
                    "SMTP failure while sending EMAIL for event {}",
                    eventDto.getEventId(),
                    e
            );
            throw new TransientNotificationException(
                    "Temporary SMTP failure for event: "
                            + eventDto.getEventId(),
                    e
            );

        } catch (Exception e) {
            log.error(
                    "Unexpected EMAIL delivery failure for event {}",
                    eventDto.getEventId(),
                    e
            );
            throw new TransientNotificationException(
                    "Unexpected email delivery failure for event: "
                            + eventDto.getEventId(),
                    e
            );
        }
    }

    private void validateEmailEvent(NotificationEventDto eventDto) {

        if (eventDto == null) {
            throw new InvalidNotificationEventException(
                    "Notification event cannot be null"
            );
        }
        if (eventDto.getEventId() == null) {
            throw new InvalidNotificationEventException(
                    "Event ID is missing"
            );
        }
        if (eventDto.getEmail() == null || eventDto.getEmail().isBlank()) {

            throw new InvalidNotificationEventException(
                    "Recipient email is missing for event: "
                            + eventDto.getEventId()
            );
        }
        if (eventDto.getSubject() == null || eventDto.getSubject().isBlank()) {

            throw new InvalidNotificationEventException(
                    "Email subject is missing for event: "
                            + eventDto.getEventId()
            );
        }
        if (eventDto.getMessage() == null || eventDto.getMessage().isBlank()) {

            throw new InvalidNotificationEventException(
                    "Email message body is missing for event: "
                            + eventDto.getEventId()
            );
        }
    }

    private SimpleMailMessage buildMailMessage(
            NotificationEventDto eventDto
    ) {

        SimpleMailMessage mailMessage = new SimpleMailMessage();

        mailMessage.setFrom(fromEmail);
        mailMessage.setTo(eventDto.getEmail());
        mailMessage.setSubject(eventDto.getSubject());
        mailMessage.setText(eventDto.getMessage());

        return mailMessage;
    }
}