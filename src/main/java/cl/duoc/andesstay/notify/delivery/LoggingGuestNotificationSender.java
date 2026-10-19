package cl.duoc.andesstay.notify.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LoggingGuestNotificationSender implements GuestNotificationSender {

	private static final Logger log = LoggerFactory.getLogger(LoggingGuestNotificationSender.class);

	@Override
	public void send(GuestNotification notification) {
		log.info(
				"Guest notification delivered reservationId={} channel={} kind={} eventId={}",
				notification.reservationId(),
				notification.channel(),
				notification.kind(),
				notification.eventId());
	}
}
