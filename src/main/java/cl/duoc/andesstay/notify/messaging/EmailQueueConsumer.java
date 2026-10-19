package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.GuestNotificationService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class EmailQueueConsumer {

	private final GuestNotificationService guestNotifications;

	public EmailQueueConsumer(GuestNotificationService guestNotifications) {
		this.guestNotifications = guestNotifications;
	}

	@RabbitListener(queues = RabbitTopologyConfig.EMAIL_QUEUE)
	public void onMessage(NotificationEnvelope envelope) {
		guestNotifications.deliver(envelope);
	}
}
