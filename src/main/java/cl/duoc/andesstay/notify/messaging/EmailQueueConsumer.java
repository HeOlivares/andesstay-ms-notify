package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.GuestNotificationService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class EmailQueueConsumer {

	private final GuestNotificationService guestNotifications;
	private final ReliableDelivery delivery;

	public EmailQueueConsumer(GuestNotificationService guestNotifications, ReliableDelivery delivery) {
		this.guestNotifications = guestNotifications;
		this.delivery = delivery;
	}

	@RabbitListener(queues = RabbitTopologyConfig.EMAIL_QUEUE, ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				RabbitTopologyConfig.EMAIL_QUEUE,
				envelope,
				channel,
				deliveryTag,
				() -> guestNotifications.deliver(envelope));
	}
}
