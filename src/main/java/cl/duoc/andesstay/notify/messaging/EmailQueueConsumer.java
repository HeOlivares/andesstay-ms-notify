package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
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
	private final RabbitTopologyProperties topology;

	public EmailQueueConsumer(
			GuestNotificationService guestNotifications,
			ReliableDelivery delivery,
			RabbitTopologyProperties topology) {
		this.guestNotifications = guestNotifications;
		this.delivery = delivery;
		this.topology = topology;
	}

	@RabbitListener(queues = "${andesstay.notify.topology.queues.email}", ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				topology.getQueues().getEmail(),
				envelope,
				channel,
				deliveryTag,
				() -> guestNotifications.deliver(envelope));
	}
}
