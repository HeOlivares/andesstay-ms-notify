package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.HousekeepingTicketService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class HousekeepingQueueConsumer {

	private final HousekeepingTicketService tickets;
	private final ReliableDelivery delivery;

	public HousekeepingQueueConsumer(HousekeepingTicketService tickets, ReliableDelivery delivery) {
		this.tickets = tickets;
		this.delivery = delivery;
	}

	@RabbitListener(queues = RabbitTopologyConfig.HOUSEKEEPING_QUEUE, ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				RabbitTopologyConfig.HOUSEKEEPING_QUEUE,
				envelope,
				channel,
				deliveryTag,
				() -> tickets.deliver(envelope));
	}
}
