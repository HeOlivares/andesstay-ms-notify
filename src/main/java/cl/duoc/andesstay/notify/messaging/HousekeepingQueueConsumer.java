package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
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
	private final RabbitTopologyProperties topology;

	public HousekeepingQueueConsumer(
			HousekeepingTicketService tickets,
			ReliableDelivery delivery,
			RabbitTopologyProperties topology) {
		this.tickets = tickets;
		this.delivery = delivery;
		this.topology = topology;
	}

	@RabbitListener(queues = "${andesstay.notify.topology.queues.housekeeping}", ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				topology.getQueues().getHousekeeping(),
				envelope,
				channel,
				deliveryTag,
				() -> tickets.deliver(envelope));
	}
}
