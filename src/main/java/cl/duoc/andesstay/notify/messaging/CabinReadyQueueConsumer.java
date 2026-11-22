package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import cl.duoc.andesstay.notify.delivery.CabinReadyService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class CabinReadyQueueConsumer {

	private final CabinReadyService cabinReady;
	private final ReliableDelivery delivery;
	private final RabbitTopologyProperties topology;

	public CabinReadyQueueConsumer(
			CabinReadyService cabinReady,
			ReliableDelivery delivery,
			RabbitTopologyProperties topology) {
		this.cabinReady = cabinReady;
		this.delivery = delivery;
		this.topology = topology;
	}

	@RabbitListener(queues = "${andesstay.notify.topology.queues.cabin-ready}", ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				topology.getQueues().getCabinReady(),
				envelope,
				channel,
				deliveryTag,
				() -> cabinReady.deliver(envelope));
	}
}
