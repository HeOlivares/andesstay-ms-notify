package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class NotificationPublisher {

	private final RabbitTemplate rabbitTemplate;
	private final RabbitTopologyProperties topology;

	public NotificationPublisher(RabbitTemplate rabbitTemplate, RabbitTopologyProperties topology) {
		this.rabbitTemplate = rabbitTemplate;
		this.topology = topology;
	}

	public void publish(NotificationEnvelope envelope) {
		rabbitTemplate.convertAndSend(topology.getExchanges().getDirect(), envelope.type(), envelope);
	}
}
