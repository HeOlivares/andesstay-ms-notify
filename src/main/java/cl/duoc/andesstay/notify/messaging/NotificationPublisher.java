package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class NotificationPublisher {

	private final RabbitTemplate rabbitTemplate;

	public NotificationPublisher(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}

	public void publish(NotificationEnvelope envelope) {
		rabbitTemplate.convertAndSend(RabbitTopologyConfig.DIRECT_EXCHANGE, envelope.type(), envelope);
	}
}
