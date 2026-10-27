package cl.duoc.andesstay.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Flujo email: notificación al huésped (routing email.send / patrón email.*). */
@Configuration
public class EmailTopologyConfig {

	private final RabbitTopologyProperties topology;

	public EmailTopologyConfig(RabbitTopologyProperties topology) {
		this.topology = topology;
	}

	@Bean
	Queue emailQueue() {
		String name = topology.getQueues().getEmail();
		return QueueBuilder.durable(name)
				.deadLetterExchange(topology.getExchanges().getDead())
				.deadLetterRoutingKey(topology.deadLetterQueue(name))
				.build();
	}

	@Bean
	Queue emailDeadLetterQueue() {
		return QueueBuilder.durable(topology.deadLetterQueue(topology.getQueues().getEmail())).build();
	}

	@Bean
	Binding emailDirectBinding(Queue emailQueue, DirectExchange cmdDirectExchange) {
		return BindingBuilder.bind(emailQueue).to(cmdDirectExchange).with(topology.getRoutingKeys().getEmail());
	}

	@Bean
	Binding emailTopicBinding(Queue emailQueue, TopicExchange cmdTopicExchange) {
		return BindingBuilder.bind(emailQueue).to(cmdTopicExchange).with(topology.getTopicPatterns().getEmail());
	}

	@Bean
	Binding emailDeadLetterBinding(Queue emailDeadLetterQueue, DirectExchange cmdDeadLetterExchange) {
		String dlq = emailDeadLetterQueue.getName();
		return BindingBuilder.bind(emailDeadLetterQueue).to(cmdDeadLetterExchange).with(dlq);
	}
}
