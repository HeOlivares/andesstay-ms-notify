package cl.duoc.andesstay.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Flujo housekeeping: ticket de preparación/limpieza (housekeeping.ticket / housekeeping.#). */
@Configuration
public class HousekeepingTopologyConfig {

	private final RabbitTopologyProperties topology;

	public HousekeepingTopologyConfig(RabbitTopologyProperties topology) {
		this.topology = topology;
	}

	@Bean
	Queue housekeepingQueue() {
		String name = topology.getQueues().getHousekeeping();
		return QueueBuilder.durable(name)
				.deadLetterExchange(topology.getExchanges().getDead())
				.deadLetterRoutingKey(topology.deadLetterQueue(name))
				.build();
	}

	@Bean
	Queue housekeepingDeadLetterQueue() {
		return QueueBuilder.durable(topology.deadLetterQueue(topology.getQueues().getHousekeeping())).build();
	}

	@Bean
	Binding housekeepingDirectBinding(Queue housekeepingQueue, DirectExchange cmdDirectExchange) {
		return BindingBuilder.bind(housekeepingQueue)
				.to(cmdDirectExchange)
				.with(topology.getRoutingKeys().getHousekeeping());
	}

	@Bean
	Binding housekeepingTopicBinding(Queue housekeepingQueue, TopicExchange cmdTopicExchange) {
		return BindingBuilder.bind(housekeepingQueue)
				.to(cmdTopicExchange)
				.with(topology.getTopicPatterns().getHousekeeping());
	}

	@Bean
	Binding housekeepingDeadLetterBinding(Queue housekeepingDeadLetterQueue, DirectExchange cmdDeadLetterExchange) {
		String dlq = housekeepingDeadLetterQueue.getName();
		return BindingBuilder.bind(housekeepingDeadLetterQueue).to(cmdDeadLetterExchange).with(dlq);
	}
}
