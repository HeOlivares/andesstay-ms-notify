package cl.duoc.andesstay.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Flujo cabin ready: la cabaña quedó lista (routing cabin.ready en cmd.direct). */
@Configuration
public class CabinReadyTopologyConfig {

	private final RabbitTopologyProperties topology;

	public CabinReadyTopologyConfig(RabbitTopologyProperties topology) {
		this.topology = topology;
	}

	@Bean
	Queue cabinReadyQueue() {
		String name = topology.getQueues().getCabinReady();
		return QueueBuilder.durable(name)
				.deadLetterExchange(topology.getExchanges().getDead())
				.deadLetterRoutingKey(topology.deadLetterQueue(name))
				.build();
	}

	@Bean
	Queue cabinReadyDeadLetterQueue() {
		return QueueBuilder.durable(topology.deadLetterQueue(topology.getQueues().getCabinReady())).build();
	}

	@Bean
	Binding cabinReadyDirectBinding(Queue cabinReadyQueue, DirectExchange cmdDirectExchange) {
		return BindingBuilder.bind(cabinReadyQueue)
				.to(cmdDirectExchange)
				.with(topology.getRoutingKeys().getCabinReady());
	}

	@Bean
	Binding cabinReadyDeadLetterBinding(Queue cabinReadyDeadLetterQueue, DirectExchange cmdDeadLetterExchange) {
		String dlq = cabinReadyDeadLetterQueue.getName();
		return BindingBuilder.bind(cabinReadyDeadLetterQueue).to(cmdDeadLetterExchange).with(dlq);
	}
}
