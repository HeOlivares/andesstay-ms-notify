package cl.duoc.andesstay.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Flujo voucher: generación de comprobante (voucher.gen / voucher.*). */
@Configuration
public class VoucherTopologyConfig {

	private final RabbitTopologyProperties topology;

	public VoucherTopologyConfig(RabbitTopologyProperties topology) {
		this.topology = topology;
	}

	@Bean
	Queue voucherQueue() {
		String name = topology.getQueues().getVoucher();
		return QueueBuilder.durable(name)
				.deadLetterExchange(topology.getExchanges().getDead())
				.deadLetterRoutingKey(topology.deadLetterQueue(name))
				.build();
	}

	@Bean
	Queue voucherDeadLetterQueue() {
		return QueueBuilder.durable(topology.deadLetterQueue(topology.getQueues().getVoucher())).build();
	}

	@Bean
	Binding voucherDirectBinding(Queue voucherQueue, DirectExchange cmdDirectExchange) {
		return BindingBuilder.bind(voucherQueue).to(cmdDirectExchange).with(topology.getRoutingKeys().getVoucher());
	}

	@Bean
	Binding voucherTopicBinding(Queue voucherQueue, TopicExchange cmdTopicExchange) {
		return BindingBuilder.bind(voucherQueue).to(cmdTopicExchange).with(topology.getTopicPatterns().getVoucher());
	}

	@Bean
	Binding voucherDeadLetterBinding(Queue voucherDeadLetterQueue, DirectExchange cmdDeadLetterExchange) {
		String dlq = voucherDeadLetterQueue.getName();
		return BindingBuilder.bind(voucherDeadLetterQueue).to(cmdDeadLetterExchange).with(dlq);
	}
}
