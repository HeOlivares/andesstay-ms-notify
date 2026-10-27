package cl.duoc.andesstay.notify.config;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Exchanges compartidos: comando directo, topic y dead-letter. */
@Configuration
public class ExchangeTopologyConfig {

	private final RabbitTopologyProperties topology;

	public ExchangeTopologyConfig(RabbitTopologyProperties topology) {
		this.topology = topology;
	}

	@Bean
	DirectExchange cmdDirectExchange() {
		return new DirectExchange(topology.getExchanges().getDirect(), true, false);
	}

	@Bean
	TopicExchange cmdTopicExchange() {
		return new TopicExchange(topology.getExchanges().getTopic(), true, false);
	}

	@Bean
	DirectExchange cmdDeadLetterExchange() {
		return new DirectExchange(topology.getExchanges().getDead(), true, false);
	}
}
