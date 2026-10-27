package cl.duoc.andesstay.notify.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({RabbitTopologyProperties.class, NotifyRetryProperties.class})
public class RabbitTopologyConfig {

	private final RabbitTopologyProperties topology;

	public RabbitTopologyConfig(RabbitTopologyProperties topology) {
		this.topology = topology;
	}

	@Bean
	public Declarables andesStayTopology() {
		DirectExchange direct = new DirectExchange(topology.getExchanges().getDirect(), true, false);
		TopicExchange topic = new TopicExchange(topology.getExchanges().getTopic(), true, false);
		DirectExchange dead = new DirectExchange(topology.getExchanges().getDead(), true, false);
		List<Declarable> declarables = new ArrayList<>();
		declarables.add(direct);
		declarables.add(topic);
		declarables.add(dead);
		addFlow(
				declarables,
				direct,
				topic,
				dead,
				topology.getQueues().getEmail(),
				topology.getRoutingKeys().getEmail(),
				topology.getTopicPatterns().getEmail());
		addFlow(
				declarables,
				direct,
				topic,
				dead,
				topology.getQueues().getHousekeeping(),
				topology.getRoutingKeys().getHousekeeping(),
				topology.getTopicPatterns().getHousekeeping());
		addFlow(
				declarables,
				direct,
				topic,
				dead,
				topology.getQueues().getVoucher(),
				topology.getRoutingKeys().getVoucher(),
				topology.getTopicPatterns().getVoucher());
		return new Declarables(declarables);
	}

	@Bean
	public MessageConverter jackson2JsonMessageConverter(ObjectMapper objectMapper) {
		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
		DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
		typeMapper.setTrustedPackages("cl.duoc.andesstay.notify.messaging");
		converter.setJavaTypeMapper(typeMapper);
		return converter;
	}

	private void addFlow(
			List<Declarable> declarables,
			DirectExchange direct,
			TopicExchange topic,
			DirectExchange dead,
			String queueName,
			String directKey,
			String topicPattern) {
		String dlqName = topology.deadLetterQueue(queueName);
		Queue queue = QueueBuilder.durable(queueName)
				.deadLetterExchange(topology.getExchanges().getDead())
				.deadLetterRoutingKey(dlqName)
				.build();
		Queue dlq = QueueBuilder.durable(dlqName).build();
		Binding directBinding = BindingBuilder.bind(queue).to(direct).with(directKey);
		Binding topicBinding = BindingBuilder.bind(queue).to(topic).with(topicPattern);
		Binding deadBinding = BindingBuilder.bind(dlq).to(dead).with(dlqName);
		declarables.add(queue);
		declarables.add(dlq);
		declarables.add(directBinding);
		declarables.add(topicBinding);
		declarables.add(deadBinding);
	}
}
