package cl.duoc.andesstay.notify.config;

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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitTopologyConfig {

	public static final String DIRECT_EXCHANGE = "cmd.direct";
	public static final String TOPIC_EXCHANGE = "cmd.topic";
	public static final String DEAD_EXCHANGE = "cmd.dead.dlx";
	public static final String EMAIL_QUEUE = "q.cmd.email";
	public static final String HOUSEKEEPING_QUEUE = "q.cmd.housekeeping";
	public static final String VOUCHER_QUEUE = "q.cmd.voucher";
	public static final String EMAIL_ROUTING_KEY = "email.send";
	public static final String HOUSEKEEPING_ROUTING_KEY = "housekeeping.ticket";
	public static final String VOUCHER_ROUTING_KEY = "voucher.gen";

	@Bean
	public Declarables andesStayTopology() {
		DirectExchange direct = new DirectExchange(DIRECT_EXCHANGE, true, false);
		TopicExchange topic = new TopicExchange(TOPIC_EXCHANGE, true, false);
		DirectExchange dead = new DirectExchange(DEAD_EXCHANGE, true, false);
		List<Declarable> declarables = new ArrayList<>();
		declarables.add(direct);
		declarables.add(topic);
		declarables.add(dead);
		addFlow(declarables, direct, topic, dead, EMAIL_QUEUE, EMAIL_ROUTING_KEY, "email.*");
		addFlow(declarables, direct, topic, dead, HOUSEKEEPING_QUEUE, HOUSEKEEPING_ROUTING_KEY, "housekeeping.#");
		addFlow(declarables, direct, topic, dead, VOUCHER_QUEUE, VOUCHER_ROUTING_KEY, "voucher.*");
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

	private static void addFlow(
			List<Declarable> declarables,
			DirectExchange direct,
			TopicExchange topic,
			DirectExchange dead,
			String queueName,
			String directKey,
			String topicPattern) {
		String dlqName = queueName + ".dlq";
		Queue queue = QueueBuilder.durable(queueName)
				.deadLetterExchange(DEAD_EXCHANGE)
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
