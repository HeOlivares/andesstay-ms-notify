package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration")
class ReservationActivityListenerConcurrencyTest {

	@MockitoBean
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Test
	void activityListenerResolvesConcurrencyFromConfiguration() {
		List<MessageListenerContainer> containers = List.copyOf(registry.getListenerContainers());
		assertEquals(1, containers.size());

		ConcurrentMessageListenerContainer<?, ?> container = assertInstanceOf(
				ConcurrentMessageListenerContainer.class, containers.get(0));
		assertEquals(List.of("reservations.activity"), List.of(container.getContainerProperties().getTopics()));
		assertEquals("notify-activity", container.getGroupId());
		assertEquals(3, container.getConcurrency());
	}
}
