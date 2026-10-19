package cl.duoc.andesstay.notify.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import cl.duoc.andesstay.notify.messaging.NotificationPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration")
@AutoConfigureMockMvc
class RabbitTopologyTest {

	@Autowired
	private Declarables topology;

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private NotificationPublisher publisher;

	@MockitoBean
	private RabbitTemplate rabbitTemplate;

	@Test
	void healthIsUp() throws Exception {
		mockMvc.perform(get("/api/notify/health"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("UP"))
				.andExpect(jsonPath("$.service").value("andesstay-ms-notify"));
	}

	@Test
	void declaresAndesStayExchangesQueuesAndDeadLetters() {
		assertEquals(List.of("cmd.direct", "cmd.topic", "cmd.dead.dlx"), exchangeNames());
		assertQueueWithDlq("q.cmd.email", "email.send", "email.*");
		assertQueueWithDlq("q.cmd.housekeeping", "housekeeping.ticket", "housekeeping.#");
		assertQueueWithDlq("q.cmd.voucher", "voucher.gen", "voucher.*");
	}

	@Test
	void publishedEventCarriesTheEnvelope() throws Exception {
		NotificationEnvelope envelope = NotificationEnvelope.create("email.send", "trace-1", "corr-1", null);
		publisher.publish(envelope);

		verify(rabbitTemplate).convertAndSend("cmd.direct", "email.send", envelope);
		String json = objectMapper.writeValueAsString(envelope);
		assertFalse(json.contains("\"type\":\"\""));
		for (String field : List.of("type", "eventId", "timestamp", "traceId", "correlationId")) {
			org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"" + field + "\""), field);
		}
	}

	private void assertQueueWithDlq(String queueName, String directKey, String topicPattern) {
		Queue queue = queue(queueName);
		Queue dlq = queue(queueName + ".dlq");
		assertEquals("cmd.dead.dlx", queue.getArguments().get("x-dead-letter-exchange"));
		assertEquals(dlq.getName(), queue.getArguments().get("x-dead-letter-routing-key"));
		assertFalse(dlq.getArguments().containsKey("x-dead-letter-exchange"));
		assertEquals(1, bindings(queueName, "cmd.direct", directKey).size());
		assertEquals(1, bindings(queueName, "cmd.topic", topicPattern).size());
		assertEquals(1, bindings(dlq.getName(), "cmd.dead.dlx", dlq.getName()).size());
	}

	private List<String> exchangeNames() {
		return topology.getDeclarables().stream()
				.filter(Exchange.class::isInstance)
				.map(Exchange.class::cast)
				.map(Exchange::getName)
				.toList();
	}

	private Queue queue(String name) {
		return topology.getDeclarables().stream()
				.filter(Queue.class::isInstance)
				.map(Queue.class::cast)
				.filter(queue -> queue.getName().equals(name))
				.findFirst()
				.orElseThrow();
	}

	private List<Declarable> bindings(String queueName, String exchange, String routingKey) {
		return topology.getDeclarables().stream()
				.filter(Binding.class::isInstance)
				.map(Binding.class::cast)
				.filter(binding -> queueName.equals(binding.getDestination()))
				.filter(binding -> exchange.equals(binding.getExchange()))
				.filter(binding -> routingKey.equals(binding.getRoutingKey()))
				.map(Declarable.class::cast)
				.toList();
	}
}
