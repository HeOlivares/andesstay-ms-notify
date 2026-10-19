package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class AsyncNotificationFlowTest {

	private static final String EMAIL_QUEUE = "q.cmd.email";
	private static final String HOUSEKEEPING_QUEUE = "q.cmd.housekeeping";
	private static final String VOUCHER_QUEUE = "q.cmd.voucher";

	@Container
	final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3-alpine");

	private CachingConnectionFactory connectionFactory;
	private RabbitAdmin admin;
	private NotificationPublisher publisher;
	private ReliableDelivery delivery;

	@BeforeEach
	void setUp() {
		connectionFactory = new CachingConnectionFactory(rabbit.getHost(), rabbit.getAmqpPort());
		admin = new RabbitAdmin(connectionFactory);
		for (Declarable declarable : new RabbitTopologyConfig().andesStayTopology().getDeclarables()) {
			if (declarable instanceof Exchange exchange) {
				admin.declareExchange(exchange);
			} else if (declarable instanceof Queue queue) {
				admin.declareQueue(queue);
			} else if (declarable instanceof Binding binding) {
				admin.declareBinding(binding);
			}
		}
		purge(EMAIL_QUEUE, HOUSEKEEPING_QUEUE, VOUCHER_QUEUE);
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setMessageConverter(new Jackson2JsonMessageConverter());
		publisher = new NotificationPublisher(template);
		delivery = new ReliableDelivery(new ProcessedEventIds(Duration.ofHours(1)), new DlqRateMeter());
	}

	@AfterEach
	void tearDown() {
		if (connectionFactory != null) {
			connectionFactory.destroy();
		}
	}

	@Test
	void confirmedReservationRemainsQueuedUntilEmailAndHousekeepingAreAcknowledged() throws Exception {
		publishConfirmedReservation(false);

		awaitCount(EMAIL_QUEUE, 1);
		awaitCount(HOUSEKEEPING_QUEUE, 1);
		assertEquals(0, messageCount(VOUCHER_QUEUE));

		acknowledge(EMAIL_QUEUE, "email.send", "event-email");
		acknowledge(HOUSEKEEPING_QUEUE, "housekeeping.ticket", "event-housekeeping");

		assertEquals(0, messageCount(EMAIL_QUEUE));
		assertEquals(0, messageCount(HOUSEKEEPING_QUEUE));
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(EMAIL_QUEUE)));
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(HOUSEKEEPING_QUEUE)));
	}

	@Test
	void voucherIsQueuedOnlyWhenTheReservationIncludesIt() throws Exception {
		publishConfirmedReservation(true);

		awaitCount(VOUCHER_QUEUE, 1);
		acknowledge(EMAIL_QUEUE, "email.send", "event-email");
		acknowledge(HOUSEKEEPING_QUEUE, "housekeeping.ticket", "event-housekeeping");
		acknowledge(VOUCHER_QUEUE, "voucher.gen", "event-voucher");

		assertEquals(0, messageCount(VOUCHER_QUEUE));
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(VOUCHER_QUEUE)));
	}

	@Test
	void failedEmailIsRoutedByTheBrokerToItsDeadLetterQueue() throws Exception {
		NotificationEnvelope email = envelope("email.send", "event-failed");
		publisher.publish(email);

		try (Channel channel = connectionFactory.createConnection().createChannel(false)) {
			GetResponse response = channel.basicGet(EMAIL_QUEUE, false);
			assertNotNull(response);
			assertEquals("email.send", response.getEnvelope().getRoutingKey());

			DeliveryResult result = delivery.consume(EMAIL_QUEUE, email, channel, response.getEnvelope().getDeliveryTag(), () -> {
				throw new IllegalStateException("mail gateway down");
			});

			assertEquals(DeliveryResult.NACK, result);
			String deadLetterQueue = ReliableDelivery.deadLetterQueue(EMAIL_QUEUE);
			awaitCount(deadLetterQueue, 1);
			assertEquals(0, messageCount(EMAIL_QUEUE));
			GetResponse dead = channel.basicGet(deadLetterQueue, false);
			assertNotNull(dead);
			assertNotNull(dead.getProps().getHeaders().get("x-death"));
			assertTrue(body(dead).contains("event-failed"));
		}
	}

	private void publishConfirmedReservation(boolean voucher) {
		publisher.publish(envelope("email.send", "event-email"));
		publisher.publish(envelope("housekeeping.ticket", "event-housekeeping"));
		if (voucher) {
			publisher.publish(envelope("voucher.gen", "event-voucher"));
		}
	}

	private void acknowledge(String queue, String routingKey, String eventId) throws Exception {
		NotificationEnvelope envelope = envelope(routingKey, eventId);
		try (Channel channel = connectionFactory.createConnection().createChannel(false)) {
			GetResponse response = channel.basicGet(queue, false);
			assertNotNull(response);
			assertEquals(routingKey, response.getEnvelope().getRoutingKey());
			assertTrue(body(response).contains(eventId));
			DeliveryResult result = delivery.consume(
					queue,
					envelope,
					channel,
					response.getEnvelope().getDeliveryTag(),
					() -> {
					});
			assertEquals(DeliveryResult.ACK, result);
		}
	}

	private void awaitCount(String queue, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			if (messageCount(queue) == expected) {
				return;
			}
			Thread.sleep(50);
		}
		assertEquals(expected, messageCount(queue));
	}

	private int messageCount(String queue) {
		Properties properties = admin.getQueueProperties(queue);
		assertNotNull(properties);
		return Integer.parseInt(String.valueOf(properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT)));
	}

	private void purge(String... queues) {
		for (String queue : queues) {
			admin.purgeQueue(queue, false);
			admin.purgeQueue(ReliableDelivery.deadLetterQueue(queue), false);
		}
	}

	private static String body(GetResponse response) {
		return new String(response.getBody(), StandardCharsets.UTF_8);
	}

	private static NotificationEnvelope envelope(String type, String eventId) {
		return new NotificationEnvelope(type, eventId, Instant.parse("2026-10-19T03:40:00Z"), "trace-1", "corr-1", null);
	}
}
