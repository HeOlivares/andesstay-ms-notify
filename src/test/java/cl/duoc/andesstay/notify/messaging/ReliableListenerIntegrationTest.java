package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.GuestNotification;
import cl.duoc.andesstay.notify.delivery.GuestNotificationSender;
import cl.duoc.andesstay.notify.delivery.HousekeepingTicket;
import cl.duoc.andesstay.notify.delivery.HousekeepingTicketSender;
import cl.duoc.andesstay.notify.delivery.VoucherDocument;
import cl.duoc.andesstay.notify.delivery.VoucherSender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class ReliableListenerIntegrationTest {

	@Container
	static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:3-alpine");

	@DynamicPropertySource
	static void rabbitProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.rabbitmq.host", RABBIT::getHost);
		registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
		registry.add("spring.rabbitmq.username", () -> "guest");
		registry.add("spring.rabbitmq.password", () -> "guest");
		registry.add("andesstay.notify.processed-event-ttl", () -> "PT1H");
	}

	@Autowired
	private NotificationPublisher publisher;

	@Autowired
	private RabbitAdmin rabbitAdmin;

	@Autowired
	private ConnectionFactory connectionFactory;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private DlqRateMeter dlqRateMeter;

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private GuestNotificationSender guestNotificationSender;

	@MockitoBean
	private HousekeepingTicketSender housekeepingTicketSender;

	@MockitoBean
	private VoucherSender voucherSender;

	@BeforeEach
	void cleanQueuesAndMocks() throws InterruptedException {
		reset(guestNotificationSender, housekeepingTicketSender, voucherSender);
		purge(RabbitTopologyConfig.EMAIL_QUEUE);
		purge(RabbitTopologyConfig.HOUSEKEEPING_QUEUE);
		purge(RabbitTopologyConfig.VOUCHER_QUEUE);
		awaitListenersReady();
	}

	@Test
	void emailMessageIsAcknowledgedByTheRealListener() throws Exception {
		long attemptsBefore = dlqRateMeter.attempts();
		long deadBefore = dlqRateMeter.deadLetters(RabbitTopologyConfig.EMAIL_QUEUE);
		publisher.publish(email("event-email-ok", "CONFIRMATION"));

		verify(guestNotificationSender, timeout(15_000).times(1)).send(any(GuestNotification.class));
		awaitEmpty(RabbitTopologyConfig.EMAIL_QUEUE);
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.EMAIL_QUEUE)));
		assertEquals(attemptsBefore + 1, dlqRateMeter.attempts());
		assertEquals(deadBefore, dlqRateMeter.deadLetters(RabbitTopologyConfig.EMAIL_QUEUE));
	}

	@Test
	void housekeepingMessageIsAcknowledgedByTheRealListener() throws Exception {
		publisher.publish(housekeeping("event-hk-ok", "PREPARATION"));

		verify(housekeepingTicketSender, timeout(15_000).times(1)).send(any(HousekeepingTicket.class));
		awaitEmpty(RabbitTopologyConfig.HOUSEKEEPING_QUEUE);
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.HOUSEKEEPING_QUEUE)));
	}

	@Test
	void voucherMessageIsAcknowledgedByTheRealListener() throws Exception {
		publisher.publish(voucher("event-voucher-ok", "VOUCHER"));

		verify(voucherSender, timeout(15_000).times(1)).send(any(VoucherDocument.class));
		awaitEmpty(RabbitTopologyConfig.VOUCHER_QUEUE);
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.VOUCHER_QUEUE)));
	}

	@Test
	void failedEmailIsNackedAndRoutedToItsDeadLetterQueue() throws Exception {
		long attemptsBefore = dlqRateMeter.attempts();
		long deadBefore = dlqRateMeter.deadLetters();
		doThrow(new IllegalStateException("mail gateway down")).when(guestNotificationSender).send(any());

		publisher.publish(email("event-email-fail", "CONFIRMATION"));

		awaitCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.EMAIL_QUEUE), 1);
		assertEquals(0, messageCount(RabbitTopologyConfig.EMAIL_QUEUE));
		assertEquals(attemptsBefore + 1, dlqRateMeter.attempts());
		assertEquals(deadBefore + 1, dlqRateMeter.deadLetters());
		assertTrue(dlqRateMeter.deadLetters(RabbitTopologyConfig.EMAIL_QUEUE) >= 1);
		assertTrue(dlqRateMeter.rate() > 0d);

		try (Channel channel = connectionFactory.createConnection().createChannel(false)) {
			GetResponse dead = channel.basicGet(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.EMAIL_QUEUE), true);
			assertNotNull(dead);
			assertNotNull(dead.getProps().getHeaders().get("x-death"));
			assertTrue(new String(dead.getBody(), StandardCharsets.UTF_8).contains("event-email-fail"));
		}
	}

	@Test
	void failedHousekeepingAndVoucherReachTheirDeadLetterQueues() throws Exception {
		doThrow(new IllegalStateException("housekeeping unavailable")).when(housekeepingTicketSender).send(any());
		doThrow(new IllegalStateException("pdf unavailable")).when(voucherSender).send(any());

		publisher.publish(housekeeping("event-hk-fail", "CLEANING"));
		publisher.publish(voucher("event-voucher-fail", "RECEIPT"));

		awaitCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.HOUSEKEEPING_QUEUE), 1);
		awaitCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.VOUCHER_QUEUE), 1);
		assertEquals(0, messageCount(RabbitTopologyConfig.HOUSEKEEPING_QUEUE));
		assertEquals(0, messageCount(RabbitTopologyConfig.VOUCHER_QUEUE));
	}

	@Test
	void sameEventIdIsDeliveredOnlyOnceByTheRealListener() throws Exception {
		publisher.publish(email("event-dup", "CONFIRMATION"));
		verify(guestNotificationSender, timeout(15_000).times(1)).send(any(GuestNotification.class));
		awaitEmpty(RabbitTopologyConfig.EMAIL_QUEUE);

		publisher.publish(email("event-dup", "CONFIRMATION"));
		awaitEmpty(RabbitTopologyConfig.EMAIL_QUEUE);

		verify(guestNotificationSender, times(1)).send(any(GuestNotification.class));
		assertEquals(0, messageCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.EMAIL_QUEUE)));
	}

	@Test
	void concurrentDuplicatesExecuteTheWorkOnlyOnce() throws Exception {
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicInteger runs = new AtomicInteger();
		doAnswer(invocation -> {
			runs.incrementAndGet();
			started.countDown();
			assertTrue(release.await(5, TimeUnit.SECONDS));
			return null;
		}).when(guestNotificationSender).send(any());

		publisher.publish(email("event-race", "CONFIRMATION"));
		assertTrue(started.await(5, TimeUnit.SECONDS));
		publisher.publish(email("event-race", "CONFIRMATION"));
		release.countDown();

		awaitEmpty(RabbitTopologyConfig.EMAIL_QUEUE);
		assertEquals(1, runs.get());
		verify(guestNotificationSender, times(1)).send(any(GuestNotification.class));
	}

	@Test
	void aFailedEventIdCanBeRetriedAfterRelease() throws Exception {
		CountDownLatch failed = new CountDownLatch(1);
		AtomicInteger runs = new AtomicInteger();
		doAnswer(invocation -> {
			if (runs.incrementAndGet() == 1) {
				failed.countDown();
				throw new IllegalStateException("temporary");
			}
			return null;
		}).when(guestNotificationSender).send(any());

		publisher.publish(email("event-retry", "CONFIRMATION"));
		assertTrue(failed.await(15, TimeUnit.SECONDS));
		awaitCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.EMAIL_QUEUE), 1);

		publisher.publish(email("event-retry", "CONFIRMATION"));
		verify(guestNotificationSender, timeout(15_000).times(2)).send(any(GuestNotification.class));
		awaitEmpty(RabbitTopologyConfig.EMAIL_QUEUE);
	}

	@Test
	void dlqMetricsEndpointExposesAttemptsDeadLettersAndRate() throws Exception {
		long attemptsBefore = dlqRateMeter.attempts();
		long deadBefore = dlqRateMeter.deadLetters();
		doThrow(new IllegalStateException("mail gateway down")).when(guestNotificationSender).send(any());
		publisher.publish(email("event-metrics", "CONFIRMATION"));
		awaitCount(ReliableDelivery.deadLetterQueue(RabbitTopologyConfig.EMAIL_QUEUE), 1);

		mockMvc.perform(get("/api/notify/metrics/dlq"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.attempts").value((int) (attemptsBefore + 1)))
				.andExpect(jsonPath("$.deadLetters").value((int) (deadBefore + 1)))
				.andExpect(jsonPath("$.rate").isNumber())
				.andExpect(jsonPath("$.deadLettersByQueue['" + RabbitTopologyConfig.EMAIL_QUEUE + "']").exists());
	}

	private NotificationEnvelope email(String eventId, String kind) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("guestName", "Ana");
		payload.put("kind", kind);
		payload.put("channel", "EMAIL");
		payload.put("guestEmail", "ana@andesstay.local");
		return new NotificationEnvelope(
				RabbitTopologyConfig.EMAIL_ROUTING_KEY,
				eventId,
				Instant.parse("2026-10-19T04:00:00Z"),
				"trace-1",
				"corr-1",
				payload);
	}

	private NotificationEnvelope housekeeping(String eventId, String kind) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("unitId", "U-4");
		payload.put("kind", kind);
		return new NotificationEnvelope(
				RabbitTopologyConfig.HOUSEKEEPING_ROUTING_KEY,
				eventId,
				Instant.parse("2026-10-19T04:00:00Z"),
				"trace-1",
				"corr-1",
				payload);
	}

	private NotificationEnvelope voucher(String eventId, String document) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("guestName", "Ana");
		payload.put("document", document);
		return new NotificationEnvelope(
				RabbitTopologyConfig.VOUCHER_ROUTING_KEY,
				eventId,
				Instant.parse("2026-10-19T04:00:00Z"),
				"trace-1",
				"corr-1",
				payload);
	}

	private void awaitEmpty(String queue) throws InterruptedException {
		awaitCount(queue, 0);
	}

	private void awaitCount(String queue, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (System.nanoTime() < deadline) {
			if (messageCount(queue) == expected) {
				return;
			}
			Thread.sleep(50);
		}
		assertEquals(expected, messageCount(queue), "queue=" + queue);
	}

	private int messageCount(String queue) {
		Properties properties = rabbitAdmin.getQueueProperties(queue);
		assertNotNull(properties, "missing queue " + queue);
		return Integer.parseInt(String.valueOf(properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT)));
	}

	private void purge(String queue) {
		rabbitAdmin.purgeQueue(queue, false);
		rabbitAdmin.purgeQueue(ReliableDelivery.deadLetterQueue(queue), false);
	}

	private void awaitListenersReady() throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (System.nanoTime() < deadline) {
			try {
				messageCount(RabbitTopologyConfig.EMAIL_QUEUE);
				messageCount(RabbitTopologyConfig.HOUSEKEEPING_QUEUE);
				messageCount(RabbitTopologyConfig.VOUCHER_QUEUE);
				Thread.sleep(200);
				return;
			} catch (AssertionError | RuntimeException ignored) {
				Thread.sleep(100);
			}
		}
	}
}
