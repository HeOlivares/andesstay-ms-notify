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

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
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
	private RabbitTopologyProperties topology;

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
		purge(topology.getQueues().getEmail());
		purge(topology.getQueues().getHousekeeping());
		purge(topology.getQueues().getVoucher());
		awaitListenersReady();
	}

	@Test
	void emailMessageIsAcknowledgedByTheRealListener() throws Exception {
		long attemptsBefore = dlqRateMeter.attempts();
		long deadBefore = dlqRateMeter.deadLetters(topology.getQueues().getEmail());
		publisher.publish(email("event-email-ok", "CONFIRMATION"));

		verify(guestNotificationSender, timeout(15_000).times(1)).send(any(GuestNotification.class));
		awaitEmpty(topology.getQueues().getEmail());
		assertEquals(0, messageCount(topology.deadLetterQueue(topology.getQueues().getEmail())));
		assertEquals(attemptsBefore + 1, dlqRateMeter.attempts());
		assertEquals(deadBefore, dlqRateMeter.deadLetters(topology.getQueues().getEmail()));
	}

	@Test
	void housekeepingMessageIsAcknowledgedByTheRealListener() throws Exception {
		publisher.publish(housekeeping("event-hk-ok", "PREPARATION"));

		verify(housekeepingTicketSender, timeout(15_000).times(1)).send(any(HousekeepingTicket.class));
		awaitEmpty(topology.getQueues().getHousekeeping());
		assertEquals(0, messageCount(topology.deadLetterQueue(topology.getQueues().getHousekeeping())));
	}

	@Test
	void voucherMessageIsAcknowledgedByTheRealListener() throws Exception {
		publisher.publish(voucher("event-voucher-ok", "VOUCHER"));

		verify(voucherSender, timeout(15_000).times(1)).send(any(VoucherDocument.class));
		awaitEmpty(topology.getQueues().getVoucher());
		assertEquals(0, messageCount(topology.deadLetterQueue(topology.getQueues().getVoucher())));
	}

	/**
	 * H6 evidence: publish → consumer fails → NACK without requeue → message lands in DLQ with x-death.
	 */
	@Test
	void failedEmailIsNackedAndRoutedToItsDeadLetterQueue() throws Exception {
		long attemptsBefore = dlqRateMeter.attempts();
		long deadBefore = dlqRateMeter.deadLetters();
		String emailQueue = topology.getQueues().getEmail();
		String emailDlq = topology.deadLetterQueue(emailQueue);
		doThrow(new IllegalStateException("mail gateway down")).when(guestNotificationSender).send(any());

		publisher.publish(email("event-email-fail", "CONFIRMATION"));

		awaitCount(emailDlq, 1);
		assertEquals(0, messageCount(emailQueue), "main queue must be empty (NACK requeue=false)");
		assertEquals(1, messageCount(emailDlq), "rejected message must be in the email DLQ");
		assertEquals(attemptsBefore + 1, dlqRateMeter.attempts());
		assertEquals(deadBefore + 1, dlqRateMeter.deadLetters());
		assertTrue(dlqRateMeter.deadLetters(emailQueue) >= 1);
		assertTrue(dlqRateMeter.rate() > 0d);

		try (Channel channel = connectionFactory.createConnection().createChannel(false)) {
			GetResponse dead = channel.basicGet(emailDlq, true);
			assertNotNull(dead, "DLQ must contain the rejected envelope");
			assertNotNull(dead.getProps().getHeaders().get("x-death"), "broker must stamp x-death on dead-lettered messages");
			assertTrue(new String(dead.getBody(), StandardCharsets.UTF_8).contains("event-email-fail"));
		}
	}

	@Test
	void failedHousekeepingAndVoucherReachTheirDeadLetterQueues() throws Exception {
		doThrow(new IllegalStateException("housekeeping unavailable")).when(housekeepingTicketSender).send(any());
		doThrow(new IllegalStateException("pdf unavailable")).when(voucherSender).send(any());

		publisher.publish(housekeeping("event-hk-fail", "CLEANING"));
		publisher.publish(voucher("event-voucher-fail", "RECEIPT"));

		awaitCount(topology.deadLetterQueue(topology.getQueues().getHousekeeping()), 1);
		awaitCount(topology.deadLetterQueue(topology.getQueues().getVoucher()), 1);
		assertEquals(0, messageCount(topology.getQueues().getHousekeeping()));
		assertEquals(0, messageCount(topology.getQueues().getVoucher()));
	}

	@Test
	void sameEventIdIsDeliveredOnlyOnceByTheRealListener() throws Exception {
		publisher.publish(email("event-dup", "CONFIRMATION"));
		verify(guestNotificationSender, timeout(15_000).times(1)).send(any(GuestNotification.class));
		awaitEmpty(topology.getQueues().getEmail());

		publisher.publish(email("event-dup", "CONFIRMATION"));
		awaitEmpty(topology.getQueues().getEmail());

		verify(guestNotificationSender, times(1)).send(any(GuestNotification.class));
		assertEquals(0, messageCount(topology.deadLetterQueue(topology.getQueues().getEmail())));
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

		awaitEmpty(topology.getQueues().getEmail());
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
		awaitCount(topology.deadLetterQueue(topology.getQueues().getEmail()), 1);

		publisher.publish(email("event-retry", "CONFIRMATION"));
		verify(guestNotificationSender, timeout(15_000).times(2)).send(any(GuestNotification.class));
		awaitEmpty(topology.getQueues().getEmail());
	}

	@Test
	void dlqMetricsEndpointExposesAttemptsDeadLettersAndRate() throws Exception {
		long attemptsBefore = dlqRateMeter.attempts();
		long deadBefore = dlqRateMeter.deadLetters();
		doThrow(new IllegalStateException("mail gateway down")).when(guestNotificationSender).send(any());
		publisher.publish(email("event-metrics", "CONFIRMATION"));
		awaitCount(topology.deadLetterQueue(topology.getQueues().getEmail()), 1);

		mockMvc.perform(get("/api/notify/metrics/dlq"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.attempts").value((int) (attemptsBefore + 1)))
				.andExpect(jsonPath("$.deadLetters").value((int) (deadBefore + 1)))
				.andExpect(jsonPath("$.rate").isNumber())
				.andExpect(jsonPath("$.deadLettersByQueue['" + topology.getQueues().getEmail() + "']").exists());
	}

	private NotificationEnvelope email(String eventId, String kind) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("guestName", "Ana");
		payload.put("kind", kind);
		payload.put("channel", "EMAIL");
		payload.put("guestEmail", "ana@andesstay.local");
		return new NotificationEnvelope(
				topology.getRoutingKeys().getEmail(),
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
				topology.getRoutingKeys().getHousekeeping(),
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
				topology.getRoutingKeys().getVoucher(),
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
		rabbitAdmin.purgeQueue(topology.deadLetterQueue(queue), false);
	}

	private void awaitListenersReady() throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (System.nanoTime() < deadline) {
			try {
				messageCount(topology.getQueues().getEmail());
				messageCount(topology.getQueues().getHousekeeping());
				messageCount(topology.getQueues().getVoucher());
				Thread.sleep(200);
				return;
			} catch (AssertionError | RuntimeException ignored) {
				Thread.sleep(100);
			}
		}
	}
}
