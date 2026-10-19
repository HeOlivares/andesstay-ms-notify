package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import com.rabbitmq.client.Channel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

class AsyncNotificationFlowTest {

	private RabbitTemplate rabbitTemplate;
	private NotificationPublisher publisher;
	private ReliableDelivery delivery;
	private Channel channel;
	private final List<String> delivered = new ArrayList<>();
	private long nextTag = 1L;

	@BeforeEach
	void setUp() {
		rabbitTemplate = mock(RabbitTemplate.class);
		publisher = new NotificationPublisher(rabbitTemplate);
		delivery = new ReliableDelivery(new ProcessedEventIds(ProcessedEventIds.DEFAULT_TTL), new DlqRateMeter());
		channel = mock(Channel.class);
	}

	@Test
	void confirmedReservationPublishesBeforeAnyQueueIsConsumed() throws Exception {
		publishConfirmedReservation(false);

		assertEquals(List.of(), delivered);
		List<NotificationEnvelope> queued = published();
		assertEquals(List.of("email.send", "housekeeping.ticket"), types(queued));

		consume("q.cmd.email", queued.get(0));
		consume("q.cmd.housekeeping", queued.get(1));

		assertEquals(List.of("email.send", "housekeeping.ticket"), delivered);
		verify(channel).basicAck(1L, false);
		verify(channel).basicAck(2L, false);
	}

	@Test
	void voucherQueueIsConsumedWhenTheReservationIncludesIt() throws Exception {
		publishConfirmedReservation(true);

		List<NotificationEnvelope> queued = published();
		assertEquals(List.of("email.send", "housekeeping.ticket", "voucher.gen"), types(queued));
		for (int index = 0; index < queued.size(); index++) {
			consume(queueFor(queued.get(index).type()), queued.get(index));
		}

		assertEquals(List.of("email.send", "housekeeping.ticket", "voucher.gen"), delivered);
		verify(channel).basicAck(3L, false);
	}

	@Test
	void failedConsumptionNacksAndKeepsTheMessageOnThatQueueDlq() throws Exception {
		publisher.publish(envelope("email.send", "event-email"));
		NotificationEnvelope queued = published().get(0);

		DeliveryResult result = delivery.consume("q.cmd.email", queued, channel, 4L, () -> {
			throw new IllegalStateException("mail gateway down");
		});

		assertEquals(DeliveryResult.NACK, result);
		assertEquals(List.of(), delivered);
		verify(channel).basicNack(4L, false, false);
		assertEquals(List.of(ReliableDelivery.deadLetterQueue("q.cmd.email")), delivery.deadLetterQueues());
	}

	private void publishConfirmedReservation(boolean voucher) {
		publisher.publish(envelope("email.send", "event-email"));
		publisher.publish(envelope("housekeeping.ticket", "event-housekeeping"));
		if (voucher) {
			publisher.publish(envelope("voucher.gen", "event-voucher"));
		}
	}

	private void consume(String queue, NotificationEnvelope envelope) throws Exception {
		long tag = nextTag++;
		DeliveryResult result = delivery.consume(queue, envelope, channel, tag, () -> delivered.add(envelope.type()));
		assertEquals(DeliveryResult.ACK, result);
	}

	private List<NotificationEnvelope> published() {
		ArgumentCaptor<NotificationEnvelope> captor = ArgumentCaptor.forClass(NotificationEnvelope.class);
		verify(rabbitTemplate, atLeastOnce()).convertAndSend(eq(RabbitTopologyConfig.DIRECT_EXCHANGE), anyString(), captor.capture());
		return captor.getAllValues();
	}

	private static List<String> types(List<NotificationEnvelope> envelopes) {
		return envelopes.stream().map(NotificationEnvelope::type).toList();
	}

	private static String queueFor(String type) {
		if (type.startsWith("email.")) {
			return "q.cmd.email";
		}
		if (type.startsWith("housekeeping.")) {
			return "q.cmd.housekeeping";
		}
		if (type.startsWith("voucher.")) {
			return "q.cmd.voucher";
		}
		throw new IllegalArgumentException(type);
	}

	private static NotificationEnvelope envelope(String type, String eventId) {
		return new NotificationEnvelope(type, eventId, Instant.parse("2026-10-19T03:40:00Z"), "trace-1", "corr-1", null);
	}
}
