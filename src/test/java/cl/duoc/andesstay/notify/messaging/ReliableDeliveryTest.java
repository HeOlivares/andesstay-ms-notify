package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.rabbitmq.client.Channel;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReliableDeliveryTest {

	private DlqRateMeter meter;
	private ReliableDelivery delivery;
	private Channel channel;

	@BeforeEach
	void setUp() {
		meter = new DlqRateMeter();
		delivery = new ReliableDelivery(new ProcessedEventIds(), meter);
		channel = mock(Channel.class);
	}

	@Test
	void successfulProcessingIsAcknowledged() throws Exception {
		AtomicInteger runs = new AtomicInteger();
		DeliveryResult result = delivery.consume("q.cmd.email", envelope("event-1"), channel, 7L, runs::incrementAndGet);

		assertEquals(DeliveryResult.ACK, result);
		assertEquals(1, runs.get());
		verify(channel).basicAck(7L, false);
		verify(channel, never()).basicNack(7L, false, false);
	}

	@Test
	void failedProcessingIsNegativelyAcknowledgedIntoThatQueueDlq() throws Exception {
		DeliveryResult result = delivery.consume("q.cmd.housekeeping", envelope("event-2"), channel, 8L, () -> {
			throw new IllegalStateException("housekeeping unavailable");
		});

		assertEquals(DeliveryResult.NACK, result);
		verify(channel).basicNack(8L, false, false);
		verify(channel, never()).basicAck(8L, false);
		assertEquals(List.of("q.cmd.housekeeping.dlq"), delivery.deadLetterQueues());
		assertEquals(1, meter.deadLetters("q.cmd.housekeeping"));
		assertEquals(1d, meter.rate());
	}

	@Test
	void sameEventIdDoesNotRunTwice() throws Exception {
		AtomicInteger runs = new AtomicInteger();
		delivery.consume("q.cmd.voucher", envelope("event-3"), channel, 1L, runs::incrementAndGet);
		DeliveryResult duplicate = delivery.consume("q.cmd.voucher", envelope("event-3"), channel, 2L, runs::incrementAndGet);

		assertEquals(DeliveryResult.DUPLICATE, duplicate);
		assertEquals(1, runs.get());
		verify(channel).basicAck(2L, false);
		assertEquals(0d, meter.rate());
	}

	@Test
	void dlqRateCountsFailuresAgainstAttempts() throws Exception {
		delivery.consume("q.cmd.email", envelope("ok"), channel, 1L, () -> {
		});
		delivery.consume("q.cmd.email", envelope("bad"), channel, 2L, () -> {
			throw new IllegalStateException("send failed");
		});

		assertEquals(2, meter.attempts());
		assertEquals(1, meter.deadLetters());
		assertEquals(0.5d, meter.rate());
		assertTrue(delivery.deadLetterQueues().contains("q.cmd.email.dlq"));
	}

	private static NotificationEnvelope envelope(String eventId) {
		return new NotificationEnvelope("email.send", eventId, Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", null);
	}
}
