package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import com.rabbitmq.client.Channel;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReliableDeliveryTest {

	private DlqRateMeter meter;
	private ReliableDelivery delivery;
	private Channel channel;
	private RabbitTopologyProperties topology;

	@BeforeEach
	void setUp() {
		meter = new DlqRateMeter();
		topology = RabbitTopologyProperties.defaults();
		delivery = new ReliableDelivery(new ProcessedEventIds(ProcessedEventIds.DEFAULT_TTL), meter, topology);
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

	@Test
	void aFailedEventCanBeRetriedBecauseTheReservationIsReleased() throws Exception {
		AtomicInteger runs = new AtomicInteger();
		delivery.consume("q.cmd.email", envelope("event-retry"), channel, 3L, () -> {
			runs.incrementAndGet();
			throw new IllegalStateException("temporary");
		});
		DeliveryResult retry = delivery.consume("q.cmd.email", envelope("event-retry"), channel, 4L, runs::incrementAndGet);

		assertEquals(DeliveryResult.ACK, retry);
		assertEquals(2, runs.get());
	}

	@Test
	void anEventAlreadyInProgressDoesNotRunInParallel() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicInteger runs = new AtomicInteger();
		Thread first = new Thread(() -> {
			try {
				delivery.consume("q.cmd.email", envelope("event-race"), channel, 11L, () -> {
					runs.incrementAndGet();
					entered.countDown();
					await(release);
				});
			} catch (Exception failure) {
				throw new IllegalStateException(failure);
			}
		});
		first.start();
		assertTrue(entered.await(2, TimeUnit.SECONDS));

		DeliveryResult duplicate = delivery.consume("q.cmd.email", envelope("event-race"), channel, 12L, runs::incrementAndGet);
		assertEquals(DeliveryResult.DUPLICATE, duplicate);
		assertEquals(1, runs.get());

		release.countDown();
		first.join(2000);
		assertFalse(first.isAlive());
	}

	@Test
	void expiredEventIdsAreDropped() {
		MutableClock clock = new MutableClock(Instant.parse("2026-10-19T03:00:00Z"));
		ProcessedEventIds ids = new ProcessedEventIds(Duration.ofMinutes(30), clock);
		assertTrue(ids.tryReserve("event-1"));
		ids.complete("event-1");
		assertFalse(ids.tryReserve("event-1"));

		clock.plus(Duration.ofMinutes(31));

		assertTrue(ids.tryReserve("event-1"));
		assertEquals(1, ids.size());
	}

	private static void await(CountDownLatch latch) {
		try {
			if (!latch.await(2, TimeUnit.SECONDS)) {
				throw new IllegalStateException("timed out");
			}
		} catch (InterruptedException failure) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(failure);
		}
	}

	private static NotificationEnvelope envelope(String eventId) {
		return new NotificationEnvelope("email.send", eventId, Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", null);
	}

	private static final class MutableClock extends Clock {

		private Instant instant;

		private MutableClock(Instant instant) {
			this.instant = instant;
		}

		private void plus(Duration duration) {
			instant = instant.plus(duration);
		}

		@Override
		public ZoneId getZone() {
			return ZoneId.of("UTC");
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

		@Override
		public Instant instant() {
			return instant;
		}
	}
}
