package cl.duoc.andesstay.notify.messaging;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.delivery.GuestNotificationService;
import cl.duoc.andesstay.notify.delivery.VoucherService;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class QueueConsumerTest {

	@Test
	void emailQueueDelegatesDelivery() {
		GuestNotificationService guests = mock(GuestNotificationService.class);
		EmailQueueConsumer consumer = new EmailQueueConsumer(guests);
		NotificationEnvelope envelope = envelope("email.send");

		consumer.onMessage(envelope);

		verify(guests).deliver(envelope);
	}

	@Test
	void voucherQueueDelegatesDelivery() {
		VoucherService vouchers = mock(VoucherService.class);
		VoucherQueueConsumer consumer = new VoucherQueueConsumer(vouchers);
		NotificationEnvelope envelope = envelope("voucher.gen");

		consumer.onMessage(envelope);

		verify(vouchers).deliver(envelope);
	}

	private static NotificationEnvelope envelope(String type) {
		return new NotificationEnvelope(type, "event-1", Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", JsonNodeFactory.instance.objectNode());
	}
}
