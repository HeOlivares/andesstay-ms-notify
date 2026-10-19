package cl.duoc.andesstay.notify.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.GuestNotificationService;
import cl.duoc.andesstay.notify.delivery.VoucherService;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.rabbitmq.client.Channel;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class QueueConsumerTest {

	@Test
	void emailQueueProcessesThroughReliableDelivery() throws Exception {
		GuestNotificationService guests = mock(GuestNotificationService.class);
		ReliableDelivery delivery = mock(ReliableDelivery.class);
		Channel channel = mock(Channel.class);
		EmailQueueConsumer consumer = new EmailQueueConsumer(guests, delivery);
		NotificationEnvelope envelope = envelope("email.send");
		doAnswer(invocation -> {
			Runnable work = invocation.getArgument(4);
			work.run();
			return DeliveryResult.ACK;
		}).when(delivery).consume(eq(RabbitTopologyConfig.EMAIL_QUEUE), eq(envelope), eq(channel), eq(9L), any());

		consumer.onMessage(envelope, channel, 9L);

		verify(guests).deliver(envelope);
		verify(delivery).consume(eq(RabbitTopologyConfig.EMAIL_QUEUE), eq(envelope), eq(channel), eq(9L), any());
	}

	@Test
	void voucherQueueProcessesThroughReliableDelivery() throws Exception {
		VoucherService vouchers = mock(VoucherService.class);
		ReliableDelivery delivery = mock(ReliableDelivery.class);
		Channel channel = mock(Channel.class);
		VoucherQueueConsumer consumer = new VoucherQueueConsumer(vouchers, delivery);
		NotificationEnvelope envelope = envelope("voucher.gen");
		doAnswer(invocation -> {
			Runnable work = invocation.getArgument(4);
			work.run();
			return DeliveryResult.ACK;
		}).when(delivery).consume(eq(RabbitTopologyConfig.VOUCHER_QUEUE), eq(envelope), eq(channel), eq(4L), any());

		consumer.onMessage(envelope, channel, 4L);

		verify(vouchers).deliver(envelope);
		verify(delivery).consume(eq(RabbitTopologyConfig.VOUCHER_QUEUE), eq(envelope), eq(channel), eq(4L), any());
	}

	private static NotificationEnvelope envelope(String type) {
		return new NotificationEnvelope(type, "event-1", Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", JsonNodeFactory.instance.objectNode());
	}
}
