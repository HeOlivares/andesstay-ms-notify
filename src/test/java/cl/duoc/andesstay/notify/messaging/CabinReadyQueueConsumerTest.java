package cl.duoc.andesstay.notify.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import cl.duoc.andesstay.notify.delivery.CabinReadyService;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.rabbitmq.client.Channel;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CabinReadyQueueConsumerTest {

	@Test
	void cabinReadyQueueProcessesThroughReliableDelivery() throws Exception {
		RabbitTopologyProperties topology = RabbitTopologyProperties.defaults();
		CabinReadyService cabinReady = mock(CabinReadyService.class);
		ReliableDelivery delivery = mock(ReliableDelivery.class);
		Channel channel = mock(Channel.class);
		CabinReadyQueueConsumer consumer = new CabinReadyQueueConsumer(cabinReady, delivery, topology);
		NotificationEnvelope envelope = new NotificationEnvelope(
				topology.getRoutingKeys().getCabinReady(),
				"event-1",
				Instant.parse("2026-10-19T03:00:00Z"),
				"trace-1",
				"corr-1",
				JsonNodeFactory.instance.objectNode());
		doAnswer(invocation -> {
			Runnable work = invocation.getArgument(4);
			work.run();
			return DeliveryResult.ACK;
		}).when(delivery).consume(eq(topology.getQueues().getCabinReady()), eq(envelope), eq(channel), eq(5L), any());

		consumer.onMessage(envelope, channel, 5L);

		verify(cabinReady).deliver(envelope);
		verify(delivery).consume(eq(topology.getQueues().getCabinReady()), eq(envelope), eq(channel), eq(5L), any());
	}
}
