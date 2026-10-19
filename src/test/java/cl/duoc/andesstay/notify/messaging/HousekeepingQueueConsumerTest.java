package cl.duoc.andesstay.notify.messaging;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.delivery.HousekeepingTicketService;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class HousekeepingQueueConsumerTest {

	@Test
	void housekeepingQueueDelegatesDelivery() {
		HousekeepingTicketService tickets = mock(HousekeepingTicketService.class);
		HousekeepingQueueConsumer consumer = new HousekeepingQueueConsumer(tickets);
		NotificationEnvelope envelope = new NotificationEnvelope(
				"housekeeping.ticket",
				"event-1",
				Instant.parse("2026-10-19T03:00:00Z"),
				"trace-1",
				"corr-1",
				JsonNodeFactory.instance.objectNode());

		consumer.onMessage(envelope);

		verify(tickets).deliver(envelope);
	}
}
