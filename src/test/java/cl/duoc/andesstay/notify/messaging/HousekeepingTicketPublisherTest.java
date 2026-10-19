package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HousekeepingTicketPublisherTest {

	private final RabbitTopologyProperties topology = RabbitTopologyProperties.defaults();
	private final NotificationPublisher publisher = mock(NotificationPublisher.class);
	private final HousekeepingTicketPublisher tickets =
			new HousekeepingTicketPublisher(publisher, new ObjectMapper(), topology);

	@Test
	void preparationTicketIsOnlyEnqueued() {
		tickets.publish(new HousekeepingTicketRequest("10", "U-4", "PREPARATION", "trace-1", "corr-1"));

		NotificationEnvelope envelope = published();
		assertEquals(topology.getRoutingKeys().getHousekeeping(), envelope.type());
		assertEquals("PREPARATION", envelope.payload().get("kind").asText());
		assertEquals("U-4", envelope.payload().get("unitId").asText());
		assertEquals("trace-1", envelope.traceId());
		assertEquals("corr-1", envelope.correlationId());
	}

	private NotificationEnvelope published() {
		ArgumentCaptor<NotificationEnvelope> captor = ArgumentCaptor.forClass(NotificationEnvelope.class);
		verify(publisher, times(1)).publish(captor.capture());
		return captor.getValue();
	}
}
