package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ConfirmedReservationPublisherTest {

	private final RabbitTopologyProperties topology = RabbitTopologyProperties.defaults();
	private final NotificationPublisher publisher = mock(NotificationPublisher.class);
	private final ConfirmedReservationPublisher events =
			new ConfirmedReservationPublisher(publisher, new ObjectMapper(), topology);

	@Test
	void confirmedReservationEnqueuesEmailAndVoucherWithoutDeliveringThem() {
		events.publish(notice(true, false, "EMAIL", "ana@andesstay.local", null));

		List<NotificationEnvelope> envelopes = published(2);
		assertEquals(topology.getRoutingKeys().getEmail(), envelopes.get(0).type());
		assertEquals("CONFIRMATION", envelopes.get(0).payload().get("kind").asText());
		assertEquals("EMAIL", envelopes.get(0).payload().get("channel").asText());
		assertEquals(topology.getRoutingKeys().getVoucher(), envelopes.get(1).type());
		assertEquals("VOUCHER", envelopes.get(1).payload().get("document").asText());
		assertEquals(envelopes.get(0).traceId(), envelopes.get(1).traceId());
	}

	@Test
	void pushConfirmationWithoutVoucherEnqueuesOnlyTheEmailQueue() {
		events.publish(notice(false, false, "PUSH", null, "https://push.example/ana"));

		List<NotificationEnvelope> envelopes = published(1);
		assertEquals("PUSH", envelopes.get(0).payload().get("channel").asText());
		assertEquals("https://push.example/ana", envelopes.get(0).payload().get("pushEndpoint").asText());
	}

	@Test
	void receiptIsEnqueuedOnTheVoucherQueue() {
		events.publish(notice(false, true, "EMAIL", "ana@andesstay.local", null));

		List<NotificationEnvelope> envelopes = published(2);
		assertEquals("RECEIPT", envelopes.get(1).payload().get("document").asText());
		assertEquals(topology.getRoutingKeys().getVoucher(), envelopes.get(1).type());
	}

	private List<NotificationEnvelope> published(int times) {
		ArgumentCaptor<NotificationEnvelope> captor = ArgumentCaptor.forClass(NotificationEnvelope.class);
		verify(publisher, times(times)).publish(captor.capture());
		return captor.getAllValues();
	}

	private static ConfirmedReservation notice(boolean voucher, boolean receipt, String channel, String email, String push) {
		return new ConfirmedReservation("10", "Ana", email, channel, push, voucher, receipt, "trace-1", "corr-1");
	}
}
