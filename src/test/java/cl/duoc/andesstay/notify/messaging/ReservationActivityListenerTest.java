package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cl.duoc.andesstay.notify.activity.ReservationActivity;
import cl.duoc.andesstay.notify.activity.ReservationActivityRecorder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;

class ReservationActivityListenerTest {

	private static final String PRODUCER_JSON = """
			{"eventId":"evt-1","occurredAt":"2026-10-26T12:00:00Z","actor":"cliente@x.cl","action":"RESERVATION_CREATED","resourceType":"RESERVATION","resourceId":7,"detail":{"newStatus":"PENDING","previousStatus":null,"unitName":"Cabana 1","guests":2}}
			""";

	@Test
	void listenerDelegatesTheDeserializedProducerEvent() {
		ReservationActivityRecorder recorder = mock(ReservationActivityRecorder.class);
		ReservationActivityListener listener = new ReservationActivityListener(recorder);
		ReservationActivity activity = deserialize(PRODUCER_JSON);

		listener.onActivity(activity);

		verify(recorder).record(activity);
		assertEquals("evt-1", activity.eventId());
		assertEquals(Instant.parse("2026-10-26T12:00:00Z"), activity.occurredAt());
		assertEquals("cliente@x.cl", activity.actor());
		assertEquals("RESERVATION_CREATED", activity.action());
		assertEquals("RESERVATION", activity.resourceType());
		assertEquals(7L, activity.resourceId());
		assertEquals("Cabana 1", activity.detail().get("unitName"));
		assertNull(activity.detail().get("previousStatus"));
	}

	private static ReservationActivity deserialize(String json) {
		JsonDeserializer<ReservationActivity> deserializer = new JsonDeserializer<>();
		deserializer.configure(Map.of(
				JsonDeserializer.TRUSTED_PACKAGES, "cl.duoc.andesstay.notify.activity",
				JsonDeserializer.USE_TYPE_INFO_HEADERS, false,
				JsonDeserializer.VALUE_DEFAULT_TYPE, ReservationActivity.class.getName()), false);
		return deserializer.deserialize("reservations.activity", json.getBytes(StandardCharsets.UTF_8));
	}
}
