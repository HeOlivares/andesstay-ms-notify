package cl.duoc.andesstay.notify.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class GuestNotificationServiceTest {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final List<GuestNotification> sent = new ArrayList<>();
	private final GuestNotificationService service = new GuestNotificationService(sent::add);

	@Test
	void confirmationEmailIsDeliveredFromTheEmailEvent() {
		service.deliver(envelope("email.send", payload("CONFIRMATION", "EMAIL", "ana@andesstay.local", null)));

		GuestNotification notification = sent.get(0);
		assertEquals("EMAIL", notification.channel());
		assertEquals("CONFIRMATION", notification.kind());
		assertEquals("ana@andesstay.local", notification.destination());
		assertEquals("10", notification.reservationId());
	}

	@Test
	void checkInReminderUsesPushWhenThatChannelIsRequested() {
		service.deliver(envelope("email.checkin", payload("CHECKIN_REMINDER", "PUSH", null, "https://push.example/ana")));

		assertEquals("PUSH", sent.get(0).channel());
		assertEquals("CHECKIN_REMINDER", sent.get(0).kind());
		assertEquals("https://push.example/ana", sent.get(0).destination());
	}

	@Test
	void checkoutEmailIsDelivered() {
		service.deliver(envelope("email.checkout", payload("CHECKOUT", "EMAIL", "ana@andesstay.local", null)));

		assertEquals("CHECKOUT", sent.get(0).kind());
		assertEquals("EMAIL", sent.get(0).channel());
	}

	@Test
	void pushWithoutEndpointIsRejected() {
		assertThrows(IllegalArgumentException.class, () -> service.deliver(
				envelope("email.send", payload("CONFIRMATION", "PUSH", "ana@andesstay.local", null))));
		assertEquals(0, sent.size());
	}

	private NotificationEnvelope envelope(String type, ObjectNode payload) {
		return new NotificationEnvelope(type, "event-1", Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", payload);
	}

	private ObjectNode payload(String kind, String channel, String email, String pushEndpoint) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("guestName", "Ana");
		payload.put("kind", kind);
		payload.put("channel", channel);
		if (email != null) {
			payload.put("guestEmail", email);
		}
		if (pushEndpoint != null) {
			payload.put("pushEndpoint", pushEndpoint);
		}
		return payload;
	}
}
