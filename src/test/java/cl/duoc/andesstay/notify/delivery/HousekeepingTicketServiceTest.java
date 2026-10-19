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

class HousekeepingTicketServiceTest {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final List<HousekeepingTicket> sent = new ArrayList<>();
	private final HousekeepingTicketService service = new HousekeepingTicketService(sent::add);

	@Test
	void preparationTicketIsDeliveredFromTheHousekeepingEvent() {
		service.deliver(envelope("housekeeping.ticket", "PREPARATION"));

		HousekeepingTicket ticket = sent.get(0);
		assertEquals("10", ticket.reservationId());
		assertEquals("U-4", ticket.unitId());
		assertEquals("PREPARATION", ticket.kind());
		assertEquals("event-1", ticket.eventId());
	}

	@Test
	void cleaningTicketIsDelivered() {
		service.deliver(envelope("housekeeping.cleaning", "CLEANING"));

		assertEquals("CLEANING", sent.get(0).kind());
	}

	@Test
	void unknownKindIsNotDelivered() {
		assertThrows(IllegalArgumentException.class, () -> service.deliver(envelope("housekeeping.ticket", "OTHER")));
		assertEquals(0, sent.size());
	}

	private NotificationEnvelope envelope(String type, String kind) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("unitId", "U-4");
		payload.put("kind", kind);
		return new NotificationEnvelope(type, "event-1", Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", payload);
	}
}
