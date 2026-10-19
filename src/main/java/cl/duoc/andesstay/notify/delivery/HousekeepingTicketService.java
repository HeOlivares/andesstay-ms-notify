package cl.duoc.andesstay.notify.delivery;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class HousekeepingTicketService {

	public static final String KIND_PREPARATION = "PREPARATION";
	public static final String KIND_CLEANING = "CLEANING";
	static final Set<String> KINDS = Set.of(KIND_PREPARATION, KIND_CLEANING);

	private final HousekeepingTicketSender sender;

	public HousekeepingTicketService(HousekeepingTicketSender sender) {
		this.sender = sender;
	}

	public void deliver(NotificationEnvelope envelope) {
		if (envelope.type() == null || !envelope.type().startsWith("housekeeping.")) {
			throw new IllegalArgumentException("Unsupported housekeeping type: " + envelope.type());
		}
		JsonNode payload = envelope.payload();
		if (payload == null || payload.isNull()) {
			throw new IllegalArgumentException("payload is required");
		}
		String kind = required(payload, "kind");
		if (!KINDS.contains(kind)) {
			throw new IllegalArgumentException("Unsupported housekeeping kind: " + kind);
		}
		sender.send(new HousekeepingTicket(
				required(payload, "reservationId"),
				required(payload, "unitId"),
				kind,
				envelope.eventId()));
	}

	private static String required(JsonNode payload, String field) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull() || node.asText().isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
		return node.asText();
	}
}
