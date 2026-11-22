package cl.duoc.andesstay.notify.delivery;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CabinReadyService {

	public static final String KIND_CABIN_READY = "CABIN_READY";

	private static final Logger log = LoggerFactory.getLogger(CabinReadyService.class);

	public void deliver(NotificationEnvelope envelope) {
		if (envelope.type() == null || !envelope.type().equals("cabin.ready")) {
			throw new IllegalArgumentException("Unsupported cabin ready type: " + envelope.type());
		}
		JsonNode payload = envelope.payload();
		if (payload == null || payload.isNull()) {
			throw new IllegalArgumentException("payload is required");
		}
		String kind = required(payload, "kind");
		if (!KIND_CABIN_READY.equals(kind)) {
			throw new IllegalArgumentException("Unsupported cabin ready kind: " + kind);
		}
		String reservationId = required(payload, "reservationId");
		log.info("Cabin ready notified reservationId={} eventId={}", reservationId, envelope.eventId());
	}

	private static String required(JsonNode payload, String field) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull() || node.asText().isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
		return node.asText();
	}
}
