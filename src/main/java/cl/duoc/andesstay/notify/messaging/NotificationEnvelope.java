package cl.duoc.andesstay.notify.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record NotificationEnvelope(
		String type,
		String eventId,
		Instant timestamp,
		String traceId,
		String correlationId,
		JsonNode payload) {

	public NotificationEnvelope {
		requireText(type, "type");
		requireText(eventId, "eventId");
		if (timestamp == null) {
			throw new IllegalArgumentException("timestamp is required");
		}
		requireText(traceId, "traceId");
		requireText(correlationId, "correlationId");
	}

	public static NotificationEnvelope create(String type, String traceId, String correlationId, JsonNode payload) {
		return new NotificationEnvelope(type, UUID.randomUUID().toString(), Instant.now(), traceId, correlationId, payload);
	}

	private static void requireText(String value, String field) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
	}
}
