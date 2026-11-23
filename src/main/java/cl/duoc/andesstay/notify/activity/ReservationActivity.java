package cl.duoc.andesstay.notify.activity;

import java.time.Instant;
import java.util.Map;

public record ReservationActivity(
		String eventId,
		Instant occurredAt,
		String actor,
		String action,
		String resourceType,
		Long resourceId,
		Map<String, Object> detail) {
}
