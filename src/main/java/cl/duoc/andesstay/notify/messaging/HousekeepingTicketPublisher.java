package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.delivery.HousekeepingTicketService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

@Service
public class HousekeepingTicketPublisher {

	static final String ROUTING_KEY = "housekeeping.ticket";

	private final NotificationPublisher publisher;
	private final ObjectMapper objectMapper;

	public HousekeepingTicketPublisher(NotificationPublisher publisher, ObjectMapper objectMapper) {
		this.publisher = publisher;
		this.objectMapper = objectMapper;
	}

	public void publish(HousekeepingTicketRequest request) {
		requireText(request.reservationId(), "reservationId");
		requireText(request.unitId(), "unitId");
		requireText(request.traceId(), "traceId");
		requireText(request.correlationId(), "correlationId");
		if (!HousekeepingTicketService.KIND_PREPARATION.equals(request.kind())
				&& !HousekeepingTicketService.KIND_CLEANING.equals(request.kind())) {
			throw new IllegalArgumentException("kind must be PREPARATION or CLEANING");
		}

		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", request.reservationId());
		payload.put("unitId", request.unitId());
		payload.put("kind", request.kind());
		publisher.publish(NotificationEnvelope.create(
				ROUTING_KEY,
				request.traceId(),
				request.correlationId(),
				payload));
	}

	private static void requireText(String value, String field) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
	}
}
