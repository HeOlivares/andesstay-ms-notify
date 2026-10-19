package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

@Service
public class ConfirmedReservationPublisher {

	static final String CHANNEL_EMAIL = "EMAIL";
	static final String CHANNEL_PUSH = "PUSH";
	static final String KIND_CONFIRMATION = "CONFIRMATION";
	static final String DOCUMENT_VOUCHER = "VOUCHER";
	static final String DOCUMENT_RECEIPT = "RECEIPT";

	private final NotificationPublisher publisher;
	private final ObjectMapper objectMapper;
	private final RabbitTopologyProperties topology;

	public ConfirmedReservationPublisher(
			NotificationPublisher publisher,
			ObjectMapper objectMapper,
			RabbitTopologyProperties topology) {
		this.publisher = publisher;
		this.objectMapper = objectMapper;
		this.topology = topology;
	}

	public void publish(ConfirmedReservation reservation) {
		requireText(reservation.reservationId(), "reservationId");
		requireText(reservation.guestName(), "guestName");
		requireText(reservation.traceId(), "traceId");
		requireText(reservation.correlationId(), "correlationId");
		String channel = reservation.channel();
		if (!CHANNEL_EMAIL.equals(channel) && !CHANNEL_PUSH.equals(channel)) {
			throw new IllegalArgumentException("channel must be EMAIL or PUSH");
		}
		if (CHANNEL_EMAIL.equals(channel)) {
			requireText(reservation.guestEmail(), "guestEmail");
		} else {
			requireText(reservation.pushEndpoint(), "pushEndpoint");
		}

		ObjectNode email = objectMapper.createObjectNode();
		email.put("reservationId", reservation.reservationId());
		email.put("guestName", reservation.guestName());
		email.put("kind", KIND_CONFIRMATION);
		email.put("channel", channel);
		putIfPresent(email, "guestEmail", reservation.guestEmail());
		putIfPresent(email, "pushEndpoint", reservation.pushEndpoint());
		publisher.publish(NotificationEnvelope.create(
				topology.getRoutingKeys().getEmail(),
				reservation.traceId(),
				reservation.correlationId(),
				email));

		if (reservation.voucher()) {
			publishDocument(reservation, DOCUMENT_VOUCHER);
		}
		if (reservation.receipt()) {
			publishDocument(reservation, DOCUMENT_RECEIPT);
		}
	}

	private void publishDocument(ConfirmedReservation reservation, String document) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", reservation.reservationId());
		payload.put("guestName", reservation.guestName());
		payload.put("document", document);
		putIfPresent(payload, "guestEmail", reservation.guestEmail());
		publisher.publish(NotificationEnvelope.create(
				topology.getRoutingKeys().getVoucher(),
				reservation.traceId(),
				reservation.correlationId(),
				payload));
	}

	private static void putIfPresent(ObjectNode node, String field, String value) {
		if (value != null && !value.isBlank()) {
			node.put(field, value);
		}
	}

	private static void requireText(String value, String field) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
	}
}
