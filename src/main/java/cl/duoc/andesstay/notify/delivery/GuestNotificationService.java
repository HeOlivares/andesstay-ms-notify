package cl.duoc.andesstay.notify.delivery;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class GuestNotificationService {

	static final String CHANNEL_EMAIL = "EMAIL";
	static final String CHANNEL_PUSH = "PUSH";
	static final Set<String> KINDS = Set.of("CONFIRMATION", "CHECKIN_REMINDER", "CHECKOUT");

	private final GuestNotificationSender sender;

	public GuestNotificationService(GuestNotificationSender sender) {
		this.sender = sender;
	}

	public void deliver(NotificationEnvelope envelope) {
		if (envelope.type() == null || !envelope.type().startsWith("email.")) {
			throw new IllegalArgumentException("Unsupported guest notification type: " + envelope.type());
		}
		JsonNode payload = requiredPayload(envelope);
		String kind = required(payload, "kind");
		if (!KINDS.contains(kind)) {
			throw new IllegalArgumentException("Unsupported guest notification kind: " + kind);
		}
		String channel = text(payload, "channel");
		if (channel == null || channel.isBlank()) {
			channel = CHANNEL_EMAIL;
		}
		String reservationId = required(payload, "reservationId");
		String guestName = required(payload, "guestName");
		String destination = switch (channel) {
			case CHANNEL_EMAIL -> required(payload, "guestEmail");
			case CHANNEL_PUSH -> required(payload, "pushEndpoint");
			default -> throw new IllegalArgumentException("Unsupported channel: " + channel);
		};
		sender.send(new GuestNotification(
				reservationId,
				guestName,
				kind,
				channel,
				destination,
				envelope.eventId()));
	}

	private static JsonNode requiredPayload(NotificationEnvelope envelope) {
		JsonNode payload = envelope.payload();
		if (payload == null || payload.isNull()) {
			throw new IllegalArgumentException("payload is required");
		}
		return payload;
	}

	private static String required(JsonNode payload, String field) {
		String value = text(payload, field);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
		return value;
	}

	private static String text(JsonNode payload, String field) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull()) {
			return null;
		}
		return node.asText();
	}
}
