package cl.duoc.andesstay.notify.delivery;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

@Service
public class VoucherService {

	static final String DOCUMENT_VOUCHER = "VOUCHER";
	static final String DOCUMENT_RECEIPT = "RECEIPT";
	static final String DOCUMENT_BOLETA = "BOLETA";

	private final VoucherSender sender;

	public VoucherService(VoucherSender sender) {
		this.sender = sender;
	}

	public void deliver(NotificationEnvelope envelope) {
		if (envelope.type() == null || !envelope.type().startsWith("voucher.")) {
			throw new IllegalArgumentException("Unsupported voucher type: " + envelope.type());
		}
		JsonNode payload = envelope.payload();
		if (payload == null || payload.isNull()) {
			throw new IllegalArgumentException("payload is required");
		}
		String document = required(payload, "document");
		String title = switch (document) {
			case DOCUMENT_VOUCHER -> DOCUMENT_VOUCHER;
			case DOCUMENT_RECEIPT, DOCUMENT_BOLETA -> DOCUMENT_BOLETA;
			default -> throw new IllegalArgumentException("Unsupported voucher document: " + document);
		};
		String reservationId = required(payload, "reservationId");
		String guestName = required(payload, "guestName");
		byte[] pdf = VoucherPdf.render(title, reservationId, guestName);
		sender.send(new VoucherDocument(reservationId, guestName, title, pdf, envelope.eventId()));
	}

	private static String required(JsonNode payload, String field) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull() || node.asText().isBlank()) {
			throw new IllegalArgumentException(field + " is required");
		}
		return node.asText();
	}
}
