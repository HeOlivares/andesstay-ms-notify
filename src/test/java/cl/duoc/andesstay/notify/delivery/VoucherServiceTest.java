package cl.duoc.andesstay.notify.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class VoucherServiceTest {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final List<VoucherDocument> sent = new ArrayList<>();
	private final VoucherService service = new VoucherService(sent::add);

	@Test
	void voucherPdfIsDeliveredFromTheVoucherQueueEvent() {
		service.deliver(envelope("VOUCHER"));

		VoucherDocument document = sent.get(0);
		assertEquals("VOUCHER", document.documentType());
		assertEquals("10", document.reservationId());
		String pdf = new String(document.pdf(), StandardCharsets.US_ASCII);
		assertTrue(pdf.startsWith("%PDF-1.4"));
		assertTrue(pdf.contains("VOUCHER 10 Ana"));
	}

	@Test
	void receiptAndBoletaAreDeliveredAsBoleta() {
		service.deliver(envelope("RECEIPT"));
		service.deliver(envelope("BOLETA"));

		assertEquals("BOLETA", sent.get(0).documentType());
		assertEquals("BOLETA", sent.get(1).documentType());
		assertTrue(new String(sent.get(1).pdf(), StandardCharsets.US_ASCII).contains("BOLETA 10 Ana"));
	}

	private NotificationEnvelope envelope(String document) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("guestName", "Ana");
		payload.put("document", document);
		return new NotificationEnvelope("voucher.gen", "event-2", Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", payload);
	}
}
