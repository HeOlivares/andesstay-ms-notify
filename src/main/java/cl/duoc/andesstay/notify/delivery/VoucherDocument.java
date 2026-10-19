package cl.duoc.andesstay.notify.delivery;

public record VoucherDocument(
		String reservationId,
		String guestName,
		String documentType,
		byte[] pdf,
		String eventId) {
}
