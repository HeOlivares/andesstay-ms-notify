package cl.duoc.andesstay.notify.messaging;

public record ConfirmedReservation(
		String reservationId,
		String guestName,
		String guestEmail,
		String channel,
		String pushEndpoint,
		boolean voucher,
		boolean receipt,
		String traceId,
		String correlationId) {
}
