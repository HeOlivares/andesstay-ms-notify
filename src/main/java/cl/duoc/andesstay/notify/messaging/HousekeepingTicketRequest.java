package cl.duoc.andesstay.notify.messaging;

public record HousekeepingTicketRequest(
		String reservationId,
		String unitId,
		String kind,
		String traceId,
		String correlationId) {
}
