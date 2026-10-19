package cl.duoc.andesstay.notify.delivery;

public record HousekeepingTicket(
		String reservationId,
		String unitId,
		String kind,
		String eventId) {
}
