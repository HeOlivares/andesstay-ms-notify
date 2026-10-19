package cl.duoc.andesstay.notify.delivery;

public record GuestNotification(
		String reservationId,
		String guestName,
		String kind,
		String channel,
		String destination,
		String eventId) {
}
