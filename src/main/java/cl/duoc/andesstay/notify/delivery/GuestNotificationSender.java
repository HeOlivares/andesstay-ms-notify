package cl.duoc.andesstay.notify.delivery;

/**
 * Entrega al canal del huésped. Ante fallos transitorios (timeout, 5xx, etc.)
 * lanzar {@link cl.duoc.andesstay.notify.messaging.RecoverableDeliveryException}
 * para que {@code ReliableDelivery} reintente; otras {@link RuntimeException} van a DLQ sin reintento.
 */
public interface GuestNotificationSender {

	void send(GuestNotification notification);
}
