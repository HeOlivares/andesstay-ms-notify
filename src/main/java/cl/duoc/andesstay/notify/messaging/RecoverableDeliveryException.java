package cl.duoc.andesstay.notify.messaging;

/**
 * Fallo transitorio del envío (timeout, gateway temporal, etc.).
 * Los senders de entrega deben lanzarla ante errores recuperables;
 * cualquier otra {@link RuntimeException} se trata como no recuperable (NACK inmediato → DLQ).
 */
public class RecoverableDeliveryException extends RuntimeException {

	public RecoverableDeliveryException(String message) {
		super(message);
	}

	public RecoverableDeliveryException(String message, Throwable cause) {
		super(message, cause);
	}
}
