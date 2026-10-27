package cl.duoc.andesstay.notify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Reintentos in-process ante {@link cl.duoc.andesstay.notify.messaging.RecoverableDeliveryException}.
 * Origen: {@code andesstay.notify.retry.*} en {@code application.yml}.
 */
@ConfigurationProperties(prefix = "andesstay.notify.retry")
public class NotifyRetryProperties {

	private int maxAttempts = 3;
	private long backoffMs = 200;

	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	public long getBackoffMs() {
		return backoffMs;
	}

	public void setBackoffMs(long backoffMs) {
		this.backoffMs = backoffMs;
	}

	public static NotifyRetryProperties defaults() {
		NotifyRetryProperties retry = new NotifyRetryProperties();
		retry.setMaxAttempts(3);
		retry.setBackoffMs(0);
		return retry;
	}
}
