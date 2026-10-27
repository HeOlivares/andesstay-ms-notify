package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.NotifyRetryProperties;
import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ReliableDelivery {

	private static final Logger log = LoggerFactory.getLogger(ReliableDelivery.class);

	private final ProcessedEventIds processedEventIds;
	private final DlqRateMeter dlqRateMeter;
	private final RabbitTopologyProperties topology;
	private final NotifyRetryProperties retry;
	private final List<String> deadLetterQueues = new CopyOnWriteArrayList<>();

	public ReliableDelivery(
			ProcessedEventIds processedEventIds,
			DlqRateMeter dlqRateMeter,
			RabbitTopologyProperties topology,
			NotifyRetryProperties retry) {
		this.processedEventIds = processedEventIds;
		this.dlqRateMeter = dlqRateMeter;
		this.topology = topology;
		this.retry = retry;
	}

	public String deadLetterQueue(String queue) {
		return topology.deadLetterQueue(queue);
	}

	public DeliveryResult consume(String queue, NotificationEnvelope envelope, Channel channel, long deliveryTag, Runnable work)
			throws IOException {
		dlqRateMeter.attempt();
		if (!processedEventIds.tryReserve(envelope.eventId())) {
			channel.basicAck(deliveryTag, false);
			return DeliveryResult.DUPLICATE;
		}
		int maxAttempts = Math.max(1, retry.getMaxAttempts());
		RuntimeException lastFailure = null;
		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			try {
				work.run();
				processedEventIds.complete(envelope.eventId());
				channel.basicAck(deliveryTag, false);
				return DeliveryResult.ACK;
			} catch (RecoverableDeliveryException recoverable) {
				lastFailure = recoverable;
				log.warn(
						"RETRY attempt={}/{} eventId={} queue={} reason={}",
						attempt,
						maxAttempts,
						envelope.eventId(),
						queue,
						recoverable.toString());
				if (attempt < maxAttempts) {
					sleepBackoff();
					continue;
				}
				return nackToDlq(queue, envelope, channel, deliveryTag, recoverable, "exhausted");
			} catch (RuntimeException nonRecoverable) {
				return nackToDlq(queue, envelope, channel, deliveryTag, nonRecoverable, "non-recoverable");
			}
		}
		return nackToDlq(queue, envelope, channel, deliveryTag, lastFailure, "exhausted");
	}

	private DeliveryResult nackToDlq(
			String queue,
			NotificationEnvelope envelope,
			Channel channel,
			long deliveryTag,
			RuntimeException failure,
			String reasonKind)
			throws IOException {
		processedEventIds.release(envelope.eventId());
		String dlq = deadLetterQueue(queue);
		channel.basicNack(deliveryTag, false, false);
		dlqRateMeter.deadLetter(queue);
		deadLetterQueues.add(dlq);
		log.warn(
				"NACK without requeue: eventId={} queue={} dlq={} deliveryTag={} reasonKind={} reason={}",
				envelope.eventId(),
				queue,
				dlq,
				deliveryTag,
				reasonKind,
				failure != null ? failure.toString() : "unknown");
		return DeliveryResult.NACK;
	}

	private void sleepBackoff() {
		long backoffMs = Math.max(0L, retry.getBackoffMs());
		if (backoffMs <= 0L) {
			return;
		}
		try {
			Thread.sleep(backoffMs);
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("retry backoff interrupted", interrupted);
		}
	}

	public List<String> deadLetterQueues() {
		return List.copyOf(deadLetterQueues);
	}
}
