package cl.duoc.andesstay.notify.messaging;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;

@Component
public class ReliableDelivery {

	private final ProcessedEventIds processedEventIds;
	private final DlqRateMeter dlqRateMeter;
	private final List<String> deadLetterQueues = new CopyOnWriteArrayList<>();

	public ReliableDelivery(ProcessedEventIds processedEventIds, DlqRateMeter dlqRateMeter) {
		this.processedEventIds = processedEventIds;
		this.dlqRateMeter = dlqRateMeter;
	}

	public static String deadLetterQueue(String queue) {
		return queue + ".dlq";
	}

	public DeliveryResult consume(String queue, NotificationEnvelope envelope, Channel channel, long deliveryTag, Runnable work)
			throws IOException {
		dlqRateMeter.attempt();
		if (processedEventIds.alreadyProcessed(envelope.eventId())) {
			channel.basicAck(deliveryTag, false);
			return DeliveryResult.DUPLICATE;
		}
		try {
			work.run();
		} catch (RuntimeException failure) {
			channel.basicNack(deliveryTag, false, false);
			dlqRateMeter.deadLetter(queue);
			deadLetterQueues.add(deadLetterQueue(queue));
			return DeliveryResult.NACK;
		}
		processedEventIds.markProcessed(envelope.eventId());
		channel.basicAck(deliveryTag, false);
		return DeliveryResult.ACK;
	}

	public List<String> deadLetterQueues() {
		return List.copyOf(deadLetterQueues);
	}
}
