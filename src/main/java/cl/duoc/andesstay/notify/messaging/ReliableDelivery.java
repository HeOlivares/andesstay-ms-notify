package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;

@Component
public class ReliableDelivery {

	private final ProcessedEventIds processedEventIds;
	private final DlqRateMeter dlqRateMeter;
	private final RabbitTopologyProperties topology;
	private final List<String> deadLetterQueues = new CopyOnWriteArrayList<>();

	public ReliableDelivery(
			ProcessedEventIds processedEventIds,
			DlqRateMeter dlqRateMeter,
			RabbitTopologyProperties topology) {
		this.processedEventIds = processedEventIds;
		this.dlqRateMeter = dlqRateMeter;
		this.topology = topology;
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
		try {
			work.run();
		} catch (RuntimeException failure) {
			processedEventIds.release(envelope.eventId());
			channel.basicNack(deliveryTag, false, false);
			dlqRateMeter.deadLetter(queue);
			deadLetterQueues.add(deadLetterQueue(queue));
			return DeliveryResult.NACK;
		}
		processedEventIds.complete(envelope.eventId());
		channel.basicAck(deliveryTag, false);
		return DeliveryResult.ACK;
	}

	public List<String> deadLetterQueues() {
		return List.copyOf(deadLetterQueues);
	}
}
