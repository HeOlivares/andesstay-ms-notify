package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.VoucherService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class VoucherQueueConsumer {

	private final VoucherService vouchers;
	private final ReliableDelivery delivery;

	public VoucherQueueConsumer(VoucherService vouchers, ReliableDelivery delivery) {
		this.vouchers = vouchers;
		this.delivery = delivery;
	}

	@RabbitListener(queues = RabbitTopologyConfig.VOUCHER_QUEUE, ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				RabbitTopologyConfig.VOUCHER_QUEUE,
				envelope,
				channel,
				deliveryTag,
				() -> vouchers.deliver(envelope));
	}
}
