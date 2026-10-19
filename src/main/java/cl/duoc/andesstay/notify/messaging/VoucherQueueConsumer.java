package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
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
	private final RabbitTopologyProperties topology;

	public VoucherQueueConsumer(
			VoucherService vouchers,
			ReliableDelivery delivery,
			RabbitTopologyProperties topology) {
		this.vouchers = vouchers;
		this.delivery = delivery;
		this.topology = topology;
	}

	@RabbitListener(queues = "${andesstay.notify.topology.queues.voucher}", ackMode = "MANUAL")
	public void onMessage(
			NotificationEnvelope envelope,
			Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
		delivery.consume(
				topology.getQueues().getVoucher(),
				envelope,
				channel,
				deliveryTag,
				() -> vouchers.deliver(envelope));
	}
}
