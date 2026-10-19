package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.config.RabbitTopologyConfig;
import cl.duoc.andesstay.notify.delivery.VoucherService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class VoucherQueueConsumer {

	private final VoucherService vouchers;

	public VoucherQueueConsumer(VoucherService vouchers) {
		this.vouchers = vouchers;
	}

	@RabbitListener(queues = RabbitTopologyConfig.VOUCHER_QUEUE)
	public void onMessage(NotificationEnvelope envelope) {
		vouchers.deliver(envelope);
	}
}
