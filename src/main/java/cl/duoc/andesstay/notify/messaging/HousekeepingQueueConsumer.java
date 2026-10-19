package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.delivery.HousekeepingTicketService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class HousekeepingQueueConsumer {

	public static final String QUEUE = "q.cmd.housekeeping";

	private final HousekeepingTicketService tickets;

	public HousekeepingQueueConsumer(HousekeepingTicketService tickets) {
		this.tickets = tickets;
	}

	@RabbitListener(queues = QUEUE)
	public void onMessage(NotificationEnvelope envelope) {
		tickets.deliver(envelope);
	}
}
