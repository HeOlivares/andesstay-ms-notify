package cl.duoc.andesstay.notify.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LoggingHousekeepingTicketSender implements HousekeepingTicketSender {

	private static final Logger log = LoggerFactory.getLogger(LoggingHousekeepingTicketSender.class);

	@Override
	public void send(HousekeepingTicket ticket) {
		log.info(
				"Housekeeping ticket delivered reservationId={} unitId={} kind={} eventId={}",
				ticket.reservationId(),
				ticket.unitId(),
				ticket.kind(),
				ticket.eventId());
	}
}
