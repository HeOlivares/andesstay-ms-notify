package cl.duoc.andesstay.notify.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LoggingVoucherSender implements VoucherSender {

	private static final Logger log = LoggerFactory.getLogger(LoggingVoucherSender.class);

	@Override
	public void send(VoucherDocument document) {
		log.info(
				"Voucher delivered reservationId={} documentType={} bytes={} eventId={}",
				document.reservationId(),
				document.documentType(),
				document.pdf().length,
				document.eventId());
	}
}
