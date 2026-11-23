package cl.duoc.andesstay.notify.activity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReservationActivityRecorder {

	private static final Logger log = LoggerFactory.getLogger(ReservationActivityRecorder.class);

	public void record(ReservationActivity activity) {
		if (activity == null
				|| blank(activity.eventId())
				|| blank(activity.action())
				|| activity.resourceId() == null) {
			throw new IllegalArgumentException("Reservation activity requires eventId, action and resourceId");
		}
		log.info(
				"Actividad de reserva registrada: eventId={}, action={}, resourceId={}",
				activity.eventId(),
				activity.action(),
				activity.resourceId());
	}

	private static boolean blank(String value) {
		return value == null || value.isBlank();
	}
}
