package cl.duoc.andesstay.notify.messaging;

import cl.duoc.andesstay.notify.activity.ReservationActivity;
import cl.duoc.andesstay.notify.activity.ReservationActivityRecorder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReservationActivityListener {

	private final ReservationActivityRecorder recorder;

	public ReservationActivityListener(ReservationActivityRecorder recorder) {
		this.recorder = recorder;
	}

	@KafkaListener(
			topics = "${andesstay.notify.kafka.activity-topic:reservations.activity}",
			groupId = "${spring.kafka.consumer.group-id:notify-activity}",
			concurrency = "${andesstay.notify.kafka.activity-concurrency:3}")
	public void onActivity(ReservationActivity activity) {
		recorder.record(activity);
	}
}
