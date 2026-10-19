package cl.duoc.andesstay.notify.web;

import cl.duoc.andesstay.notify.messaging.DlqRateMeter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notify/metrics")
public class DlqMetricsController {

	private final DlqRateMeter dlqRateMeter;

	public DlqMetricsController(DlqRateMeter dlqRateMeter) {
		this.dlqRateMeter = dlqRateMeter;
	}

	@GetMapping("/dlq")
	public DlqMetricsResponse dlq() {
		return new DlqMetricsResponse(
				dlqRateMeter.attempts(),
				dlqRateMeter.deadLetters(),
				dlqRateMeter.rate(),
				dlqRateMeter.deadLettersByQueue());
	}
}
