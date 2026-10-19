package cl.duoc.andesstay.notify.web;

import java.util.Map;

public record DlqMetricsResponse(
		long attempts,
		long deadLetters,
		double rate,
		Map<String, Long> deadLettersByQueue) {
}
