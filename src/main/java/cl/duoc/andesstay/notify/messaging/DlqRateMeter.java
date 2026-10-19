package cl.duoc.andesstay.notify.messaging;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

@Component
public class DlqRateMeter {

	private final LongAdder attempts = new LongAdder();
	private final LongAdder deadLetters = new LongAdder();
	private final ConcurrentMap<String, LongAdder> deadLettersByQueue = new ConcurrentHashMap<>();

	public void attempt() {
		attempts.increment();
	}

	public void deadLetter(String queue) {
		deadLetters.increment();
		deadLettersByQueue.computeIfAbsent(queue, ignored -> new LongAdder()).increment();
	}

	public long attempts() {
		return attempts.sum();
	}

	public long deadLetters() {
		return deadLetters.sum();
	}

	public long deadLetters(String queue) {
		LongAdder count = deadLettersByQueue.get(queue);
		return count == null ? 0 : count.sum();
	}

	public Map<String, Long> deadLettersByQueue() {
		Map<String, Long> snapshot = new LinkedHashMap<>();
		deadLettersByQueue.forEach((queue, count) -> snapshot.put(queue, count.sum()));
		return Map.copyOf(snapshot);
	}

	public double rate() {
		long total = attempts.sum();
		if (total == 0) {
			return 0d;
		}
		return deadLetters.sum() / (double) total;
	}
}
