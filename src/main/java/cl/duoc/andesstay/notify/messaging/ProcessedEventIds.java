package cl.duoc.andesstay.notify.messaging;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ProcessedEventIds {

	static final Duration DEFAULT_TTL = Duration.ofHours(24);

	private final ConcurrentMap<String, Reservation> reservations = new ConcurrentHashMap<>();
	private final Duration ttl;
	private final Clock clock;

	@Autowired
	public ProcessedEventIds(@Value("${andesstay.notify.processed-event-ttl:PT24H}") Duration ttl) {
		this(ttl, Clock.systemUTC());
	}

	ProcessedEventIds(Duration ttl, Clock clock) {
		this.ttl = ttl;
		this.clock = clock;
	}

	public boolean tryReserve(String eventId) {
		requireText(eventId);
		expire();
		Reservation reserved = new Reservation(clock.instant().plus(ttl));
		while (true) {
			Reservation existing = reservations.putIfAbsent(eventId, reserved);
			if (existing == null) {
				return true;
			}
			if (existing.expiresAt().isAfter(clock.instant())) {
				return false;
			}
			if (reservations.replace(eventId, existing, reserved)) {
				return true;
			}
		}
	}

	public void complete(String eventId) {
		reservations.put(eventId, new Reservation(clock.instant().plus(ttl)));
	}

	public void release(String eventId) {
		reservations.remove(eventId);
	}

	public int size() {
		expire();
		return reservations.size();
	}

	private void expire() {
		Instant now = clock.instant();
		reservations.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
	}

	private static void requireText(String eventId) {
		if (eventId == null || eventId.isBlank()) {
			throw new IllegalArgumentException("eventId is required");
		}
	}

	private record Reservation(Instant expiresAt) {
	}
}
