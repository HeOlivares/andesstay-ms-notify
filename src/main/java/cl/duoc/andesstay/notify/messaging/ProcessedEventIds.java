package cl.duoc.andesstay.notify.messaging;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class ProcessedEventIds {

	private final Set<String> eventIds = ConcurrentHashMap.newKeySet();

	public boolean alreadyProcessed(String eventId) {
		return eventIds.contains(eventId);
	}

	public void markProcessed(String eventId) {
		eventIds.add(eventId);
	}
}
