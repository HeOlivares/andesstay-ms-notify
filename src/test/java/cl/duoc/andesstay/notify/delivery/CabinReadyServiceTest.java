package cl.duoc.andesstay.notify.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class CabinReadyServiceTest {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final CabinReadyService service = new CabinReadyService();
	private ListAppender<ILoggingEvent> logs;

	@BeforeEach
	void captureLogs() {
		logs = new ListAppender<>();
		logs.start();
		((Logger) LoggerFactory.getLogger(CabinReadyService.class)).addAppender(logs);
	}

	@AfterEach
	void stopCapture() {
		((Logger) LoggerFactory.getLogger(CabinReadyService.class)).detachAppender(logs);
	}

	@Test
	void cabinReadyIsLoggedWithReservationAndEvent() {
		service.deliver(envelope("cabin.ready", "CABIN_READY"));

		assertEquals(1, logs.list.size());
		assertEquals("Cabin ready notified reservationId=10 eventId=event-1", logs.list.get(0).getFormattedMessage());
	}

	@Test
	void otherKindIsNotNotified() {
		assertThrows(IllegalArgumentException.class, () -> service.deliver(envelope("cabin.ready", "PENDING")));
		assertTrue(logs.list.isEmpty());
	}

	private NotificationEnvelope envelope(String type, String kind) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "10");
		payload.put("kind", kind);
		return new NotificationEnvelope(type, "event-1", Instant.parse("2026-10-19T03:00:00Z"), "trace-1", "corr-1", payload);
	}
}
