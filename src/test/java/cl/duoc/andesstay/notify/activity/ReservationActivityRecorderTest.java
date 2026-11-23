package cl.duoc.andesstay.notify.activity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class ReservationActivityRecorderTest {

	private final ReservationActivityRecorder recorder = new ReservationActivityRecorder();

	@Test
	void recordsActivityWhenTheProducerFieldsArePresent() {
		ListAppender<ILoggingEvent> logs = attachLogs();
		try {
			recorder.record(activity("evt-1", "RESERVATION_CREATED", 7L));
		} finally {
			detachLogs(logs);
		}

		assertEquals(1, logs.list.size());
		String message = logs.list.get(0).getFormattedMessage();
		assertTrue(message.contains("eventId=evt-1"), message);
		assertTrue(message.contains("action=RESERVATION_CREATED"), message);
		assertTrue(message.contains("resourceId=7"), message);
	}

	@Test
	void rejectsActivityWithoutEventId() {
		assertThrows(IllegalArgumentException.class, () -> recorder.record(null));
		assertThrows(IllegalArgumentException.class, () -> recorder.record(activity(null, "RESERVATION_CREATED", 7L)));
		assertThrows(IllegalArgumentException.class, () -> recorder.record(activity(" ", "RESERVATION_CREATED", 7L)));
	}

	@Test
	void rejectsActivityWithoutAction() {
		assertThrows(IllegalArgumentException.class, () -> recorder.record(activity("evt-1", null, 7L)));
		assertThrows(IllegalArgumentException.class, () -> recorder.record(activity("evt-1", " ", 7L)));
	}

	@Test
	void rejectsActivityWithoutResourceId() {
		assertThrows(IllegalArgumentException.class,
				() -> recorder.record(activity("evt-1", "RESERVATION_CREATED", null)));
	}

	private static ListAppender<ILoggingEvent> attachLogs() {
		Logger logger = (Logger) LoggerFactory.getLogger(ReservationActivityRecorder.class);
		ListAppender<ILoggingEvent> appender = new ListAppender<>();
		appender.start();
		logger.addAppender(appender);
		return appender;
	}

	private static void detachLogs(ListAppender<ILoggingEvent> appender) {
		Logger logger = (Logger) LoggerFactory.getLogger(ReservationActivityRecorder.class);
		logger.detachAppender(appender);
	}

	private static ReservationActivity activity(String eventId, String action, Long resourceId) {
		return new ReservationActivity(
				eventId,
				Instant.parse("2026-10-26T12:00:00Z"),
				"cliente@x.cl",
				action,
				"RESERVATION",
				resourceId,
				Map.of("newStatus", "PENDING"));
	}
}
