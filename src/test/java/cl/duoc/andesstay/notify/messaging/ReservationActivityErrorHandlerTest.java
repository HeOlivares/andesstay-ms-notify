package cl.duoc.andesstay.notify.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import cl.duoc.andesstay.notify.activity.ReservationActivity;
import cl.duoc.andesstay.notify.config.ActivityKafkaErrorHandlerConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.SerializationUtils;

class ReservationActivityErrorHandlerTest {

	private static final String TOPIC = "reservations.activity";

	@Test
	void invalidPayloadIsLoggedOnceAndSeeksToTheNextOffset() {
		DefaultErrorHandler handler = handler();
		Consumer<String, ReservationActivity> consumer = consumer();
		ListAppender<ILoggingEvent> logs = attachLogs();
		try {
			handler.handleRemaining(
					wrapped(new IllegalArgumentException(
							"Reservation activity requires eventId, action and resourceId")),
					batch(1, 41L),
					consumer,
					mock(MessageListenerContainer.class));
		} finally {
			detachLogs(logs);
		}

		assertTrue(handler.seeksAfterHandling());
		assertDiscarded(logs, 1, 41L);
		verify(consumer).seek(new TopicPartition(TOPIC, 1), 42L);
		verify(consumer, never()).seek(new TopicPartition(TOPIC, 1), 41L);
	}

	@Test
	void bodyThatDoesNotDeserializeFollowsTheInvalidPayloadPath() {
		DefaultErrorHandler handler = handler();
		RecordHeaders headers = new RecordHeaders();
		ErrorHandlingDeserializer<ReservationActivity> deserializer = activityDeserializer();
		ReservationActivity value = deserializer.deserialize(TOPIC, headers, "{".getBytes(StandardCharsets.UTF_8));
		DeserializationException failure = SerializationUtils.byteArrayToDeserializationException(
				new LogAccessor(ReservationActivityErrorHandlerTest.class),
				headers.lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER));
		Consumer<String, ReservationActivity> consumer = consumer();
		ListAppender<ILoggingEvent> logs = attachLogs();
		try {
			handler.handleRemaining(
					wrapped(failure),
					batch(0, 8L),
					consumer,
					mock(MessageListenerContainer.class));
		} finally {
			detachLogs(logs);
		}

		assertNull(value);
		assertDiscarded(logs, 0, 8L);
		verify(consumer).seek(new TopicPartition(TOPIC, 0), 9L);
		verify(consumer, never()).seek(new TopicPartition(TOPIC, 0), 8L);
	}

	@Test
	void transientFailureRetriesTwiceThenLogsAndSeeksPastTheRecord() {
		DefaultErrorHandler handler = handler();
		Consumer<String, ReservationActivity> consumer = consumer();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ListenerExecutionFailedException failure = wrapped(new IllegalStateException("catalog timeout"));
		List<ConsumerRecord<?, ?>> batch = batch(2, 15L);
		ListAppender<ILoggingEvent> logs = attachLogs();
		try {
			assertStillRetrying(handler, failure, batch, consumer, container);
			assertStillRetrying(handler, failure, batch, consumer, container);
			handler.handleRemaining(failure, batch, consumer, container);
		} finally {
			detachLogs(logs);
		}

		assertDiscarded(logs, 2, 15L);
		assertTrue(logs.list.get(0).getFormattedMessage().contains("catalog timeout"));
		verify(consumer, times(2)).seek(new TopicPartition(TOPIC, 2), 15L);
		verify(consumer).seek(new TopicPartition(TOPIC, 2), 16L);
	}

	private static void assertStillRetrying(
			DefaultErrorHandler handler,
			Exception failure,
			List<ConsumerRecord<?, ?>> batch,
			Consumer<String, ReservationActivity> consumer,
			MessageListenerContainer container) {
		RuntimeException retrying = assertThrows(RuntimeException.class,
				() -> handler.handleRemaining(failure, batch, consumer, container));
		assertEquals("Record in retry and not yet recovered", retrying.getMessage());
	}

	private static DefaultErrorHandler handler() {
		return ActivityKafkaErrorHandlerConfig.activityErrorHandler(3, 0L);
	}

	private static ErrorHandlingDeserializer<ReservationActivity> activityDeserializer() {
		Map<String, Object> config = new HashMap<>();
		config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "cl.duoc.andesstay.notify.activity");
		config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
		config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ReservationActivity.class.getName());
		ErrorHandlingDeserializer<ReservationActivity> deserializer = new ErrorHandlingDeserializer<>();
		deserializer.configure(config, false);
		return deserializer;
	}

	private static ListenerExecutionFailedException wrapped(Exception cause) {
		return new ListenerExecutionFailedException("Listener failed", "notify-activity", cause);
	}

	private static List<ConsumerRecord<?, ?>> batch(int partition, long offset) {
		return List.of(record(partition, offset), record(partition, offset + 1));
	}

	private static ConsumerRecord<String, ReservationActivity> record(int partition, long offset) {
		return new ConsumerRecord<>(TOPIC, partition, offset, null, null);
	}

	@SuppressWarnings("unchecked")
	private static Consumer<String, ReservationActivity> consumer() {
		return mock(Consumer.class);
	}

	private static void assertDiscarded(ListAppender<ILoggingEvent> logs, int partition, long offset) {
		assertEquals(1, logs.list.size());
		String message = logs.list.get(0).getFormattedMessage();
		assertTrue(message.contains("topic=" + TOPIC), message);
		assertTrue(message.contains("partition=" + partition), message);
		assertTrue(message.contains("offset=" + offset), message);
	}

	private static ListAppender<ILoggingEvent> attachLogs() {
		Logger logger = (Logger) LoggerFactory.getLogger(ActivityKafkaErrorHandlerConfig.class);
		ListAppender<ILoggingEvent> appender = new ListAppender<>();
		appender.start();
		logger.addAppender(appender);
		return appender;
	}

	private static void detachLogs(ListAppender<ILoggingEvent> appender) {
		Logger logger = (Logger) LoggerFactory.getLogger(ActivityKafkaErrorHandlerConfig.class);
		logger.detachAppender(appender);
	}
}
