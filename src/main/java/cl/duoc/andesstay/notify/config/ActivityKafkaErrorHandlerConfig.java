package cl.duoc.andesstay.notify.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ActivityKafkaErrorHandlerConfig {

	private static final Logger log = LoggerFactory.getLogger(ActivityKafkaErrorHandlerConfig.class);

	@Bean
	CommonErrorHandler reservationActivityErrorHandler(
			@Value("${andesstay.notify.kafka.max-attempts:3}") int maxAttempts,
			@Value("${andesstay.notify.kafka.backoff-ms:200}") long backoffMs) {
		return activityErrorHandler(maxAttempts, backoffMs);
	}

	public static DefaultErrorHandler activityErrorHandler(int maxAttempts, long backoffMs) {
		long retriesAfterFirstAttempt = Math.max(0, maxAttempts - 1L);
		DefaultErrorHandler handler = new DefaultErrorHandler(
				ActivityKafkaErrorHandlerConfig::logDiscarded,
				new FixedBackOff(Math.max(0L, backoffMs), retriesAfterFirstAttempt));
		handler.addNotRetryableExceptions(IllegalArgumentException.class, DeserializationException.class);
		handler.setSeekAfterError(true);
		return handler;
	}

	private static void logDiscarded(ConsumerRecord<?, ?> record, Exception exception) {
		log.error(
				"Actividad de reserva descartada: topic={} partition={} offset={} reason={}",
				record.topic(),
				record.partition(),
				record.offset(),
				rootReason(exception),
				exception);
	}

	private static String rootReason(Throwable exception) {
		Throwable current = exception;
		while (current.getCause() != null && current.getCause() != current) {
			current = current.getCause();
		}
		return current.toString();
	}
}
