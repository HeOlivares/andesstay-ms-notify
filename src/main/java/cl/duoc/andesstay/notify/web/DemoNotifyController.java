package cl.duoc.andesstay.notify.web;

import cl.duoc.andesstay.notify.config.RabbitTopologyProperties;
import cl.duoc.andesstay.notify.messaging.ConfirmedReservation;
import cl.duoc.andesstay.notify.messaging.ConfirmedReservationPublisher;
import cl.duoc.andesstay.notify.messaging.HousekeepingTicketPublisher;
import cl.duoc.andesstay.notify.messaging.HousekeepingTicketRequest;
import cl.duoc.andesstay.notify.messaging.NotificationEnvelope;
import cl.duoc.andesstay.notify.messaging.NotificationPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.UUID;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoints solo para demo en vivo (local / EC2). Publican a las colas de notify.
 */
@RestController
@RequestMapping("/api/notify/demo")
public class DemoNotifyController {

	private final ConfirmedReservationPublisher confirmedReservationPublisher;
	private final HousekeepingTicketPublisher housekeepingTicketPublisher;
	private final NotificationPublisher notificationPublisher;
	private final RabbitTopologyProperties topology;
	private final ObjectMapper objectMapper;

	public DemoNotifyController(
			ConfirmedReservationPublisher confirmedReservationPublisher,
			HousekeepingTicketPublisher housekeepingTicketPublisher,
			NotificationPublisher notificationPublisher,
			RabbitTopologyProperties topology,
			ObjectMapper objectMapper) {
		this.confirmedReservationPublisher = confirmedReservationPublisher;
		this.housekeepingTicketPublisher = housekeepingTicketPublisher;
		this.notificationPublisher = notificationPublisher;
		this.topology = topology;
		this.objectMapper = objectMapper;
	}

	/**
	 * Simula confirmar una reserva: email + voucher en sus colas, más ticket de housekeeping.
	 */
	@PostMapping("/confirmed-reservation")
	public Map<String, Object> confirmedReservation() {
		String trace = "demo-" + UUID.randomUUID();
		String corr = "corr-" + UUID.randomUUID();
		confirmedReservationPublisher.publish(new ConfirmedReservation(
				"42",
				"Ana Demo",
				"ana.demo@andesstay.local",
				"EMAIL",
				null,
				true,
				false,
				trace,
				corr));
		housekeepingTicketPublisher.publish(new HousekeepingTicketRequest(
				"42",
				"U-7",
				"PREPARATION",
				trace,
				corr));
		return Map.of(
				"status", "PUBLISHED",
				"traceId", trace,
				"queues", Map.of(
						"email", topology.getQueues().getEmail(),
						"voucher", topology.getQueues().getVoucher(),
						"housekeeping", topology.getQueues().getHousekeeping()),
				"hint", "Revisa logs de notify (Guest notification / voucher / housekeeping) o Management UI");
	}

	/**
	 * Publica un email inválido para forzar fallo → NACK → DLQ.
	 */
	@PostMapping("/fail-email")
	public Map<String, Object> failEmail(@RequestParam(defaultValue = "event-demo-fail") String eventId) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("reservationId", "42");
		payload.put("guestName", "Ana Demo");
		payload.put("kind", "INVALID_KIND");
		payload.put("channel", "EMAIL");
		payload.put("guestEmail", "ana.demo@andesstay.local");
		notificationPublisher.publish(new NotificationEnvelope(
				topology.getRoutingKeys().getEmail(),
				eventId,
				java.time.Instant.now(),
				"demo-fail",
				"corr-fail",
				payload));
		return Map.of(
				"status", "PUBLISHED_POISON",
				"eventId", eventId,
				"queue", topology.getQueues().getEmail(),
				"expectedDlq", topology.deadLetterQueue(topology.getQueues().getEmail()),
				"hint", "Espera 1s y mira q.cmd.email.dlq + log WARN NACK without requeue");
	}
}
