# Evidencia DLQ — andesstay-ms-notify (H6)

## Objetivo

Demostrar: **publicar mensaje → consumidor falla → NACK sin requeue → mensaje en DLQ + log**.

## Evidencia automatizada

Archivo: `src/test/java/cl/duoc/andesstay/notify/messaging/ReliableListenerIntegrationTest.java`

Método: `failedEmailIsNackedAndRoutedToItsDeadLetterQueue`

Ejecutar:

```bash
.\mvnw.cmd "-Dtest=ReliableListenerIntegrationTest#failedEmailIsNackedAndRoutedToItsDeadLetterQueue" test
```

Requisito: Docker Desktop (Testcontainers `rabbitmq:3-alpine`).

### Qué valida

| Paso | Assertion |
| --- | --- |
| Publish | `publisher.publish(email("event-email-fail", ...))` |
| Fallo | `guestNotificationSender.send` lanza `IllegalStateException("mail gateway down")` |
| Sin requeue | `messageCount(q.cmd.email) == 0` |
| DLQ | `messageCount(q.cmd.email.dlq) == 1` |
| Broker | header `x-death` en el mensaje de la DLQ |
| Payload | body contiene `event-email-fail` |
| Métrica | `DlqRateMeter.deadLetters` incrementa |

### Log esperado (WARN)

```
NACK without requeue: eventId=event-email-fail queue=q.cmd.email dlq=q.cmd.email.dlq deliveryTag=... reason=java.lang.IllegalStateException: mail gateway down
```

Emitido por `ReliableDelivery` al hacer `basicNack(..., requeue=false)`.

## Evidencia manual (opcional)

1. `docker compose up -d rabbitmq`
2. Levantar notify con un sender que falle (o mock en test).
3. Publicar a `cmd.direct` / `email.send`.
4. En RabbitMQ Management (`http://localhost:15672`): cola `q.cmd.email` vacía, `q.cmd.email.dlq` con 1 mensaje y `x-death`.
5. Revisar log WARN de NACK y `GET /api/notify/metrics/dlq`.
