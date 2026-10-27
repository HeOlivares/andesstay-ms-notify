# andesstay-ms-notify

Microservicio de notificaciones de AndesStay. Consume RabbitMQ, no expone API de negocio pública y no tiene base de datos.

## Arranque local

```bash
docker compose up -d rabbitmq
.\mvnw.cmd spring-boot:run
```

Variables útiles:

| Variable | Default | Uso |
| --- | --- | --- |
| `SERVER_PORT` | `8083` | Puerto HTTP |
| `SPRING_RABBITMQ_HOST` | `localhost` | Host de RabbitMQ |
| `SPRING_RABBITMQ_PORT` | `5672` | Puerto AMQP |
| `SPRING_RABBITMQ_USERNAME` | `guest` | Usuario |
| `SPRING_RABBITMQ_PASSWORD` | `guest` | Clave |
| `PROCESSED_EVENT_TTL` | `PT24H` | TTL de `eventId` procesados |

Health: `GET /api/notify/health`

## Topología

Los nombres de exchanges, colas, routing keys, topic patterns y sufijo DLQ viven en un solo lugar:

`andesstay.notify.topology.*` dentro de `src/main/resources/application.yml`.

`RabbitTopologyConfig` solo declara beans leyendo `RabbitTopologyProperties`. Los productores y consumidores no hardcodean esos nombres.

Defaults actuales:

| Cola | Routing direct | DLQ |
| --- | --- | --- |
| `q.cmd.email` | `email.send` | `q.cmd.email.dlq` |
| `q.cmd.housekeeping` | `housekeeping.ticket` | `q.cmd.housekeeping.dlq` |
| `q.cmd.voucher` | `voucher.gen` | `q.cmd.voucher.dlq` |

Exchanges: `cmd.direct`, `cmd.topic`, `cmd.dead.dlx`.

## ACK / NACK

Los tres `@RabbitListener` usan `ackMode = MANUAL` y delegan en `ReliableDelivery`.
La reserva del `eventId` ocurre **una sola vez** al inicio. Reintentos in-process ante
`RecoverableDeliveryException` (config: `andesstay.notify.retry.max-attempts` / `backoff-ms`).
Los senders deben lanzar esa excepción en fallos transitorios; cualquier otra `RuntimeException` es no recuperable.

| Situación | Acción | Resultado |
| --- | --- | --- |
| Trabajo OK | `basicAck` | Procesado |
| `RecoverableDeliveryException`, intentos &lt; N | Log `RETRY attempt=x/N`, espera `backoff-ms`, reintenta | Sigue en la misma entrega |
| `RecoverableDeliveryException`, agotados N | Libera `eventId`, `basicNack(..., requeue=false)` | DLQ (`reasonKind=exhausted`) |
| Otra `RuntimeException` | Libera `eventId`, `basicNack(..., requeue=false)` sin reintento | DLQ (`reasonKind=non-recoverable`) |
| `eventId` ya reservado/procesado | `basicAck` sin ejecutar el trabajo | Duplicado ignorado |

También:

```yaml
spring.rabbitmq.listener.simple.acknowledge-mode: manual
spring.rabbitmq.listener.simple.default-requeue-rejected: false
```

El NACK sin requeue hace que RabbitMQ enrute el mensaje a la DLQ de esa cola (`x-dead-letter-exchange` + `x-dead-letter-routing-key`).

## Deduplicación

`ProcessedEventIds` guarda reservas en memoria con TTL (`andesstay.notify.processed-event-ttl` / `PROCESSED_EVENT_TTL`).

Limitación: el almacenamiento es local al proceso. Con varias réplicas, dos instancias pueden procesar el mismo `eventId` si el mensaje se entrega a pods distintos. Para un despliegue multi-réplica hace falta un store compartido.

## Métrica de DLQ

`GET /api/notify/metrics/dlq`

```json
{
  "attempts": 10,
  "deadLetters": 2,
  "rate": 0.2,
  "deadLettersByQueue": {
    "q.cmd.email": 2
  }
}
```

`rate = deadLetters / attempts`. El endpoint es de solo lectura.

## Pruebas

```bash
.\mvnw.cmd test
```

Requiere Docker Desktop para las pruebas de integración con Testcontainers (`ReliableListenerIntegrationTest`). Esas pruebas publican en RabbitMQ real, esperan el ACK de los listeners y, ante fallo, consumen la DLQ correspondiente.

## Evidencia DLQ (H6)

Flujo verificable:

1. Se publica un `NotificationEnvelope` a `q.cmd.email` (`email.send`).
2. El `@RabbitListener` ejecuta el trabajo; el sender lanza error (`mail gateway down`).
3. `ReliableDelivery` hace `basicNack(deliveryTag, false, false)` (sin requeue) y escribe un log `WARN`:
   `NACK without requeue: eventId=... queue=q.cmd.email dlq=q.cmd.email.dlq ...`.
4. RabbitMQ enruta el mensaje a `q.cmd.email.dlq` con header `x-death`.
5. La cola principal queda vacía; la métrica `GET /api/notify/metrics/dlq` incrementa `deadLetters`.

### Test de integración (evidencia automatizada)

```bash
.\mvnw.cmd -Dtest=ReliableListenerIntegrationTest#failedEmailIsNackedAndRoutedToItsDeadLetterQueue test
```

El método `failedEmailIsNackedAndRoutedToItsDeadLetterQueue` demuestra contra RabbitMQ real (Testcontainers):

- publish → fallo del consumidor
- cola principal vacía (sin requeue)
- 1 mensaje en `q.cmd.email.dlq`
- header `x-death` presente
- body con el `eventId` fallido

También existe `failedHousekeepingAndVoucherReachTheirDeadLetterQueues` para las otras DLQ.
