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

Exchanges: `cmd.direct`, `cmd.topic`, `cmd.dead.dlx`.

| Cola | Routing direct | DLQ |
| --- | --- | --- |
| `q.cmd.email` | `email.send` | `q.cmd.email.dlq` |
| `q.cmd.housekeeping` | `housekeeping.ticket` | `q.cmd.housekeeping.dlq` |
| `q.cmd.voucher` | `voucher.gen` | `q.cmd.voucher.dlq` |

## ACK / NACK

Los tres `@RabbitListener` usan `ackMode = MANUAL` y delegan en `ReliableDelivery`:

1. Se reserva el `eventId` de forma atómica.
2. Se ejecuta el trabajo del dominio (`GuestNotificationService`, `HousekeepingTicketService` o `VoucherService`).
3. Si termina bien: se completa la reserva y se hace `basicAck`.
4. Si falla: se libera la reserva y se hace `basicNack(..., requeue=false)`.
5. Si el `eventId` ya está reservado o procesado: `basicAck` sin volver a ejecutar el trabajo.

También queda configurado:

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
