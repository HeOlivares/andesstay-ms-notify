# Guion demo — Mensajería (Héctor)

Duración sugerida: 5–8 min. Local o Rabbit de la EC2.

## 0. Arranque (1 min antes)

```bash
# Terminal A — Rabbit + Management
cd Caso semestral/andesstay-ms-notify
docker compose up -d rabbitmq

# Terminal B — notify (8083)
.\mvnw.cmd spring-boot:run

# Terminal C — rabbit-admin (8085)  [rama con H3–H5 mergeada, o la tip más actual]
cd ../andesstay-ms-rabbit-admin
.\mvnw.cmd spring-boot:run
```

UI RabbitMQ: http://localhost:15672 (`guest` / `guest`)

Si usas Rabbit en EC2, exporta:

```bash
$env:SPRING_RABBITMQ_HOST="IP_O_HOST_EC2"
$env:SPRING_RABBITMQ_PORT="5672"
```

---

## 1. Código: topology + listeners + ACK/DLQ (~2 min)

Abrir en IDE y comentar:

| Qué mostrar | Dónde |
| --- | --- |
| Nombres centralizados | `application.yml` → `andesstay.notify.topology.*` |
| Beans Queue/Exchange/Binding + DLQ | `RabbitTopologyConfig` |
| Listeners por dominio | `EmailQueueConsumer`, `HousekeepingQueueConsumer`, `VoucherQueueConsumer` (`ackMode = MANUAL`) |
| ACK / NACK → DLQ | `ReliableDelivery` (`basicAck` / `basicNack(..., requeue=false)`) |

Frase: *“Un solo origen de nombres en YAML; los tres listeners confirman a mano; si falla, NACK sin requeue y el broker manda a la DLQ.”*

En Management UI: exchanges `cmd.direct`, `cmd.topic`, `cmd.dead.dlx` y colas `q.cmd.email|housekeeping|voucher` + `*.dlq`.

---

## 2. ms-rabbit-admin: crear / borrar cola demo (~1 min)

> Si `main` aún tiene rutas viejas usa `/api/rabbit/queues`. Con H4 mergeado: `/api/queues`.

```bash
# Crear
curl -X POST http://localhost:8085/api/queues ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"q.demo.hector\",\"durable\":true,\"exclusive\":false,\"autoDelete\":false}"

# Ver en Management UI → Queues → q.demo.hector

# Borrar
curl -X DELETE http://localhost:8085/api/queues/q.demo.hector
```

Validación (opcional, H5):

```bash
curl -i -X POST http://localhost:8085/api/queues ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"\",\"durable\":true,\"exclusive\":false,\"autoDelete\":false}"
```

Esperado: `400` + `{"error":"name must not be blank"}`.

---

## 3. “Confirmar reserva” → 3 colas / logs (~2 min)

Reservas aún no publica a Rabbit; el demo usa endpoints de notify:

```bash
curl -X POST http://localhost:8083/api/notify/demo/confirmed-reservation
```

Qué mirar:

1. **Logs de notify** (casi al instante):
   - `Guest notification delivered ...`
   - log de voucher / housekeeping ticket
2. **Management UI**: las tres colas pueden quedar en 0 (ACK rápido). Lo importante son los **logs** = procesamiento exitoso.
3. Métrica: `curl http://localhost:8083/api/notify/metrics/dlq` (attempts sube, deadLetters no).

Frase: *“Al confirmar, salen email, voucher y housekeeping; los listeners hacen ACK y dejan log de entrega.”*

---

## 4. Forzar fallo → DLQ (~2 min)

```bash
curl -X POST "http://localhost:8083/api/notify/demo/fail-email?eventId=event-demo-fail"
```

Qué mirar:

1. Log **WARN**: `NACK without requeue: eventId=event-demo-fail queue=q.cmd.email dlq=q.cmd.email.dlq ...`
2. Management UI → cola `q.cmd.email` vacía; `q.cmd.email.dlq` con **1** mensaje (header `x-death`)
3. `curl http://localhost:8083/api/notify/metrics/dlq` → `deadLetters` incrementa

Frase: *“Falló el procesamiento, NACK sin requeue, el mensaje quedó en la DLQ; no se reencola infinito.”*

---

## Plan B (si no hay endpoints demo)

```bash
.\mvnw.cmd "-Dtest=ReliableListenerIntegrationTest#failedEmailIsNackedAndRoutedToItsDeadLetterQueue" test
```

Muestra en consola el WARN de NACK y el test verde = misma evidencia.

---

## Checklist rápido

- [ ] YAML topology + 3 listeners + ReliableDelivery
- [ ] rabbit-admin crea y borra `q.demo.hector`
- [ ] `POST .../demo/confirmed-reservation` → logs en 3 flujos
- [ ] `POST .../demo/fail-email` → DLQ + WARN NACK
