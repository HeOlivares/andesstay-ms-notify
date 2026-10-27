# H4 — Capturas (después del merge de H1–H3)

Hacerlas con Docker/Rabbit locales. **Ocultar** Bearer tokens y passwords de Rabbit en las imágenes.

## Arranque

```bash
# notify
cd Caso semestral/andesstay-ms-notify
docker compose up -d rabbitmq
.\mvnw.cmd spring-boot:run

# rabbit-admin (otra terminal)
cd ../andesstay-ms-rabbit-admin
.\mvnw.cmd spring-boot:run
```

UI Management: http://localhost:15672 (ocultar credenciales en la captura).

## 13-dlq.png

1. `POST http://localhost:8083/api/notify/demo/fail-email?eventId=event-demo-fail`
2. Management → Queues → `q.cmd.email.dlq` con ≥1 mensaje (o log WARN NACK).
3. Guardar como `docs/evidence/13-dlq.png` (o la carpeta de evidencia del curso).

Guion: `docs/DEMO-MENSAJERIA.md`.

## 14-admin-create-queue.png

```bash
curl -i -X POST http://localhost:8085/api/queues ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"q.demo.ep3\",\"durable\":true,\"exclusive\":false,\"autoDelete\":false}"
```

Esperado: **201**. Capturar terminal y/o Management con la cola.

## 15-admin-validation-400.png

```bash
curl -i -X POST http://localhost:8085/api/queues ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"\",\"durable\":true,\"exclusive\":false,\"autoDelete\":false}"
```

Esperado: **400** + `{"error":"name must not be blank"}`.
