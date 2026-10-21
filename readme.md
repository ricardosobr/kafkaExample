# Kafka Example — mensajería distribuida con Apache Kafka

Demo de dos aplicaciones Spring Boot que se comunican exclusivamente por Kafka, más un
front-end React. La actividad del Módulo 8 añade una **consulta de clientes** que viaja por
Kafka con patrón *request/reply* y correlación por `requestId`.

## Módulos

| Módulo | Puerto | Rol |
| --- | --- | --- |
| `str-producer` | 8000 (8001 si el 8000 está ocupado) | API REST; publica mensajes y correlaciona respuestas |
| `str-consumer` | 8100 | Consume, procesa y responde |

## Tópicos

| Tópico | Particiones | Réplicas | Productor | Consumidor |
| --- | --- | --- | --- | --- |
| `str-topic` | 2 | 1 | `StringProducerService` | `StrConsumerListener` (`group-1`, `group-2`) |
| `str-topic-response` | 2 | 1 | `StrConsumerListener.listener2` | `ResponseListener` (`group-1`) |
| `cliente-consulta` | 3 | 1 | `ClienteQueryPublisher` | `ClienteQueryListener` (`cliente-query-group`) |
| `cliente-consulta-respuesta` | 3 | 1 | `ClienteQueryListener` | `ClienteQueryResponseListener` (`cliente-query-response-group`) |

Los tópicos se declaran con `TopicBuilder` en el `KafkaAdminConfig` de cada módulo.
La réplica es 1 porque el clúster de desarrollo tiene un único broker; con más brokers basta
subir `.replicas(n)` sin tocar el código.

## Endpoints (`str-producer`)

Base: `http://localhost:8000`, o `http://localhost:8001` si se usó el puerto alternativo.

| Método | Ruta | Descripción |
| --- | --- | --- |
| `GET` | `/producer/menu?message=…` | Flujo original de menús (partición 0 sin respuesta, partición 1 responde) |
| `GET` | `/producer/test?message=…` | Igual que `/menu`, para pruebas |
| `GET` | `/producer/clientes?filtro=…` | **Nuevo.** Consulta de clientes vía Kafka |

### Consulta de clientes

```
GET http://localhost:8000/producer/clientes?filtro=vegan

200 OK
{
  "requestId": "…",
  "filtro": "vegan",
  "total": 2,
  "clientes": [ { "id": 1, "nombre": "…", "email": "…", "ciudad": "…", "telefono": "…" } ],
  "kafka": {
    "topicSolicitud": "cliente-consulta",
    "topicRespuesta": "cliente-consulta-respuesta",
    "partition": 0,
    "offset": 9,
    "key": "vegan"
  },
  "latenciaMs": 163,
  "procesadoEn": "…"
}
```

Secuencia:

1. `ClienteQueryPublisher` genera un `requestId` y registra un `CompletableFuture`.
2. Publica el JSON en `cliente-consulta` con `key = filtro` (misma consulta → misma partición).
3. `ClienteQueryListener` (nuevo, en `str-consumer`) busca con `ClienteQueryService` /
   `ClienteRepository` y responde en `cliente-consulta-respuesta`.
4. `ClienteQueryResponseListener` (en `str-producer`) completa el future por `requestId`.
5. Si no hay respuesta en 10 s, el endpoint devuelve `504` con `{"error":"TIMEOUT"}`.

## Ejecución

Requisitos: Docker, Docker Compose y **JDK 17**, usado para compilar y ejecutar esta práctica.

```bash
# 1. Kafka, Zookeeper y Kafdrop (desde kafkaExample)
docker compose up -d
# Kafdrop -> http://localhost:19000

# 2. Back-end: una terminal por servicio, desde la raíz de la actividad
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# terminal A
cd kafkaExample/str-producer && ./mvnw spring-boot:run     # :8000

# terminal B
cd kafkaExample/str-consumer && ./mvnw spring-boot:run     # :8100
```

Si el puerto 8000 está ocupado, arranca el productor con el puerto 8001 desde el código:

```bash
cd kafkaExample/str-producer
./mvnw spring-boot:run -Dspring-boot.run.arguments="--server.port=8001"
```

Y apunta el front a ese puerto:

```bash
cd frontProducerKafka
VITE_API_URL=http://localhost:8001 npm run dev -- --host 127.0.0.1 --port 5173 --strictPort
```

El consumidor no cambia: sigue en 8100. Si prefieres ejecutar el JAR, compílalo antes con
`./mvnw package` y luego `"$JAVA_HOME/bin/java" -jar target/str-producer-0.0.1-SNAPSHOT.jar --server.port=8001`.

## Notas

- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` está en `false` en `docker-compose.yml`. Sin eso, Kafka
  creaba `str-topic` con una sola partición al primer envío y los envíos explícitos a la
  partición 1 fallaban.
- `str-consumer/…/services/clientes.java` es código muerto previo (duplica `OrderProcessor`);
  se conserva para no alterar el material original.
