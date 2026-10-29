# Análisis de la Abstracción — Fase 2

## TODO 2: Análisis de la Abstracción

### 1. Migración de broker: ¿qué archivos cambiarían?

Si mañana migráramos de Redis a Apache Kafka, en este repositorio **solo cambiaría el manifiesto del componente**: `02-componentes-declarativos/pubsub-redis.yaml` y su variante de gobernanza `04-reto-gobernanza/pubsub-redis-challenge.yaml`, idealmente renombrados a `pubsub-kafka.yaml`. El cambio concreto sería:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: order-pubsub          # ← se mantiene: es el contrato con el código
spec:
  type: pubsub.kafka          # ← cambia la implementación
  version: v1
  metadata:
    - name: brokers
      value: "kafka.kafka.svc.cluster.local:9092"
    - name: consumerGroup
      value: "{appID}"
    - name: authType
      value: "none"
```

**El código Python del publisher y del subscriber no cambiaría ni una línea.** Ambos solo dependen de tres cosas que pertenecen al contrato de Dapr y no al broker:

- el nombre lógico `order-pubsub` (`PUBSUB_NAME` en el publisher y `pubsubname` en `/dapr/subscribe`),
- el nombre del tópico `orders`,
- la API HTTP del sidecar (`/v1.0/publish/...`) y el formato CloudEvents de entrega.

Tampoco cambiarían los Dockerfiles, el `requirements.txt` (no hay ningún `redis-py` ni `kafka-python`), el Deployment ni el Job. Fuera del código de aplicación sí habría trabajo de plataforma: aprovisionar Kafka (Strimzi, MSK, Confluent), ajustar las *network policies* y revisar la metadata de gobernanza. Como explico en la Fase 4, algunas propiedades como el TTL o el consumer group **no tienen una semántica idéntica entre brokers**, así que la migración es transparente para el código, pero no necesariamente para el comportamiento operativo.

### 2. Inventario de 5 brokers soportados por Dapr

En todos los casos el campo que cambia es **`spec.type`**, junto con el bloque `spec.metadata`, que es propio de cada broker (direcciones, autenticación, tuning):

| Broker | `spec.type` en el YAML | Metadata característica |
|---|---|---|
| Redis Streams | `pubsub.redis` | `redisHost`, `consumerID`, `streamTTL` |
| Apache Kafka | `pubsub.kafka` | `brokers`, `consumerGroup`, `authType` |
| RabbitMQ | `pubsub.rabbitmq` | `connectionString`, `durable`, `deliveryMode` |
| Azure Service Bus (Topics) | `pubsub.azure.servicebus.topics` | `connectionString`, `defaultMessageTimeToLiveInSec` |
| AWS SNS/SQS | `pubsub.aws.snssqs` | `region`, `messageRetentionPeriod` |

### 3. Manejo seguro de secretos en producción

Dapr permite que cualquier entrada de `spec.metadata` tome su valor de un **Secret Store component** usando `secretKeyRef` en lugar de `value`. El YAML solo contiene una **referencia**, nunca el secreto:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: order-pubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
    - name: redisHost
      value: "redis-master.redis.svc.cluster.local:6379"
    - name: redisPassword
      secretKeyRef:
        name: redis          # nombre del secreto
        key: redis-password  # clave dentro del secreto
auth:
  secretStore: kubernetes    # qué secret store resuelve la referencia
```

Funciona así:

- En Kubernetes, Dapr carga automáticamente un secret store llamado `kubernetes` (`secretstores.kubernetes`) que lee `Secret` nativos del mismo namespace. Lo confirmé consultando `GET /v1.0/metadata` en el sidecar del subscriber: aparece cargado junto a `order-pubsub` sin que yo lo declarara.
- Para producción, en lugar de `kubernetes` uso un secret store respaldado por un gestor dedicado: **HashiCorp Vault** (`secretstores.hashicorp.vault`), **Azure Key Vault**, **AWS Secrets Manager** o **GCP Secret Manager**. Cambiar de gestor es, de nuevo, solo cambiar un componente.
- El sidecar resuelve la referencia **en runtime**, al cargar el componente. El secreto nunca pasa por Git ni por el código de la app.
