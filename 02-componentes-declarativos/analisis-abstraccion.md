# Análisis de la Abstracción — Fase 2

## TODO 1: `redisHost` con el DNS de Kubernetes

`kubectl get svc -n redis` mostró que el chart de Bitnami crea tres Services:

```text
NAME             TYPE        CLUSTER-IP     PORT(S)
redis-headless   ClusterIP   None           6379/TCP
redis-master     ClusterIP   10.96.99.154   6379/TCP
redis-replicas   ClusterIP   10.96.27.90    6379/TCP
```

Configuré `redisHost: "redis-master.redis.svc.cluster.local:6379"`. Elegí **`redis-master`** porque el componente `pubsub.redis` de Dapr usa **Redis Streams** (`XADD`, `XREADGROUP`, `XACK`), que requieren escritura, y las réplicas son de solo lectura. El headless service tampoco me sirve, porque resuelve a las IPs de cada Pod en lugar de una IP estable. Usé el FQDN completo (y no solo `redis-master.redis`) para que la resolución no dependa del `search` de `/etc/resolv.conf` del Pod.

Lo verifiqué con `kubectl get components.dapr.io order-pubsub -o yaml` y, de forma más contundente, inspeccionando el broker después de publicar. El stream `orders` y el consumer group `order-subscriber` existían en Redis sin que el código Python mencionara Redis en ningún momento.

---

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
| NATS JetStream | `pubsub.jetstream` | `natsURL`, `durableName`, `queueGroupName` |
| Azure Service Bus (Topics) | `pubsub.azure.servicebus.topics` | `connectionString`, `defaultMessageTimeToLiveInSec` |
| AWS SNS/SQS | `pubsub.aws.snssqs` | `region`, `messageRetentionPeriod` |
| GCP Pub/Sub | `pubsub.gcp.pubsub` | `projectId`, `maxDeliveryAttempts` |
| Apache Pulsar | `pubsub.pulsar` | `host`, `consumerID` |

Incluí más de cinco a propósito: el catálogo cubre brokers *self-hosted* y los servicios gestionados de los tres hyperscalers, que es justamente lo que hace atractiva la abstracción para una plataforma multi-nube.

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
- Además, con `Configuration.spec.secrets.scopes` puedo restringir qué secretos puede leer cada app-id con la API de secretos (`defaultAccess: deny` + `allowedSecrets`). Esto aplica mínimo privilegio también al acceso programático.

Como complemento operativo, combinaría esto con **External Secrets Operator** o **Sealed Secrets** para que el ciclo de vida del secreto (rotación, auditoría) quede en manos del equipo de seguridad. Así el repositorio GitOps conserva solo referencias.
