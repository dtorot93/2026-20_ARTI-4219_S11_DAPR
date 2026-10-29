# Fase 4: Reto de Gobernanza — TTL y Multi-Tenant Namespaces

Esta fase es un **reto semi-guiado**. Se proporcionan el contexto, los conceptos y el archivo plantilla, pero la implementación es responsabilidad del estudiante.

---

## 4.1 Contexto: ¿Por qué gobernanza en mensajería?

En plataformas multi-tenant donde múltiples equipos comparten la misma infraestructura de mensajería, dos problemas críticos de gobernanza emergen:

| Problema | Consecuencia si no se controla |
|---|---|
| **Mensajes obsoletos (stale messages)** | Un servicio inactivo por horas acumula miles de mensajes en la cola. Al reiniciarse, los procesa todos aunque ya no sean válidos (órdenes canceladas, precios desactualizados), causando inconsistencias de datos. |
| **Colisión de Consumer Groups** | Dos equipos distintos con servicios llamados `order-service` comparten el mismo broker. Sus sidecars compiten por los mismos mensajes, provocando que cada equipo reciba solo el 50% de sus eventos. |

Dapr resuelve ambos problemas **de forma declarativa**, desde el Component YAML, sin modificar el código de la aplicación.

---

## 4.2 Reto 1 — Time-to-Live (TTL)

### El problema

Un mensaje publicado en el tópico `orders` con `status: created` tiene sentido procesarlo durante los próximos 2 minutos. Después de eso, el sistema de pagos ya marcó la orden como timeout y procesarla sería un error.

### La solución

Dapr permite configurar un **TTL (Time-to-Live)** en el componente Pub/Sub. Los mensajes más antiguos que el TTL son descartados automáticamente por el sidecar, nunca llegan a la aplicación.

### Documentación de referencia

- 📖 [Message TTL in Pub/Sub — Dapr docs](https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-message-ttl/)
- 📖 [Redis Pub/Sub Component metadata](https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-redis-pubsub/#spec-metadata-fields)

### TODO 1: Configurar TTL en el componente

Abre el archivo `04-reto-gobernanza/pubsub-redis-challenge.yaml`.

Agrega la entrada de metadata necesaria para que los mensajes expiren automáticamente después de **120 segundos**.

Una vez completado, aplica el componente:

```bash
kubectl apply -f 04-reto-gobernanza/pubsub-redis-challenge.yaml

# Verificar que el componente fue actualizado
kubectl get components.dapr.io order-pubsub -o yaml
```

**Prueba del TTL:** Publica un mensaje y espera más de 2 minutos antes de que el subscriber los procese.

```bash
# Escalar el subscriber a 0 para que no consuma mensajes
kubectl scale deployment order-subscriber --replicas=0

# Lanzar el publisher para publicar 3 mensajes
kubectl delete job order-publisher --ignore-not-found
kubectl apply -f 03-aplicaciones/publisher-job.yaml

# Esperar 130 segundos (más que el TTL de 120s)
sleep 130

# Restaurar el subscriber
kubectl scale deployment order-subscriber --replicas=1

# Observar los logs: ¿llegan los mensajes del publisher?
kubectl logs -l app=order-subscriber -c order-subscriber -f
```

---

## 4.3 Reto 2 — Multi-Tenant Namespaces

### El problema

Imagina una plataforma donde el equipo de `checkout` y el equipo de `inventory` tienen ambos un servicio llamado `order-subscriber`. Ambos están en el mismo clúster Kubernetes pero en distintos namespaces (`checkout` e `inventory`). Ambos se suscriben al mismo tópico `orders`.

Con una configuración estándar, Redis asigna a todos los suscriptores del mismo topic el **mismo consumer group**. El resultado: Redis distribuye los mensajes entre ambos equipos en round-robin, y cada equipo solo recibe ~50% de sus mensajes.

### La solución

Dapr permite parametrizar el `consumerID` (grupo de consumidores en Redis) usando el marcador `{namespace}`. Con esto, Dapr crea automáticamente un consumer group distinto por namespace de Kubernetes:

- `checkout` → consumer group: `order-subscriber-checkout`
- `inventory` → consumer group: `order-subscriber-inventory`

Cada equipo recibe **el 100% de sus mensajes**.

### Documentación de referencia

- 📖 [Namespaced components — Dapr docs](https://docs.dapr.io/operations/components/component-scopes/#namespaced-components)
- 📖 [Redis Streams consumer group](https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-redis-pubsub/#spec-metadata-fields)

### TODO 2: Configurar el Consumer Group con Namespace

En el mismo archivo `04-reto-gobernanza/pubsub-redis-challenge.yaml`, agrega la entrada de metadata para el `consumerID` usando el marcador de namespace dinámico de Dapr.

**Pista:** El valor del campo debe contener el marcador `{namespace}` que Dapr reemplazará en runtime por el namespace de Kubernetes donde corre la aplicación.

---

## 4.4 Reto 3 — Reflexión Arquitectónica

Responde en el archivo `04-reto-gobernanza/reflexion-arquitectonica.md`:

1. **Comparación de enfoques:** En una arquitectura sin Dapr usando el SDK de Kafka directamente, ¿dónde estaría el código que gestiona el TTL? ¿Y el consumer group? ¿Quién sería responsable de mantenerlo (el equipo de la app o el equipo de plataforma)?

2. **El patrón sidecar como frontera de responsabilidad:** La decisión de usar `ttlInSeconds=120` es una decisión de negocio (las órdenes expiran en 2 minutos) pero se implementa en la capa de infraestructura (el Component YAML). ¿Cómo distribuirías esta responsabilidad entre el equipo de producto (que conoce el SLA de expiración) y el equipo de plataforma (que gestiona el broker)?

3. **Limitaciones del patrón:** Dapr es agnóstico al broker, pero ¿significa eso que puedes migrar de Redis a Kafka sin *ningún* cambio? Investiga si hay diferencias entre los brokers en cuanto a soporte de TTL y consumer groups, y documenta al menos una limitación concreta de la abstracción.

---

## 4.5 Limpieza del entorno

Una vez completado el laboratorio:

```bash
# Eliminar todos los recursos del lab
kubectl delete -f 03-aplicaciones/subscriber-deployment.yaml
kubectl delete -f 03-aplicaciones/publisher-job.yaml            --ignore-not-found
kubectl delete -f 04-reto-gobernanza/pubsub-redis-challenge.yaml --ignore-not-found
kubectl delete -f 02-componentes-declarativos/pubsub-redis.yaml  --ignore-not-found

# Eliminar el clúster Kind completo
kind delete cluster --name dapr-lab
```
