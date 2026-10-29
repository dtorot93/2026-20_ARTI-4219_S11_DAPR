# Análisis de Trazabilidad (CloudEvents) — Fase 3

## TODO: Análisis de Trazabilidad

### 1. El atributo `id`: ¿quién lo genera y para qué sirve?

Lo genera el **sidecar `daprd` del publisher** en el momento de la publicación: es un UUID v4 que el runtime crea cuando la aplicación no provee un `id` propio. El código Python no tiene ninguna línea que lo produzca. Comprobé que el mismo `id` aparece en el stream de Redis y en el log del subscriber, es decir, **viaja inalterado de extremo a extremo**.

Es la base de la **idempotencia** por la semántica de entrega de Dapr Pub/Sub, que es **at-least-once**. El sidecar del subscriber solo confirma el mensaje en Redis (`XACK`) cuando la app responde `200 {"status":"SUCCESS"}`. Si la app falla, tarda demasiado o el Pod muere después de procesar pero antes del ACK, Redis vuelve a entregar el **mismo** mensaje (Dapr reclama los pendientes según `redeliverInterval`/`processingTimeout`). Por tanto el consumidor **va a recibir duplicados** tarde o temprano, y el `id` es la clave natural para detectarlos:

- Guardo los `id` ya procesados en un state store con escritura condicional (*first-write-wins* o ETag), o aplico el patrón **Idempotent Consumer / Inbox**.
- Si el `id` ya existe, respondo `SUCCESS` sin reprocesar, de modo que el broker deje de reintentar y el efecto de negocio ocurra una sola vez.

Sin un identificador estable generado **en el origen** esto no sería posible, porque dos entregas del mismo evento serían indistinguibles de dos eventos distintos con el mismo payload.

### 2. El atributo `source`: ¿qué valor tiene y de dónde lo toma Dapr?

El valor es **`order-publisher`**. Dapr lo toma del **`app-id` del sidecar que publica**, es decir, de la anotación `dapr.io/app-id: "order-publisher"` del `publisher-job.yaml`. No viene del tópico, del nombre del Pod ni del código.

Lo confirmé con un contraexperimento: al publicar desde un Deployment de depuración con `dapr.io/app-id: "ttl-debug"`, el CloudEvent llegó con `"source": "ttl-debug"`. Si en algún caso se necesitara otro valor (por ejemplo, una URI de dominio), Dapr permite sobrescribirlo con la metadata `cloudevent.source` al publicar.

Desde la perspectiva de plataforma, esto significa que `source` es una **identidad verificada por la infraestructura**. El sidecar obtuvo su identidad de Sentry (mTLS/SPIFFE) a partir del `app-id`, así que el atributo no es un texto libre que cualquier desarrollador pueda falsificar desde su código.

### 3. El atributo `traceparent` y su relación con OpenTelemetry

`traceparent` implementa el estándar **W3C Trace Context** (Recomendación W3C, nivel 1). Su formato es `version-traceid-parentid-flags`. Por ejemplo, para `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`:

| Segmento | Valor | Significado |
|---|---|---|
| `version` | `00` | Versión del formato |
| `trace-id` | `4bf92f3577b34da6a3ce929d0e0e4736` | 16 bytes. Identifica **toda la transacción distribuida** |
| `parent-id` | `00f067aa0ba902b7` | 8 bytes. Identifica el *span* que publicó el mensaje |
| `trace-flags` | `01` | Bit *sampled* activo: la traza se está registrando |

Viene acompañado de `tracestate`, para metadata propia de cada vendor, y de `traceid`, un campo heredado que Dapr mantiene por compatibilidad.

**Relación con OpenTelemetry:** W3C Trace Context es el **propagador por defecto de OpenTelemetry**. Esto significa que:

- Dapr crea los *spans* de publicación y de entrega y los exporta con OTLP (o Zipkin) a cualquier backend compatible con OTel: Jaeger, Tempo, Grafana, Datadog, etc.
- Como el contexto viaja **dentro** del mensaje, la traza no se rompe al cruzar el broker, que es justo el punto donde la mensajería asíncrona suele perder la correlación. El sidecar del subscriber continúa el mismo `trace-id`, y la app puede leer ese contexto para crear *spans* hijos con el SDK de OpenTelemetry.

En el lab el campo llegó **vacío**: con la configuración por defecto el sidecar no muestrea trazas, y la propagación se activa de forma declarativa con una `Configuration` de tracing (`samplingRate`), sin tocar el código de ninguna aplicación.

### 4. Valor de negocio para auditoría: atributos más útiles del sobre CloudEvents

Para que un equipo de auditoría reconstruya **todo lo que pasó por el sistema en las últimas 24 h**, priorizaría estos atributos:

| Atributo | Pregunta de auditoría que responde |
|---|---|
| `time` | **¿Cuándo?** Permite filtrar exactamente la ventana de 24 h y ordenar cronológicamente. Es la marca del momento de publicación, no de consumo. |
| `id` | **¿Cuántos y cuáles?** Identificador único para contar eventos sin duplicados (deduplicando las re-entregas at-least-once) y para citar un evento puntual en un hallazgo. |
| `source` | **¿Quién lo emitió?** Identidad del servicio publicador, respaldada por la identidad mTLS del sidecar. Clave para atribuir responsabilidad. |
| `type` | **¿Qué tipo de hecho es?** Permite clasificar los eventos. En producción lo personalizaría (p. ej. `com.empresa.orders.created`) en lugar del genérico `com.dapr.event.sent`. |
| `pubsubname` + `topic` | **¿Por qué canal pasó?** Identifica el broker lógico y el flujo de negocio, útil cuando hay varios tópicos y componentes. |
| `traceparent` / `traceid` | **¿Qué más ocurrió en esa misma transacción?** Correlaciona el evento con la traza completa (la petición HTTP original, las llamadas a otros servicios y los consumidores) para reconstruir el flujo extremo a extremo. |
| `expiration` | **¿Se descartó algo por política?** Cuando hay TTL, permite distinguir un evento expirado de uno perdido. |

Como el sobre es un **estándar CNCF**, el equipo de auditoría puede construir una sola herramienta que funcione para todos los tópicos y equipos, sin acoplarse al payload de negocio de cada servicio.

Hay una advertencia arquitectónica: **el broker no es un log de auditoría.** Redis Streams puede recortarse (`maxLenApprox`, `streamTTL`) y no es inmutable. Para auditoría real agregaría un suscriptor dedicado de auditoría con su propio consumer group, que reciba el 100% de los eventos y los persista en un almacenamiento *append-only* (por ejemplo, object storage con *object lock* o un data lake), indexado por `time`, `source`, `type` e `id`. Si se requiere **no repudio**, también firmaría los eventos, porque CloudEvents por sí solo no garantiza la integridad del contenido.
