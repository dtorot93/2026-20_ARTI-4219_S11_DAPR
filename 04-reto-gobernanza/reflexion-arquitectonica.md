# Reflexión Arquitectónica — Fase 4

## Resultados de la verificación de los retos

Antes de reflexionar dejo los resultados medidos en el clúster (Dapr 1.19.0 + Redis 8.10.3, chart Bitnami `redis-28.3.10`), porque varias de mis conclusiones salen de ellos y no solo de la documentación.

### Reto 1 — TTL de 120 s

Primero apliqué `ttlInSeconds: "120"` **solo en el Component** y ejecuté la prueba de la guía: subscriber escalado a 0, publicación de 3 órdenes, espera de 130 s y subscriber escalado de nuevo a 1. **La prueba falló: las 3 órdenes llegaron a la app.** Al inspeccionar Redis vi que el CloudEvent no tenía el atributo `expiration`.

Diseñé entonces un experimento controlado, publicando desde un Deployment de depuración con el subscriber apagado durante 130 s:

| Mensaje | Mecanismo de TTL | ¿Llegó a la app tras 130 s? |
|---|---|---|
| Control | `ttlInSeconds: "120"` + `streamTTL: "120s"` en el Component | **Sí** (no expiró) |
| Prueba | `?metadata.ttlInSeconds=60` en la llamada de publicación | **No** (descartado) |

Además, `XLEN` del stream no bajó después de 130 s. `streamTTL` recorta de forma **perezosa** (en el siguiente `XADD`) y **aproximada** (`MINID ~`), así que no garantiza nada por mensaje.

**Conclusión:** en Redis, Dapr implementa el TTL **en el runtime** y solo cuando viaja como metadata de la publicación. El sidecar publicador escribe `expiration` en el CloudEvent y el sidecar suscriptor descarta lo que llega tarde. Por eso mantuve `ttlInSeconds` en el componente (como declaración de la política que pide el reto) y lo hice efectivo con una variable `ORDER_TTL_SECONDS: "120"` en el `publisher-job.yaml`, que el publisher envía como `metadata.ttlInSeconds`. Con eso, la prueba de la guía pasó:

```text
Redis: {"expiration":"2026-10-29T00:25:57Z", "id":"f24ed865-…", ...}   metadata: {"ttlInSeconds":"120"}
daprd: level=warning msg="dropping expired pub/sub event f24ed865-9904-4fc1-9e8d-a6d2b2075a25 as of 2026-10-29T00:25:57Z"
App:   0 CloudEvents recibidos tras 130 s   |   control con subscriber activo: 3/3 recibidos
```

### Reto 2 — Consumer group por namespace

Con `consumerID: "{appID}-{namespace}"` ejecuté `demo-multitenant.sh`, que despliega el mismo `order-subscriber` en los namespaces `checkout` e `inventory` contra el mismo Redis:

| Escenario | checkout publica 3 | inventory publica 3 | Grupos en Redis |
|---|---|---|---|
| **A** — sin `consumerID` | checkout: **2**, inventory: **1** (colisión) | — | `orders` → `order-subscriber` (compartido) |
| **B** — `{appID}-{namespace}` | checkout: **3**, inventory: **0** | checkout: **3**, inventory: **3** | `checkoutorders` → `order-subscriber-checkout.order-subscriber`<br>`inventoryorders` → `order-subscriber-inventory.order-subscriber` |

El escenario A reproduce exactamente el problema que plantea el reto: los mensajes se reparten entre equipos. El escenario B lo resuelve, pero con un matiz importante que no menciona la guía. Según la documentación de *Namespace consumer groups*, cuando `{namespace}` aparece en la metadata Dapr **aísla el componente completo**: no solo cambia el consumer group, también **prefija el tópico** con el namespace (`checkoutorders`, `inventoryorders`). Por lo tanto, cada equipo recibe el 100% de **sus** eventos, pero un publicador en `checkout` ya no alcanza a los suscriptores de `inventory`. Si el requisito fuera que ambos equipos consumieran **el mismo** flujo de órdenes de forma independiente (fan-out), la solución correcta sería no usar `{namespace}` y darle a cada equipo un `consumerID` literal distinto en su propio componente.

---

## Reto 3: Reflexión Arquitectónica

### 1. Sin Dapr: ¿dónde vive el código de TTL y consumer group?

Con el SDK de Kafka usado directamente, ambas responsabilidades quedarían **dentro del código de cada microservicio**:

- **Consumer group:** sería la propiedad `group.id` de la configuración del `KafkaConsumer`, escrita en el código o inyectada por variable de entorno pero **interpretada por la app**. Cada equipo tendría que conocer la convención de nombres (incluir el namespace o el tenant) y aplicarla correctamente. Un error de copiar y pegar (dos equipos con `group.id=order-service`) reproduce silenciosamente el problema del 50% de los mensajes, y nadie de plataforma podría prevenirlo salvo con revisión manual.
- **TTL:** Kafka **no tiene TTL por mensaje**. `retention.ms` es una política de almacenamiento del tópico (borra segmentos completos tarde o temprano) y no garantiza que un consumidor no procese un mensaje viejo. El TTL de negocio tendría que implementarse en el consumidor: leer el timestamp o un header de expiración de cada `ConsumerRecord`, compararlo con el reloj y descartar el mensaje (haciendo commit del offset para no reprocesarlo). Habría que escribirlo, probarlo y mantenerlo **en cada servicio y en cada lenguaje** (Python, Java, Go…), con riesgo de implementaciones inconsistentes.

**¿Quién lo mantiene?** El **equipo de la aplicación**, por definición, porque el código está en su repositorio. El equipo de plataforma solo operaría el clúster de Kafka y quedaría limitado a publicar guías y a esperar que se cumplan. Además, actualizar el SDK, migrar de broker o cambiar la convención de grupos se convierte en un cambio coordinado en N repositorios. Con Dapr, en cambio, verifiqué que el consumer group se gobierna desde un YAML y el descarte por TTL lo ejecuta el sidecar, que es una pieza que versiona y opera la plataforma.

### 2. El patrón sidecar como frontera de responsabilidad

Hay que separar **el valor** (una decisión de negocio) del **mecanismo** (una capacidad de plataforma). Mi propuesta de distribución es esta:

| Responsabilidad | Dueño | Dónde vive |
|---|---|---|
| Definir que una orden expira a los 120 s (el SLA) | **Equipo de producto** | Su manifiesto de despliegue: `ORDER_TTL_SECONDS: "120"` en el Job/Deployment, o un `values.yaml` de su chart |
| Proveer el mecanismo de expiración y garantizar que funcione con el broker | **Equipo de plataforma** | Versión de Dapr, Component YAML, pruebas de conformidad por broker |
| Definir límites y defaults (TTL máximo permitido, TTL por defecto si el equipo no define uno, formato de `consumerID`) | **Plataforma**, acordado con arquitectura | Políticas como código (Kyverno/OPA Gatekeeper) que validan los manifiestos en admisión o en CI |
| Operar el broker (capacidad, retención, seguridad) | **Plataforma** | IaC del broker, secret stores |

Para que la frontera funcione en el día a día, la implementaría así:

1. **Golden path / plantilla:** plataforma publica un chart o plantilla (por ejemplo, en Backstage) donde el equipo de producto solo llena parámetros de negocio (`messageTTL`, `topic`) y el chart genera el Component, las anotaciones y la Configuration correctos. El equipo de producto declara *qué* necesita y nunca escribe metadata específica de Redis.
2. **CODEOWNERS en GitOps:** los Components y Configurations quedan bajo revisión obligatoria de plataforma; las variables de SLA, bajo revisión del producto. Cada cambio pasa por el dueño correcto.
3. **Guardrails automáticos:** una política rechaza un TTL mayor que la retención real del broker, o un `consumerID` sin `{namespace}` en clústeres multi-tenant.
4. **Contratos verificables:** la prueba de TTL que ejecuté (subscriber a 0, espera mayor al TTL y verificación de 0 entregas) se convierte en un test de conformidad del pipeline de plataforma. El hallazgo de este lab muestra por qué es necesario: un YAML que *parece* correcto (`ttlInSeconds` en el componente) **no** producía el comportamiento esperado.

Mi solución del Reto 1 ya refleja esta división. El valor de negocio está en el manifiesto del publisher, que pertenece al equipo de producto, y el mecanismo (`metadata.ttlInSeconds`, `expiration`, descarte en el sidecar) lo provee la plataforma. El código no menciona Redis.

### 3. Limitaciones de la abstracción: ¿migración 100% transparente?

No. Dapr hace la migración transparente **para el código**, pero no para la **semántica operativa**. Documento las limitaciones concretas, empezando por las que comprobé en este lab:

1. **El TTL no se comporta igual en todos los brokers ni en todos los niveles (lo verifiqué).** En Redis, `ttlInSeconds` a nivel de componente no tuvo efecto y solo funcionó como metadata por mensaje, aplicado por el runtime. La documentación de Dapr indica que algunos brokers como **Azure Service Bus** sí tienen TTL nativo a nivel de entidad y que Dapr delega la expiración en ellos, mientras que en el resto la aplica el sidecar. Las consecuencias son prácticas:
   - Con TTL aplicado por el runtime, el mensaje expirado **igual ocupa espacio en el broker** y se lee para luego descartarse; en Redis la limpieza física depende de `streamTTL`/`maxLenApprox`, con semántica distinta.
   - El parámetro de TTL a nivel de entidad tiene **nombres distintos** por componente (por ejemplo, `defaultMessageTimeToLiveInSec` en Service Bus frente a `streamTTL` en Redis). Migrar de broker exige reescribir esa metadata y volver a validar el comportamiento.
2. **El consumer group cambia de nombre y de significado.** En Redis el campo es `consumerID` (grupo de un Stream); en Kafka es `consumerGroup` (grupo con *offsets* por partición); en Azure Service Bus el `consumerID` se convierte en el nombre de una *subscription* (con límite de longitud); en RabbitMQ determina el nombre de la cola. Un mismo template (`{appID}-{namespace}`) produce entidades diferentes en cada broker.
3. **`{namespace}` tiene efectos laterales que no son evidentes (lo verifiqué).** No solo separa consumer groups: también prefija el tópico (`checkoutorders`). Si un equipo asumía que seguía compartiendo el tópico `orders` con otros namespaces, la migración a esta configuración corta silenciosamente ese flujo. Además, el stream antiguo `orders` y su grupo quedaron huérfanos en Redis y había que limpiarlos manualmente.
4. **Garantías de orden y particionamiento.** Kafka garantiza orden por partición usando una clave (`partitionKey` en la metadata de publicación); Redis Streams con varias réplicas consumidoras en el mismo grupo no garantiza orden por entidad; Service Bus lo logra con *sessions*. Un flujo que depende del orden (por ejemplo, `created → processing → shipped` de la misma orden) puede funcionar con un broker y romperse con otro sin que cambie una línea de código.
5. **Capacidades exclusivas de un broker.** Funcionalidades como el *replay* desde un offset en Kafka, la compactación de tópicos o las colas de prioridad en RabbitMQ no tienen equivalente en la API común de Dapr. Si una app llega a depender de ellas (vía metadata específica), queda de nuevo acoplada al broker.

En síntesis, Dapr mueve el acoplamiento **del código al YAML**. Esa sigue siendo una gran ganancia, porque el YAML es declarativo, revisable y gobernable por plataforma, pero la portabilidad real exige que plataforma mantenga una **matriz de compatibilidad por broker** y **pruebas de conformidad** (como las de TTL y multi-tenancy de este lab) que se ejecuten antes de cualquier migración.
