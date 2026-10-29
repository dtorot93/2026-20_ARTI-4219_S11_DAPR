# Análisis del Plano de Control de Dapr — Fase 1

## TODO 1: Análisis del Plano de Control

### 1. ¿Qué tipo de objeto de Kubernetes es el `dapr-sidecar-injector`?

El `dapr-sidecar-injector` se compone de dos objetos complementarios:

1. **La carga de trabajo:** un `Deployment` (el pod está *Controlled By: ReplicaSet/dapr-sidecar-injector-8b9bff8d5*) expuesto por un `Service` en el puerto 443. Es un servidor HTTPS que atiende la ruta `/mutate`.
2. **El registro en la API de Kubernetes:** un objeto cluster-scoped **`MutatingWebhookConfiguration`** llamado `dapr-sidecar-injector`. Le indica al API Server que, antes de persistir cualquier Pod, debe consultar ese servicio:

```yaml
webhooks:
- name: sidecar-injector.dapr.io
  clientConfig:
    service: {name: dapr-sidecar-injector, namespace: dapr-system, path: /mutate, port: 443}
  rules:
  - operations: [CREATE]
    resources: [pods]
  failurePolicy: Ignore
  reinvocationPolicy: IfNeeded
  sideEffects: None
```

**¿Por qué debe ser *Mutating* y no *Validating*?** Un `ValidatingWebhook` solo puede aceptar o rechazar un objeto, mientras que la inyección necesita **modificar** el `PodSpec` antes de que llegue a etcd. El injector responde al `AdmissionReview` con un JSON Patch que agrega el contenedor `daprd` con sus puertos (3500 HTTP, 50001 gRPC, 50002 gRPC interno entre sidecars y 9090 métricas), el volumen proyectado `dapr-identity-token` y la variable `DAPR_TRUST_ANCHORS` con el *trust bundle* de Sentry. Lo verifiqué inspeccionando el `spec` del Pod ya mutado. Como el desarrollador nunca escribe ese contenedor en su manifiesto, la única manera transparente de agregarlo es mutar el objeto en la fase de admisión.

### 2. Réplicas del `dapr-operator` y comparación con Istio

Está corriendo **1 réplica** del `dapr-operator` (`deployment.apps/dapr-operator 1/1`). Es el valor por defecto del chart. En producción habilitaría `--set global.ha.enabled=true`, que sube a 3 réplicas los componentes sin estado (operator, sentry, injector) y usa *leader election* en el operator.

El plano de control de Dapr es mucho más liviano que el de Istio porque **resuelve un problema distinto**:

| Dimensión | Istio | Dapr |
|---|---|---|
| Qué intercepta | **Todo** el tráfico L4/L7 del Pod (iptables redirige entradas y salidas a Envoy) | Nada. La app llama **explícitamente** a `localhost:3500` solo cuando quiere usar un building block |
| Qué distribuye el control plane | Configuración xDS completa de la malla (listeners, routes, clusters y endpoints de **todos** los servicios), que se recalcula y empuja a cada Envoy con cada cambio en el clúster | Solo los `Component`, `Configuration`, `Subscription` y `Resiliency` del namespace o app-id, que el sidecar descarga al arrancar (con hot reload) |
| Estado que mantiene | Vista global de la topología de red | Prácticamente ninguno. El operator reconcilia CRDs y Sentry emite certificados |
| Costo por sidecar | Envoy completo en el *data path* de cada request | `daprd` solo participa cuando la app invoca su API |

Dicho de otro modo, Istio es una **malla de red** (*service mesh*), cuyo control plane tiene que conocer el grafo completo de servicios. Dapr es un **runtime de aplicación** (*application runtime*): cada sidecar es relativamente autónomo, y el control plane solo actúa como distribuidor de configuración declarativa y como CA.

### 3. Flujo de inyección del sidecar

Reconstruí el flujo con los logs del injector y del `daprd` del subscriber:

1. **`kubectl apply`** envía el `Deployment` al **API Server**, que lo autentica, autoriza y persiste en etcd. En ese momento todavía no hay Pods.
2. El **Deployment controller** crea un `ReplicaSet` y el **ReplicaSet controller** solicita la creación del `Pod` usando la service account `system:serviceaccount:kube-system:replicaset-controller`.
3. En la fase de **mutating admission**, el API Server encuentra la regla `CREATE pods` de la `MutatingWebhookConfiguration` y envía un `AdmissionReview` por HTTPS al servicio `dapr-sidecar-injector/mutate`. En el log del injector se ve: `AdmissionReview for Kind=/v1, Kind=Pod, Namespace=default ... UserInfo={system:serviceaccount:kube-system:replicaset-controller ...}`.
4. El injector verifica que exista la anotación `dapr.io/enabled: "true"` y que el solicitante sea un controlador permitido. Luego lee el resto de anotaciones (`app-id`, `app-port`) y devuelve un **JSON Patch** con el contenedor `daprd` (`ghcr.io/dapr/daprd:1.19.0`) y sus volúmenes y variables. En el log aparece `Sidecar injector succeeded injection for app 'order-subscriber'`.
5. El API Server aplica el patch, ejecuta los webhooks de validación y persiste el Pod ya mutado. El **scheduler** lo asigna a `dapr-lab-control-plane` y el **kubelet** arranca los dos contenedores, por eso `READY 2/2`.
6. Al arrancar, `daprd` hace su propio *bootstrap* contra el plano de control:
   - `Fetching initial identity` → `Security is initialized successfully`: obtiene de **Sentry** su certificado mTLS a partir del token de la service account.
   - `Initializing the operator client`: descarga del **operator** los `Component` (`order-pubsub`).
   - Se conecta a **placement** y **scheduler** para actores y jobs.
   - `application discovered on port 5001`: espera a que Flask escuche en el `app-port`.
   - Hace `GET /dapr/subscribe` a la app (se ve en el log de Flask) y registra la suscripción: `app is subscribed to the following topics: [[orders]] through pubsub=order-pubsub`.
   - `dapr initialized. Status: Running`.

---

## TODO 2: Exploración de los CRDs de Dapr

### 4. CRDs instalados por Dapr

`kubectl get crds | grep dapr.io` devolvió **7 CRDs** en Dapr 1.19:

| CRD | Para qué sirve |
|---|---|
| `components.dapr.io` | Declara una **implementación concreta** de un building block (p. ej. `pubsub.redis`, `state.postgresql`, `secretstores.kubernetes`) con su metadata de conexión. Es el CRD central de este lab. |
| `configurations.dapr.io` | Configura el comportamiento del sidecar: tracing, métricas, mTLS, middlewares HTTP, *feature flags* y políticas de acceso. Se asocia a una app con la anotación `dapr.io/config`. |
| `subscriptions.dapr.io` | Permite declarar suscripciones (pubsub + topic + ruta) de forma **declarativa** en YAML, en lugar de hacerlo programáticamente con `GET /dapr/subscribe` como hace nuestro subscriber. Incluye reglas de enrutamiento por contenido y *dead-letter topics*. |
| `resiliencies.dapr.io` | Define políticas de resiliencia (*retries*, *timeouts*, *circuit breakers*) aplicables a apps, componentes y actores sin tocar código. |
| `httpendpoints.dapr.io` | Registra endpoints HTTP **externos** (fuera de Dapr) para invocarlos con la API de service invocation, con headers y TLS configurados de forma declarativa. |
| `workflowaccesspolicies.dapr.io` | Controla qué apps pueden invocar o interactuar con los workflows de otra app. Es gobernanza del building block de Workflows. |
| `mcpservers.dapr.io` | Recurso reciente (`kubectl explain` lo describe como *"a connection to an MCP (Model Context Protocol) server for durable tool execution"*). Permite que agentes de IA y workflows de Dapr usen herramientas MCP con la durabilidad del runtime. |

### 5. Relación entre `components.dapr.io` y el concepto de Building Block

El **building block es la abstracción** y el **componente es la implementación concreta**, o más precisamente, la *instancia configurada* de una implementación.

- El building block *Pub/Sub* es un **contrato de API** que el runtime expone siempre igual (`POST /v1.0/publish/{pubsubname}/{topic}`, `GET /dapr/subscribe`, CloudEvents). El código del publisher y del subscriber está escrito contra ese contrato.
- Un objeto `Component` dice **qué tecnología respalda ese contrato y cómo conectarse a ella**. El prefijo de `spec.type` identifica el building block (`pubsub.`) y el sufijo la implementación (`redis`); `spec.metadata` contiene los parámetros propios de esa tecnología.
- El punto de unión es **`metadata.name`** (`order-pubsub`). El código solo conoce ese nombre lógico, nunca el tipo. Por eso puedo tener varios componentes Pub/Sub en un mismo namespace (uno Redis y otro Kafka, por ejemplo) y cada app elige cuál usar por nombre.

En términos de arquitectura, es el patrón **Puerto y Adaptador** (arquitectura hexagonal) llevado a la plataforma: el building block es el *puerto*, el código de `components-contrib` es el *adaptador* y el CRD `Component` es el *wiring* declarativo que conecta ambos en tiempo de despliegue.
