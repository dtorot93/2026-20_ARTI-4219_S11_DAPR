# Fase 3: Aplicaciones Pub/Sub — Subscriber y Publisher

## 3.1 Arquitectura de la PoC

En Kubernetes con Dapr, cada Pod tiene **dos contenedores**:

```
┌─────────────────────────────────────────────┐
│               Pod: order-subscriber          │
│                                             │
│  ┌─────────────────┐   ┌─────────────────┐ │
│  │  app (Python)   │◄──│  daprd sidecar  │ │
│  │  puerto: 5001   │   │  puerto: 3500   │◄├──── Redis (broker)
│  └─────────────────┘   └─────────────────┘ │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│               Pod: order-publisher (Job)     │
│                                             │
│  ┌─────────────────┐   ┌─────────────────┐ │
│  │  app (Python)   │──►│  daprd sidecar  │ │
│  │                 │   │  puerto: 3500   │─├──► Redis (broker)
│  └─────────────────┘   └─────────────────┘ │
└─────────────────────────────────────────────┘
```

El código de la aplicación **nunca conoce la existencia de Redis**. Se comunica únicamente con el sidecar a través de HTTP local (`localhost:3500`).

---

## 3.2 Estructura de archivos

```
03-aplicaciones/
├── subscriber/
│   ├── app.py             ← Flask app (servidor de suscripciones, ya implementada)
│   ├── requirements.txt
│   └── Dockerfile
├── publisher/
│   ├── app.py             ← Script de publicación de eventos, ya implementado)
│   ├── requirements.txt
│   └── Dockerfile
├── subscriber-deployment.yaml   ← Deployment K8s (requiere completar anotaciones Dapr)
└── publisher-job.yaml           ← Job K8s (completo, usar como referencia)
```

---

## 3.3 Construir y cargar las imágenes en Kind

Kind no tiene acceso al registry local de Docker por defecto. Debes construir las imágenes y cargarlas explícitamente en el clúster.

```bash
# Posicionarte en el directorio de aplicaciones
cd 03-aplicaciones

# Construir la imagen del subscriber
docker build -t order-subscriber:latest ./subscriber

# Construir la imagen del publisher
docker build -t order-publisher:latest ./publisher

# Cargar las imágenes en el clúster Kind
kind load docker-image order-subscriber:latest --name dapr-lab
kind load docker-image order-publisher:latest --name dapr-lab

# Verificar que las imágenes están disponibles en el nodo
docker exec -it dapr-lab-control-plane crictl images | grep order
```

---

## 3.4 Desplegar el Subscriber

> **Importante:** Completa el TODO 1 en `subscriber-deployment.yaml` antes de aplicarlo.

```bash
# Aplicar el Deployment del subscriber (desde la raíz del lab)
kubectl apply -f 03-aplicaciones/subscriber-deployment.yaml

# Verificar que el Pod se creó con DOS contenedores (app + sidecar)
kubectl get pods -l app=order-subscriber

# La columna READY debe mostrar 2/2 cuando ambos contenedores estén listos
# Puede tardar 30-60 segundos en la primera vez
```

```bash
# Inspeccionar los contenedores del Pod para confirmar la inyección del sidecar
kubectl describe pod -l app=order-subscriber | grep -A5 "Containers:"
```

Deberías ver **dos contenedores**: `order-subscriber` y `daprd`.

```bash
# Ver los logs del sidecar de Dapr para confirmar que se suscribió al tópico
kubectl logs -l app=order-subscriber -c daprd | grep -i "subscribe\|order"
```

---

## 3.5 Ejecutar el Publisher

```bash
# Lanzar el Job del publisher
kubectl apply -f 03-aplicaciones/publisher-job.yaml

# Observar el estado del Job
kubectl get jobs
kubectl get pods -l app=order-publisher

# Ver los logs del publisher (esperar a que el Pod esté Running/Completed)
kubectl logs -l app=order-publisher -c order-publisher
```

---

## 3.6 Observar los CloudEvents en el Subscriber

Esta es la parte central del laboratorio. Observa cómo Dapr **envolvió automáticamente** el payload original en un sobre CloudEvents 1.0.

```bash
# Seguir los logs del subscriber en tiempo real
kubectl logs -l app=order-subscriber -c order-subscriber -f
```

Deberías ver el sobre CloudEvents con sus atributos estructurales:

**Ninguna línea del código del publisher generó estos atributos.** El sidecar los inyecta automáticamente.

---

## 3.7 Actividades TODO

### TODO 1: Análisis de trazabilidad (CloudEvents)

Después de observar los logs del subscriber con el flujo completo, responde en `03-aplicaciones/analisis-cloudevents.md`:

1. **`id`**: ¿Quién genera este UUID? ¿Por qué es importante para la idempotencia?
2. **`source`**: ¿Qué valor tiene? ¿De dónde lo toma Dapr?
3. **`traceparent`**: ¿Qué estándar implementa este atributo? ¿Cómo se relaciona con OpenTelemetry?
4. **Valor de negocio**: Un equipo de auditoría necesita rastrear todos los eventos que pasaron por el sistema en las últimas 24h. ¿Qué atributos del sobre CloudEvents les serían más útiles y por qué?

---

*Continúa con → [Fase 4: Reto de Gobernanza](../04-reto-gobernanza/README.md)*
