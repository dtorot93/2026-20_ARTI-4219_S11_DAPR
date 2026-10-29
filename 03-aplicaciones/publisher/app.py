import requests
import logging
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------------------------------------------------
# El sidecar de Dapr expone una API HTTP local en este puerto.
# La aplicación nunca habla directamente con Redis: solo usa este endpoint.
# ---------------------------------------------------------------------------
DAPR_HTTP_PORT = 3500
PUBSUB_NAME    = "order-pubsub"   # Debe coincidir con metadata.name del Component YAML
TOPIC_NAME     = "orders"

# ---------------------------------------------------------------------------
# TTL por mensaje (Fase 4 — Reto 1)
# ---------------------------------------------------------------------------
# Lo leo de una variable de entorno que se define en el manifiesto del Job, de
# modo que la política de expiración siga siendo declarativa y propiedad de
# quien despliega, no del código. La envío como metadata de la API de Dapr
# (`metadata.ttlInSeconds`), que es un concepto del building block Pub/Sub y
# no de Redis: el código sigue siendo agnóstico al broker.
#
# Por qué no basta con el Component YAML: verifiqué en Dapr 1.19 con Redis que
# `ttlInSeconds` a nivel de componente no agrega el atributo `expiration` al
# CloudEvent y los mensajes viejos se entregan igual. En cambio, con la metadata
# por mensaje el sidecar del subscriber sí los descarta.
# Referencia: https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-message-ttl/
# ---------------------------------------------------------------------------
ORDER_TTL_SECONDS = os.getenv("ORDER_TTL_SECONDS")


def publish_order(order_id: str, status: str) -> None:
    """
    Publica un evento de orden en el tópico 'orders' a través del sidecar de Dapr.

    El sidecar intercepta la llamada HTTP, serializa el payload, lo envuelve
    en un sobre CloudEvents 1.0 y lo entrega al broker (Redis) configurado
    en el Component YAML — sin que esta función lo sepa ni le importe.

    Referencia: https://docs.dapr.io/reference/api/pubsub_api/#publish-a-message-to-a-given-topic
    """
    url = f"http://localhost:{DAPR_HTTP_PORT}/v1.0/publish/{PUBSUB_NAME}/{TOPIC_NAME}"

    payload = {
        "orderId": order_id,
        "status":  status,
    }

    params = {"metadata.ttlInSeconds": ORDER_TTL_SECONDS} if ORDER_TTL_SECONDS else None

    response = requests.post(
        url,
        params=params,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=5,
    )

    # Dapr retorna HTTP 204 (No Content) en publicaciones exitosas
    if response.status_code == 204:
        logging.info("✅ Orden publicada — orderId=%s  status=%s  ttl=%ss",
                     order_id, status, ORDER_TTL_SECONDS or "∞")
    else:
        logging.error(
            "❌ Error al publicar — orderId=%s  HTTP %s: %s",
            order_id, response.status_code, response.text
        )


def wait_for_sidecar(max_attempts: int = 30) -> None:
    """
    Espera a que el sidecar esté listo para llamadas salientes.

    Durante las pruebas noté que un `sleep` fijo es frágil: si el sidecar tarda
    más en cargar el componente, la primera publicación falla. Por eso consulto
    /v1.0/healthz/outbound, que responde 204 solo cuando los componentes ya
    están inicializados.

    Referencia: https://docs.dapr.io/reference/api/health_api/
    """
    url = f"http://localhost:{DAPR_HTTP_PORT}/v1.0/healthz/outbound"
    for _ in range(max_attempts):
        try:
            if requests.get(url, timeout=1).status_code == 204:
                logging.info("Sidecar de Dapr listo.")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    logging.warning("El sidecar no reportó estar listo; intento publicar de todas formas.")


def shutdown_sidecar() -> None:
    """
    Le pide al sidecar que termine.

    En un Job, el Pod solo pasa a `Completed` cuando todos sus contenedores
    terminan. Si no apago `daprd` explícitamente, el Job queda en `Running`
    indefinidamente aunque el script ya haya terminado (lo verifiqué en el lab:
    el Pod quedaba en 1/2 NotReady).

    Referencia: https://docs.dapr.io/reference/api/shutdown_api/
    """
    try:
        requests.post(f"http://localhost:{DAPR_HTTP_PORT}/v1.0/shutdown", timeout=5)
        logging.info("Solicitud de apagado enviada al sidecar de Dapr.")
    except requests.exceptions.RequestException as exc:
        logging.warning("No pude apagar el sidecar: %s", exc)


if __name__ == '__main__':
    # Esperar a que el sidecar de Dapr esté listo antes de publicar
    logging.info("Esperando a que el sidecar de Dapr esté listo...")
    wait_for_sidecar()

    # Publicar 3 órdenes de prueba
    orders = [
        {"orderId": "1001", "status": "created"},
        {"orderId": "1002", "status": "processing"},
        {"orderId": "1003", "status": "shipped"},
    ]

    for order in orders:
        publish_order(order["orderId"], order["status"])
        time.sleep(1)

    logging.info("Publicación completada.")
    shutdown_sidecar()
