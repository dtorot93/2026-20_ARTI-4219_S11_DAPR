import requests
import logging
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

    response = requests.post(
        url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=5,
    )

    # Dapr retorna HTTP 204 (No Content) en publicaciones exitosas
    if response.status_code == 204:
        logging.info("✅ Orden publicada — orderId=%s  status=%s", order_id, status)
    else:
        logging.error(
            "❌ Error al publicar — orderId=%s  HTTP %s: %s",
            order_id, response.status_code, response.text
        )


if __name__ == '__main__':
    # Esperar a que el sidecar de Dapr esté listo antes de publicar
    logging.info("Esperando 5 segundos para que el sidecar de Dapr esté listo...")
    time.sleep(5)

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
