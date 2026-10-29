#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Demo del Reto 2 — Multi-Tenant Namespaces
# ---------------------------------------------------------------------------
# Escribí este script para comprobar empíricamente (y no solo en teoría) el
# efecto del marcador {namespace} en el consumerID. Simulo dos equipos,
# `checkout` e `inventory`, que despliegan el MISMO servicio `order-subscriber`
# contra el MISMO Redis y el MISMO tópico `orders`.
#
#   Escenario A (baseline): componente sin consumerID → ambos namespaces caen
#                           en el consumer group `order-subscriber` y se
#                           reparten los mensajes (colisión).
#   Escenario B (reto):     componente de pubsub-redis-challenge.yaml con
#                           consumerID "{appID}-{namespace}" → cada equipo
#                           queda aislado y recibe el 100% de sus eventos.
#
# Uso (desde la raíz del repositorio):
#   ./04-reto-gobernanza/demo-multitenant.sh A
#   ./04-reto-gobernanza/demo-multitenant.sh B
#   ./04-reto-gobernanza/demo-multitenant.sh clean
# ---------------------------------------------------------------------------
set -euo pipefail

TEAMS=(checkout inventory)
SCENARIO="${1:-B}"

# Reutilizo los manifiestos del lab cambiando solo el namespace, para que la
# prueba refleje exactamente lo que cada equipo desplegaría.
in_ns() { sed "s/namespace: default/namespace: $1/"; }

deploy_team() {
  local ns=$1 component=$2
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
  in_ns "$ns" < "$component"                                  | kubectl apply -f -
  in_ns "$ns" < 03-aplicaciones/tracing-config.yaml           | kubectl apply -f -
  in_ns "$ns" < 03-aplicaciones/subscriber-deployment.yaml    | kubectl apply -f -
}

publish_from() {
  local ns=$1
  kubectl delete job order-publisher -n "$ns" --ignore-not-found --wait
  in_ns "$ns" < 03-aplicaciones/publisher-job.yaml | kubectl apply -f -
  kubectl wait --for=condition=complete job/order-publisher -n "$ns" --timeout=120s
}

count_received() {
  local ns=$1
  kubectl logs -l app=order-subscriber -n "$ns" -c order-subscriber --tail=-1 \
    | grep -c "CloudEvent recibido" || true
}

case "$SCENARIO" in
  A)
    # Baseline: elimino consumerID y ttlInSeconds del componente del reto.
    BASE=$(mktemp)
    grep -v -E 'consumerID|\{appID\}-\{namespace\}|ttlInSeconds|value: "120"' \
      04-reto-gobernanza/pubsub-redis-challenge.yaml > "$BASE"
    COMPONENT="$BASE"
    ;;
  B)
    COMPONENT=04-reto-gobernanza/pubsub-redis-challenge.yaml
    ;;
  clean)
    for ns in "${TEAMS[@]}"; do kubectl delete namespace "$ns" --ignore-not-found; done
    exit 0
    ;;
  *) echo "Uso: $0 [A|B|clean]"; exit 1 ;;
esac

for ns in "${TEAMS[@]}"; do deploy_team "$ns" "$COMPONENT"; done
for ns in "${TEAMS[@]}"; do
  kubectl rollout restart deploy/order-subscriber -n "$ns"
  kubectl rollout status  deploy/order-subscriber -n "$ns" --timeout=180s
  # Espero a que termine el Pod anterior para contar solo lo que recibe el nuevo.
  until [ "$(kubectl get pods -l app=order-subscriber -n "$ns" --no-headers | wc -l | tr -d ' ')" = "1" ]; do
    sleep 2
  done
done
# Doy tiempo a que los sidecars creen sus consumer groups antes de publicar.
sleep 10

echo ">>> El equipo checkout publica 3 órdenes"
publish_from checkout
sleep 5

print_counts() {
  for ns in "${TEAMS[@]}"; do
    printf "  %-10s → CloudEvents recibidos: %s\n" "$ns" "$(count_received "$ns")"
  done
}

echo ""
echo "================ RESULTADO ESCENARIO $SCENARIO ================"
echo "Tras publicar desde checkout:"
print_counts

if [ "$SCENARIO" = "B" ]; then
  # En B también publico desde inventory para comprobar que cada equipo
  # recibe exactamente sus propios eventos y nada del otro.
  echo ""
  echo ">>> El equipo inventory publica 3 órdenes"
  publish_from inventory
  sleep 5
  echo "Tras publicar también desde inventory:"
  print_counts
fi
echo ""
echo ">>> Streams y consumer groups en Redis"
for key in $(kubectl exec -n redis redis-master-0 -- redis-cli --scan --pattern '*orders'); do
  echo "  stream: $key"
  kubectl exec -n redis redis-master-0 -- redis-cli XINFO GROUPS "$key" \
    | paste - - - - - - - - - - - - | awk '{print "    group:", $2, "| consumers:", $4}'
done
