#!/bin/sh
# Publica (crea o actualiza) todos los flows de FLOWS_DIR en Kestra vía API.
# Uso desde el host tras editar un flow:  set -a; . ./.env; set +a; ./infra/deploy_flows.sh
set -eu
KESTRA_URL="${KESTRA_URL:-http://localhost:8080}"
FLOWS_DIR="${FLOWS_DIR:-$(dirname "$0")/../flows}"
AUTH="${KESTRA_USER}:${KESTRA_PASSWORD}"

echo "Esperando a Kestra en ${KESTRA_URL} ..."
i=0
until [ "$(curl -s -o /dev/null -w '%{http_code}' -u "$AUTH" "${KESTRA_URL}/api/v1/main/flows/search")" = "200" ]; do
  i=$((i + 1))
  [ "$i" -ge 100 ] && { echo "Kestra no respondió a tiempo" >&2; exit 1; }
  sleep 3
done

for f in "$FLOWS_DIR"/*.yml; do
  invalid=$(curl -sf -u "$AUTH" -X POST -F "fileUpload=@${f}" "${KESTRA_URL}/api/v1/main/flows/import")
  if [ "$invalid" != "[]" ]; then
    echo "Flow inválido: ${f} -> ${invalid}" >&2
    exit 1
  fi
  echo "Publicado: $(basename "$f")"
done
