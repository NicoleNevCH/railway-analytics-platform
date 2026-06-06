#!/usr/bin/env bash
# Wait for the HTTP services to become healthy after `make up`.
# Usage: ./scripts/wait_for_services.sh [attempts]
set -euo pipefail

ATTEMPTS="${1:-60}"
SLEEP=3

check() {
  local name="$1" url="$2"
  printf 'Waiting for %s (%s) ' "$name" "$url"
  for _ in $(seq 1 "$ATTEMPTS"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "OK"
      return 0
    fi
    printf '.'
    sleep "$SLEEP"
  done
  echo " FAILED"
  return 1
}

check "Ingestion API" "http://localhost:8000/health"
check "Consumption API"  "http://localhost:8001/health"
echo "Services ready."
