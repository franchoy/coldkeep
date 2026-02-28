#!/usr/bin/env bash
set -euo pipefail

# Simple end-to-end smoke test for coldkeep V0.
# Requires a running Postgres with env vars set (or docker compose).
#
# Example (docker):
#   docker compose up -d postgres
#   docker compose run --rm -e COLDKEEP_STORAGE_DIR=/tmp/coldkeep-storage -v ./samples:/samples app bash scripts/smoke.sh

echo "[smoke] starting"

: "${COLDKEEP_STORAGE_DIR:=./storage/containers}"
mkdir -p "$COLDKEEP_STORAGE_DIR"

echo "[smoke] stats (before)"
coldkeep stats || true

echo "[smoke] store-folder samples"
coldkeep store-folder ./samples

echo "[smoke] stats (after)"
coldkeep stats

echo "[smoke] list files"
coldkeep list

# Restore first file id if available
FIRST_ID=$(coldkeep list | awk 'NR>2 {print $1; exit}' || true)
if [[ -n "${FIRST_ID}" ]]; then
  echo "[smoke] restoring first file id=${FIRST_ID}"
  mkdir -p ./_smoke_out
  coldkeep restore "${FIRST_ID}" ./_smoke_out/
  echo "[smoke] re-store restored file (should dedupe)"
  RESTORED=$(ls -1 ./_smoke_out | head -n 1)
  coldkeep store "./_smoke_out/${RESTORED}" || true
fi

echo "[smoke] done"
