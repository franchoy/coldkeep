#!/usr/bin/env bash
set -euo pipefail

# Simple end-to-end smoke test for Capsule V0.
# Requires a running Postgres with env vars set (or docker compose).
#
# Example (docker):
#   docker compose up -d postgres
#   docker compose run --rm -e CAPSULE_STORAGE_DIR=/tmp/capsule-storage -v ./samples:/samples app bash scripts/smoke.sh

echo "[smoke] starting"

: "${CAPSULE_STORAGE_DIR:=./storage/containers}"
mkdir -p "$CAPSULE_STORAGE_DIR"

echo "[smoke] stats (before)"
./capsule stats || true

echo "[smoke] store-folder samples"
./capsule store-folder ./samples

echo "[smoke] stats (after)"
./capsule stats

echo "[smoke] list files"
./capsule list

# Restore first file id if available
FIRST_ID=$(./capsule list | awk 'NR==2 {print $1}' || true)
if [[ -n "${FIRST_ID}" ]]; then
  echo "[smoke] restoring first file id=${FIRST_ID}"
  mkdir -p ./_smoke_out
  ./capsule restore "${FIRST_ID}" ./_smoke_out/
  echo "[smoke] re-store restored file (should dedupe)"
  RESTORED=$(ls -1 ./_smoke_out | head -n 1)
  ./capsule store "./_smoke_out/${RESTORED}" || true
fi

echo "[smoke] done"
