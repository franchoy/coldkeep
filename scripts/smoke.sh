#!/usr/bin/env bash
set -euo pipefail
# samples dir can be overridden with env var, default to ./samples
: "${COLDKEEP_SAMPLES_DIR:=./samples}"

# Simple end-to-end smoke test for coldkeep V0.
# Requires a running Postgres with env vars set (or docker compose).
#
# Example (docker):
#   docker compose up -d postgres
#   docker compose run --rm  -e COLDKEEP_SAMPLES_DIR=/samples -v ./samples:/samples --entrypoint bash app scripts/smoke.sh

cleanup() {
  rm -rf ./_smoke_out
}

trap cleanup EXIT

ensure_postgres_schema() {
  if [[ -z "${COLDKEEP_TEST_DB:-}" ]]; then
    return
  fi

  if ! command -v psql >/dev/null 2>&1; then
    echo "[smoke] ERROR: psql is required to bootstrap db/schema_postgres.sql"
    exit 1
  fi

  local host="${DB_HOST:-127.0.0.1}"
  local port="${DB_PORT:-5432}"
  local user="${DB_USER:-coldkeep}"
  local name="${DB_NAME:-coldkeep}"
  local schema_path="${COLDKEEP_SCHEMA_PATH:-db/schema_postgres.sql}"

  if [[ ! -f "$schema_path" ]]; then
    echo "[smoke] ERROR: schema file not found: $schema_path"
    exit 1
  fi

  local exists
  exists=$(PGPASSWORD="${DB_PASSWORD:-}" psql \
    -h "$host" -p "$port" -U "$user" -d "$name" \
    -tAc "SELECT to_regclass('public.logical_file') IS NOT NULL;" \
    | tr -d '[:space:]')

  if [[ "$exists" != "t" ]]; then
    echo "[smoke] applying schema: $schema_path"
    PGPASSWORD="${DB_PASSWORD:-}" psql \
      -h "$host" -p "$port" -U "$user" -d "$name" \
      -v ON_ERROR_STOP=1 \
      -f "$schema_path" >/dev/null
  else
    echo "[smoke] schema already present"
  fi
}

reset_smoke_state() {
  if [[ "${COLDKEEP_SMOKE_RESET_DB:-0}" != "1" ]]; then
    return
  fi

  if [[ -z "${COLDKEEP_TEST_DB:-}" ]]; then
    echo "[smoke] WARN: COLDKEEP_SMOKE_RESET_DB=1 ignored because COLDKEEP_TEST_DB is not set"
    return
  fi

  local host="${DB_HOST:-127.0.0.1}"
  local port="${DB_PORT:-5432}"
  local user="${DB_USER:-coldkeep}"
  local name="${DB_NAME:-coldkeep}"

  echo "[smoke] resetting db tables"
  PGPASSWORD="${DB_PASSWORD:-}" psql \
    -h "$host" -p "$port" -U "$user" -d "$name" \
    -v ON_ERROR_STOP=1 \
    -c "TRUNCATE TABLE file_chunk, blocks, chunk, logical_file, container RESTART IDENTITY CASCADE;" >/dev/null

  echo "[smoke] resetting storage dir: $COLDKEEP_STORAGE_DIR"
  find "$COLDKEEP_STORAGE_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
}

echo "[smoke] starting"

: "${COLDKEEP_STORAGE_DIR:=./storage/containers}"
mkdir -p "$COLDKEEP_STORAGE_DIR"

ensure_postgres_schema
reset_smoke_state

echo "[smoke] stats (before)"
coldkeep stats

echo "[smoke] simulate store-folder samples"
coldkeep simulate store-folder --codec "${COLDKEEP_CODEC:-plain}" "$COLDKEEP_SAMPLES_DIR"

echo "[smoke] store-folder samples"
coldkeep store-folder "$COLDKEEP_SAMPLES_DIR"

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

echo "[smoke] search test"
SEARCH_OUTPUT=$(coldkeep search --name hello)
echo "$SEARCH_OUTPUT"
if ! grep -qi "hello" <<<"$SEARCH_OUTPUT"; then
  echo "[smoke] ERROR: search --name hello returned no matching rows"
  exit 1
fi

echo "[smoke] remove test"
if [[ -n "${FIRST_ID}" ]]; then
  echo "[smoke] removing first file id=${FIRST_ID}"
  coldkeep remove "${FIRST_ID}"
fi

echo "[smoke] gc test"
coldkeep gc

echo "[smoke] verify system (full)"
coldkeep verify system --full

echo "[smoke] stats (after gc)"
coldkeep stats

# Edge cases test: many small files + pattern file
echo ""
echo "[smoke] === EDGE CASES TEST ==="
EDGE_CASES_DIR="${PWD}/samples_edge_cases"
if [[ ! -d "$EDGE_CASES_DIR" ]]; then
  echo "[smoke] WARNING: samples_edge_cases directory not found at $EDGE_CASES_DIR, skipping"
else
  echo "[smoke] resetting for edge cases run"
  reset_smoke_state

  echo "[smoke] stats (edge cases before)"
  coldkeep stats

  echo "[smoke] simulate store-folder edge cases"
  coldkeep simulate store-folder --codec "${COLDKEEP_CODEC:-plain}" "$EDGE_CASES_DIR"

  echo "[smoke] store-folder edge cases"
  coldkeep store-folder "$EDGE_CASES_DIR"

  echo "[smoke] stats (edge cases after)"
  coldkeep stats

  echo "[smoke] list edge case files"
  coldkeep list

  # Try to restore the pattern file if it exists
  FIRST_EDGE_ID=$(coldkeep list | awk 'NR>2 {print $1; exit}' || true)
  if [[ -n "${FIRST_EDGE_ID}" ]]; then
    echo "[smoke] restoring first edge case file id=${FIRST_EDGE_ID}"
    mkdir -p ./_smoke_out
    coldkeep restore "${FIRST_EDGE_ID}" ./_smoke_out/
    echo "[smoke] re-store restored edge case file (should dedupe)"
    RESTORED=$(ls -1 ./_smoke_out | head -n 1)
    coldkeep store "./_smoke_out/${RESTORED}" || true
  fi

  echo "[smoke] search edge cases (pattern.txt)"
  SEARCH_OUTPUT=$(coldkeep search --name pattern)
  echo "$SEARCH_OUTPUT"
  if ! grep -qi "pattern" <<<"$SEARCH_OUTPUT"; then
    echo "[smoke] WARNING: search --name pattern did not match expected files (might be deduplicated)"
  fi

  echo "[smoke] remove edge case test"
  if [[ -n "${FIRST_EDGE_ID}" ]]; then
    echo "[smoke] removing first edge case file id=${FIRST_EDGE_ID}"
    coldkeep remove "${FIRST_EDGE_ID}"
  fi

  echo "[smoke] gc test (edge cases)"
  coldkeep gc

  echo "[smoke] verify system --full (edge cases)"
  coldkeep verify system --full

  echo "[smoke] stats (edge cases after gc)"
  coldkeep stats
fi

echo "[smoke] done"
