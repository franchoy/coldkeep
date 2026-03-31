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

echo "[smoke] dedup test: re-storing same folder should not change key storage counters"
STATS_BEFORE=$(coldkeep stats --output json)
coldkeep store-folder "$COLDKEEP_SAMPLES_DIR"
STATS_AFTER=$(coldkeep stats --output json)
DEDUP_FAILED=0
for field in total_files completed_files total_chunks completed_chunks total_logical_size_bytes live_block_bytes total_containers; do
  before=$(echo "$STATS_BEFORE" | grep -oP "\"${field}\":[[:space:]]*\K[0-9]+")
  after=$(echo "$STATS_AFTER" | grep -oP "\"${field}\":[[:space:]]*\K[0-9]+")
  if [[ "$before" != "$after" ]]; then
    echo "[smoke] ERROR: dedup failed: '${field}' changed: ${before} -> ${after}"
    DEDUP_FAILED=1
  fi
done
if [[ "$DEDUP_FAILED" -eq 1 ]]; then
  exit 1
fi
echo "[smoke] dedup test PASSED: all key storage counters unchanged after re-storing same folder"

echo "[smoke] list files"
coldkeep list

# Capture first ID before the restore loop (needed for the remove test below)
FIRST_ID=$(coldkeep list | awk 'NR>2 && $1~/^[0-9]+$/ {print $1; exit}' || true)

echo "[smoke] restore-all: restoring every stored file and verifying byte-perfect fidelity"
rm -rf ./_smoke_out
RESTORE_ALL_FAILED=0
while IFS=' ' read -r file_id file_name; do
  restore_dir="./_smoke_out/${file_id}"
  mkdir -p "$restore_dir"
  if ! coldkeep restore "${file_id}" "${restore_dir}/"; then
    echo "[smoke] ERROR: restore command failed for id=${file_id} name=${file_name}"
    RESTORE_ALL_FAILED=1
    continue
  fi
  restored="${restore_dir}/${file_name}"
  if [[ ! -f "$restored" ]]; then
    echo "[smoke] ERROR: restore produced no output file for id=${file_id} name=${file_name}"
    RESTORE_ALL_FAILED=1
    continue
  fi
  original=$(find "$COLDKEEP_SAMPLES_DIR" -name "${file_name}" -type f 2>/dev/null | head -1)
  if [[ -z "$original" ]]; then
    echo "[smoke] WARNING: original not found for '${file_name}' (id=${file_id}), skipping hash check"
    continue
  fi
  orig_hash=$(sha256sum "$original" | awk '{print $1}')
  rest_hash=$(sha256sum "$restored" | awk '{print $1}')
  if [[ "$orig_hash" != "$rest_hash" ]]; then
    echo "[smoke] ERROR: hash mismatch for id=${file_id} name=${file_name}: want=${orig_hash} got=${rest_hash}"
    RESTORE_ALL_FAILED=1
  else
    echo "[smoke]   ok: id=${file_id} ${file_name}"
  fi
done < <(coldkeep list | awk 'NR>2 && $1~/^[0-9]+$/ {print $1, $2}')
if [[ "$RESTORE_ALL_FAILED" -eq 1 ]]; then
  exit 1
fi
echo "[smoke] restore-all PASSED: all stored files restore byte-perfectly"

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

  echo "[smoke] dedup test (edge cases): re-storing same folder should not change key storage counters"
  EDGE_STATS_BEFORE=$(coldkeep stats --output json)
  coldkeep store-folder "$EDGE_CASES_DIR"
  EDGE_STATS_AFTER=$(coldkeep stats --output json)
  EDGE_DEDUP_FAILED=0
  for field in total_files completed_files total_chunks completed_chunks total_logical_size_bytes live_block_bytes total_containers; do
    before=$(echo "$EDGE_STATS_BEFORE" | grep -oP "\"${field}\":[[:space:]]*\K[0-9]+")
    after=$(echo "$EDGE_STATS_AFTER" | grep -oP "\"${field}\":[[:space:]]*\K[0-9]+")
    if [[ "$before" != "$after" ]]; then
      echo "[smoke] ERROR: dedup (edge cases) failed: '${field}' changed: ${before} -> ${after}"
      EDGE_DEDUP_FAILED=1
    fi
  done
  if [[ "$EDGE_DEDUP_FAILED" -eq 1 ]]; then
    exit 1
  fi
  echo "[smoke] dedup test (edge cases) PASSED: all key storage counters unchanged after re-storing same folder"

  echo "[smoke] list edge case files"
  coldkeep list

  # Capture first ID before the restore loop (needed for the remove test below)
  FIRST_EDGE_ID=$(coldkeep list | awk 'NR>2 && $1~/^[0-9]+$/ {print $1; exit}' || true)

  echo "[smoke] restore-all (edge cases): restoring every stored file and verifying byte-perfect fidelity"
  rm -rf ./_smoke_out
  EDGE_RESTORE_ALL_FAILED=0
  while IFS=' ' read -r file_id file_name; do
    restore_dir="./_smoke_out/${file_id}"
    mkdir -p "$restore_dir"
    if ! coldkeep restore "${file_id}" "${restore_dir}/"; then
      echo "[smoke] ERROR: restore command failed for id=${file_id} name=${file_name}"
      EDGE_RESTORE_ALL_FAILED=1
      continue
    fi
    restored="${restore_dir}/${file_name}"
    if [[ ! -f "$restored" ]]; then
      echo "[smoke] ERROR: restore produced no output file for id=${file_id} name=${file_name}"
      EDGE_RESTORE_ALL_FAILED=1
      continue
    fi
    original=$(find "$EDGE_CASES_DIR" -name "${file_name}" -type f 2>/dev/null | head -1)
    if [[ -z "$original" ]]; then
      echo "[smoke] WARNING: original not found for '${file_name}' (id=${file_id}), skipping hash check"
      continue
    fi
    orig_hash=$(sha256sum "$original" | awk '{print $1}')
    rest_hash=$(sha256sum "$restored" | awk '{print $1}')
    if [[ "$orig_hash" != "$rest_hash" ]]; then
      echo "[smoke] ERROR: hash mismatch for id=${file_id} name=${file_name}: want=${orig_hash} got=${rest_hash}"
      EDGE_RESTORE_ALL_FAILED=1
    else
      echo "[smoke]   ok: id=${file_id} ${file_name}"
    fi
  done < <(coldkeep list | awk 'NR>2 && $1~/^[0-9]+$/ {print $1, $2}')
  if [[ "$EDGE_RESTORE_ALL_FAILED" -eq 1 ]]; then
    exit 1
  fi
  echo "[smoke] restore-all (edge cases) PASSED: all stored files restore byte-perfectly"

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
