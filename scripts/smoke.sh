#!/usr/bin/env bash
set -euo pipefail
# samples dir can be overridden with env var, default to ./samples
: "${COLDKEEP_SAMPLES_DIR:=./samples}"

# Comprehensive end-to-end smoke test for coldkeep V0.10+
# Validates store, restore, dedup, GC, recovery, and multi-level verification
# across different codecs and operational scenarios.
#
# Requires a running Postgres with env vars set (or docker compose).
#
# Example (docker):
#   docker compose up -d postgres
#   docker compose run --rm  -e COLDKEEP_SAMPLES_DIR=/samples -v ./samples:/samples --entrypoint bash app scripts/smoke.sh

cleanup() {
  rm -rf ./_smoke_out
}

trap cleanup EXIT

if ! command -v jq >/dev/null 2>&1; then
  echo "[smoke] ERROR: jq is required for JSON-based output parsing"
  exit 1
fi

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

# Validate JSON output has expected top-level fields
validate_json_output() {
  local output="$1"
  local expected_fields="$2"  # space-separated field names
  local payload

  payload=$(echo "$output" | grep -E '^\{.*\}$' | tail -n1)
  if [[ -z "$payload" ]]; then
    echo "[smoke] ERROR: JSON output contains no parseable object"
    return 1
  fi
  
  for field in $expected_fields; do
    if ! echo "$payload" | jq -e ".${field}" > /dev/null 2>&1; then
      echo "[smoke] ERROR: JSON output missing field '${field}'"
      return 1
    fi
  done
}

# Compare numeric stat fields between two JSON outputs
compare_stat_fields() {
  local before_stats="$1"
  local after_stats="$2"
  local fields="$3"  # space-separated field names
  local should_change="$4"  # "same" or "different"
  
  for field in $fields; do
    local before=$(echo "$before_stats" | jq -r ".data.${field}")
    local after=$(echo "$after_stats" | jq -r ".data.${field}")
    
    if [[ "$should_change" == "same" ]]; then
      if [[ "$before" != "$after" ]]; then
        echo "[smoke] ERROR: field '${field}' changed unexpectedly: ${before} -> ${after}"
        return 1
      fi
    else
      if [[ "$before" == "$after" ]]; then
        echo "[smoke] ERROR: field '${field}' did not change as expected: ${before}"
        return 1
      fi
    fi
  done
  return 0
}

# Test a command with invalid arguments and verify failure
test_invalid_command() {
  local description="$1"
  shift
  local cmd=("$@")
  
  echo "[smoke] testing invalid command: $description"
  if "${cmd[@]}" > /dev/null 2>&1; then
    echo "[smoke] ERROR: command should have failed: $description"
    return 1
  fi
  echo "[smoke]   ok: command correctly rejected invalid input"
}

echo "[smoke] starting"

: "${COLDKEEP_STORAGE_DIR:=./storage/containers}"
mkdir -p "$COLDKEEP_STORAGE_DIR"

ensure_postgres_schema
reset_smoke_state

echo "[smoke] stats (before)"
coldkeep stats

echo "[smoke] simulate store-folder samples"
if ! coldkeep simulate store-folder --codec "${COLDKEEP_CODEC:-plain}" "$COLDKEEP_SAMPLES_DIR"; then
  if [[ "${COLDKEEP_SMOKE_STRICT_SIMULATE:-0}" == "1" ]]; then
    echo "[smoke] ERROR: simulate store-folder samples failed"
    exit 1
  fi
  echo "[smoke] WARNING: simulate store-folder samples failed; continuing (set COLDKEEP_SMOKE_STRICT_SIMULATE=1 to fail)"
fi

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
  before=$(echo "$STATS_BEFORE" | jq -r ".data.${field}")
  after=$(echo "$STATS_AFTER" | jq -r ".data.${field}")
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

LIST_OUTPUT=$(coldkeep list --output json)

# Capture first ID before the restore loop (needed for the remove test below)
FIRST_ID=$(echo "$LIST_OUTPUT" | jq -r 'if (.files | length) > 0 then .files[0].id else empty end')

echo "[smoke] restore-all: restoring every stored file and verifying byte-perfect fidelity"
rm -rf ./_smoke_out
RESTORE_ALL_FAILED=0
while IFS=$'\t' read -r file_id file_name expected_hash; do
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
  if [[ -z "$expected_hash" || "$expected_hash" == "null" ]]; then
    echo "[smoke] ERROR: list output missing file_hash for id=${file_id} name=${file_name}"
    RESTORE_ALL_FAILED=1
    continue
  fi
  rest_hash=$(sha256sum "$restored" | awk '{print $1}')
  if [[ "$expected_hash" != "$rest_hash" ]]; then
    echo "[smoke] ERROR: hash mismatch for id=${file_id} name=${file_name}: want=${expected_hash} got=${rest_hash}"
    RESTORE_ALL_FAILED=1
  else
    echo "[smoke]   ok: id=${file_id} ${file_name}"
  fi
done < <(echo "$LIST_OUTPUT" | jq -r '.files[] | [(.id | tostring), .name, .file_hash] | @tsv')
if [[ "$RESTORE_ALL_FAILED" -eq 1 ]]; then
  exit 1
fi
echo "[smoke] restore-all PASSED: all stored files restore byte-perfectly"

echo "[smoke] search test"
SEARCH_OUTPUT=$(coldkeep search --name hello --output json)
echo "$SEARCH_OUTPUT"
if ! echo "$SEARCH_OUTPUT" | jq -e '(.files | length) > 0' > /dev/null 2>&1; then
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
  if ! coldkeep simulate store-folder --codec "${COLDKEEP_CODEC:-plain}" "$EDGE_CASES_DIR"; then
    if [[ "${COLDKEEP_SMOKE_STRICT_SIMULATE:-0}" == "1" ]]; then
      echo "[smoke] ERROR: simulate store-folder edge cases failed"
      exit 1
    fi
    echo "[smoke] WARNING: simulate store-folder edge cases failed; continuing (set COLDKEEP_SMOKE_STRICT_SIMULATE=1 to fail)"
  fi

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
    before=$(echo "$EDGE_STATS_BEFORE" | jq -r ".data.${field}")
    after=$(echo "$EDGE_STATS_AFTER" | jq -r ".data.${field}")
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

  EDGE_LIST_OUTPUT=$(coldkeep list --output json)

  # Capture first ID before the restore loop (needed for the remove test below)
  FIRST_EDGE_ID=$(echo "$EDGE_LIST_OUTPUT" | jq -r 'if (.files | length) > 0 then .files[0].id else empty end')

  echo "[smoke] restore-all (edge cases): restoring every stored file and verifying byte-perfect fidelity"
  rm -rf ./_smoke_out
  EDGE_RESTORE_ALL_FAILED=0
  while IFS=$'\t' read -r file_id file_name expected_hash; do
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
    if [[ -z "$expected_hash" || "$expected_hash" == "null" ]]; then
      echo "[smoke] ERROR: list output missing file_hash for id=${file_id} name=${file_name}"
      EDGE_RESTORE_ALL_FAILED=1
      continue
    fi
    rest_hash=$(sha256sum "$restored" | awk '{print $1}')
    if [[ "$expected_hash" != "$rest_hash" ]]; then
      echo "[smoke] ERROR: hash mismatch for id=${file_id} name=${file_name}: want=${expected_hash} got=${rest_hash}"
      EDGE_RESTORE_ALL_FAILED=1
    else
      echo "[smoke]   ok: id=${file_id} ${file_name}"
    fi
  done < <(echo "$EDGE_LIST_OUTPUT" | jq -r '.files[] | [(.id | tostring), .name, .file_hash] | @tsv')
  if [[ "$EDGE_RESTORE_ALL_FAILED" -eq 1 ]]; then
    exit 1
  fi
  echo "[smoke] restore-all (edge cases) PASSED: all stored files restore byte-perfectly"

  echo "[smoke] search edge cases (pattern.txt)"
  SEARCH_OUTPUT=$(coldkeep search --name pattern --output json 2>/dev/null | grep '^\{' | tail -n1)
  SEARCH_STATUS=$(echo "$SEARCH_OUTPUT"       | jq -r '.status // empty')
  SEARCH_CMD=$(echo "$SEARCH_OUTPUT"          | jq -r '.command // empty')
  SEARCH_COUNT=$(echo "$SEARCH_OUTPUT"        | jq -r '.files | length')
  SEARCH_MATCH=$(echo "$SEARCH_OUTPUT"        | jq -r '[.files[].name | select(test("pattern"; "i"))] | length')
  if [[ "$SEARCH_STATUS" != "ok" ]]; then
    echo "[smoke] FAIL: search --name pattern returned status='$SEARCH_STATUS' (expected 'ok')"
    exit 1
  fi
  if [[ "$SEARCH_CMD" != "search" ]]; then
    echo "[smoke] FAIL: search --name pattern returned command='$SEARCH_CMD' (expected 'search')"
    exit 1
  fi
  if [[ "$SEARCH_COUNT" -eq 0 ]]; then
    echo "[smoke] FAIL: search --name pattern returned 0 files (expected at least pattern.txt)"
    exit 1
  fi
  if [[ "$SEARCH_MATCH" -eq 0 ]]; then
    echo "[smoke] FAIL: search --name pattern returned $SEARCH_COUNT files but none match 'pattern'"
    exit 1
  fi
  echo "[smoke]   ok: search --name pattern found $SEARCH_COUNT file(s), $SEARCH_MATCH match 'pattern'"

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

echo ""
echo "[smoke] === VERIFY MODES TEST ==="

# Test all three verify modes: standard, full, deep
echo "[smoke] verify system --standard (metadata-only checks)"
if ! coldkeep verify system --standard; then
  echo "[smoke] ERROR: verify system --standard failed"
  exit 1
fi
echo "[smoke]   ok: standard verify passed"

echo "[smoke] verify system --full (includes file existence checks)"
if ! coldkeep verify system --full; then
  echo "[smoke] ERROR: verify system --full failed"
  exit 1
fi
echo "[smoke]   ok: full verify passed"

echo "[smoke] verify system --deep (complete data integrity with hash validation)"
if ! coldkeep verify system --deep; then
  echo "[smoke] ERROR: verify system --deep failed"
  exit 1
fi
echo "[smoke]   ok: deep verify passed"

echo ""
echo "[smoke] === DOCTOR COMMAND TEST (Operator Health Gate) ==="

# Doctor is treated as an operator-facing release gate command.
# It must pass before this smoke run can be considered release-ready.
# Doctor validates recovery, schema, and verification in one pass.

echo "[smoke] doctor (default) - quick health report (--standard)"
if ! coldkeep doctor; then
  echo "[smoke] ERROR: doctor command failed"
  exit 1
fi
echo "[smoke]   ok: doctor default report completed"

echo "[smoke] doctor --standard - metadata-only health check"
if ! coldkeep doctor --standard; then
  echo "[smoke] ERROR: doctor --standard failed"
  exit 1
fi
echo "[smoke]   ok: doctor --standard passed"

echo "[smoke] doctor --full - includes recovery and file checks"
if ! coldkeep doctor --full; then
  echo "[smoke] ERROR: doctor --full failed"
  exit 1
fi
echo "[smoke]   ok: doctor --full passed"

echo "[smoke] validating doctor JSON output contract"
DOCTOR_JSON=$(coldkeep doctor --output json)
DOCTOR_PAYLOAD=$(echo "$DOCTOR_JSON" | grep -E '^\{.*\}$' | tail -n1)

# Doctor JSON should contain recovery status, verify status, and schema status
if ! echo "$DOCTOR_PAYLOAD" | jq -e '.status == "ok" and .command == "doctor" and .data.recovery_status and .data.verify_status and .data.schema_status' > /dev/null 2>&1; then
  echo "[smoke] ERROR: doctor JSON output missing required fields"
  echo "$DOCTOR_JSON"
  echo "Parsed payload:"
  echo "$DOCTOR_PAYLOAD" | jq . 2>/dev/null || echo "Failed to parse JSON"
  exit 1
fi

RECOVERY_STATUS=$(echo "$DOCTOR_PAYLOAD" | jq -r '.data.recovery_status')
VERIFY_STATUS=$(echo "$DOCTOR_PAYLOAD" | jq -r '.data.verify_status')
SCHEMA_STATUS=$(echo "$DOCTOR_PAYLOAD" | jq -r '.data.schema_status')
VERIFY_LEVEL=$(echo "$DOCTOR_PAYLOAD" | jq -r '.data.verify_level')

if [[ "$VERIFY_LEVEL" != "standard" ]]; then
  echo "[smoke] ERROR: doctor --output json verify_level='$VERIFY_LEVEL' (expected 'standard')"
  exit 1
fi

echo "[smoke]   ok: doctor JSON validated (verify_level=$VERIFY_LEVEL, recovery=$RECOVERY_STATUS, verify=$VERIFY_STATUS, schema=$SCHEMA_STATUS)"

echo "[smoke] doctor --full --output json (complete operator report)"
DOCTOR_FULL_JSON=$(coldkeep doctor --full --output json)
DOCTOR_FULL_PAYLOAD=$(echo "$DOCTOR_FULL_JSON" | grep -E '^\{.*\}$' | tail -n1)

if ! echo "$DOCTOR_FULL_PAYLOAD" | jq -e '.status == "ok" and .command == "doctor" and .data.recovery_status and .data.verify_status and .data.schema_status' > /dev/null 2>&1; then
  echo "[smoke] ERROR: doctor --full --output json missing required fields"
  echo "$DOCTOR_FULL_JSON"
  exit 1
fi

DOCTOR_FULL_VERIFY_LEVEL=$(echo "$DOCTOR_FULL_PAYLOAD" | jq -r '.data.verify_level')
if [[ "$DOCTOR_FULL_VERIFY_LEVEL" != "full" ]]; then
  echo "[smoke] ERROR: doctor --full --output json verify_level='$DOCTOR_FULL_VERIFY_LEVEL' (expected 'full')"
  exit 1
fi

echo "[smoke]   ok: doctor --full --output json produced complete operator health report (verify_level=$DOCTOR_FULL_VERIFY_LEVEL)"

echo ""
echo "[smoke] === GC DRY-RUN ACCURACY TEST ==="

# Test GC dry-run prediction vs actual container deletion count.
echo "[smoke] running gc --dry-run to get predicted metric"
GC_DRYRUN_OUTPUT=$(coldkeep gc --dry-run --output json 2>/dev/null)
GC_DRYRUN_PAYLOAD=$(echo "$GC_DRYRUN_OUTPUT" | grep -E '^\{.*\}$' | tail -n1)
PREDICTED_AFFECTED_CONTAINERS=$(echo "$GC_DRYRUN_PAYLOAD" | jq -r '.data.affected_containers // 0' 2>/dev/null || echo "0")
PREDICTED_DRY_RUN=$(echo "$GC_DRYRUN_PAYLOAD" | jq -r '.data.dry_run // false' 2>/dev/null || echo "false")

if [[ "$PREDICTED_DRY_RUN" != "true" ]]; then
  echo "[smoke] ERROR: gc --dry-run did not report dry_run=true"
  echo "$GC_DRYRUN_OUTPUT"
  exit 1
fi

echo "[smoke] running real gc"
STATS_BEFORE_GC=$(coldkeep stats --output json)
BEFORE_TOTAL_CONTAINERS=$(echo "$STATS_BEFORE_GC" | jq -r '.data.total_containers')
coldkeep gc > /dev/null
STATS_AFTER_GC=$(coldkeep stats --output json)
AFTER_TOTAL_CONTAINERS=$(echo "$STATS_AFTER_GC" | jq -r '.data.total_containers')
ACTUAL_DELETED_CONTAINERS=$((BEFORE_TOTAL_CONTAINERS - AFTER_TOTAL_CONTAINERS))

if [[ "$PREDICTED_AFFECTED_CONTAINERS" != "$ACTUAL_DELETED_CONTAINERS" ]]; then
  echo "[smoke] ERROR: gc dry-run prediction mismatch: predicted=${PREDICTED_AFFECTED_CONTAINERS} actual=${ACTUAL_DELETED_CONTAINERS}"
  exit 1
fi

echo "[smoke]   ok: gc dry-run predicted ${PREDICTED_AFFECTED_CONTAINERS} deleted containers and real gc matched"

echo ""
echo "[smoke] === SIMULATION METRICS VALIDATION ==="

# Validate simulate/store-folder against a fresh unique dataset so real deltas are exact.
SIM_VALIDATE_DIR="./_smoke_sim_validation"
rm -rf "$SIM_VALIDATE_DIR"
mkdir -p "$SIM_VALIDATE_DIR"

dd if=/dev/urandom of="$SIM_VALIDATE_DIR/file_a.bin" bs=1M count=2 status=none
dd if=/dev/urandom of="$SIM_VALIDATE_DIR/file_b.bin" bs=1M count=1 status=none

echo "[smoke] getting simulation metrics"
SIM_OUTPUT=""
if ! SIM_OUTPUT=$(coldkeep simulate store-folder --codec "${COLDKEEP_CODEC:-plain}" "$SIM_VALIDATE_DIR" --output json 2>/dev/null); then
  rm -rf "$SIM_VALIDATE_DIR"
  if [[ "${COLDKEEP_SMOKE_STRICT_SIMULATE:-0}" == "1" ]]; then
    echo "[smoke] ERROR: simulation metrics command failed"
    exit 1
  fi
  echo "[smoke] WARNING: simulation metrics command failed; skipping simulate-vs-real comparison"
else
  SIM_PAYLOAD=$(echo "$SIM_OUTPUT" | grep -E '^\{.*\}$' | tail -n1)
  if ! echo "$SIM_PAYLOAD" | jq -e '.status == "ok" and .command == "simulate" and .simulated == true and .data.files != null and .data.chunks != null and .data.logical_size_bytes != null and .data.physical_size_bytes != null' > /dev/null 2>&1; then
    echo "[smoke] ERROR: simulate command returned invalid structured JSON"
    echo "$SIM_OUTPUT"
    rm -rf "$SIM_VALIDATE_DIR"
    exit 1
  fi

  SIM_FILES=$(echo "$SIM_PAYLOAD" | jq -r '.data.files')
  SIM_CHUNKS=$(echo "$SIM_PAYLOAD" | jq -r '.data.chunks')
  SIM_LOGICAL_SIZE_BYTES=$(echo "$SIM_PAYLOAD" | jq -r '.data.logical_size_bytes')
  SIM_PHYSICAL_SIZE_BYTES=$(echo "$SIM_PAYLOAD" | jq -r '.data.physical_size_bytes')

  echo "[smoke] getting real store metrics"
  STATS_BEFORE=$(coldkeep stats --output json)
  coldkeep store-folder "$SIM_VALIDATE_DIR" > /dev/null
  STATS_AFTER=$(coldkeep stats --output json)

  REAL_FILES=$(( $(echo "$STATS_AFTER" | jq -r '.data.total_files') - $(echo "$STATS_BEFORE" | jq -r '.data.total_files') ))
  REAL_CHUNKS=$(( $(echo "$STATS_AFTER" | jq -r '.data.total_chunks') - $(echo "$STATS_BEFORE" | jq -r '.data.total_chunks') ))
  REAL_LOGICAL_SIZE_BYTES=$(( $(echo "$STATS_AFTER" | jq -r '.data.total_logical_size_bytes') - $(echo "$STATS_BEFORE" | jq -r '.data.total_logical_size_bytes') ))
  REAL_PHYSICAL_SIZE_BYTES=$(( $(echo "$STATS_AFTER" | jq -r '.data.live_block_bytes') - $(echo "$STATS_BEFORE" | jq -r '.data.live_block_bytes') ))

  echo "[smoke] simulation predicted: files=$SIM_FILES, chunks=$SIM_CHUNKS, logical_bytes=$SIM_LOGICAL_SIZE_BYTES, physical_bytes=$SIM_PHYSICAL_SIZE_BYTES"
  echo "[smoke] real store delta: files=$REAL_FILES, chunks=$REAL_CHUNKS, logical_bytes=$REAL_LOGICAL_SIZE_BYTES, physical_bytes=$REAL_PHYSICAL_SIZE_BYTES"

  if [[ "$SIM_FILES" != "$REAL_FILES" || "$SIM_CHUNKS" != "$REAL_CHUNKS" || "$SIM_LOGICAL_SIZE_BYTES" != "$REAL_LOGICAL_SIZE_BYTES" || "$SIM_PHYSICAL_SIZE_BYTES" != "$REAL_PHYSICAL_SIZE_BYTES" ]]; then
    echo "[smoke] ERROR: simulation metrics mismatch with real store delta"
    rm -rf "$SIM_VALIDATE_DIR"
    exit 1
  fi

  echo "[smoke]   ok: simulation metrics match real store delta"
fi

rm -rf "$SIM_VALIDATE_DIR"

echo ""
echo "[smoke] === JSON OUTPUT VALIDATION ==="

# Validate JSON contracts for key commands
echo "[smoke] validating stats JSON output contract"
STATS_JSON=$(coldkeep stats --output json)
if ! validate_json_output "$STATS_JSON" "status data"; then
  echo "[smoke] ERROR: stats JSON output has invalid structure"
  exit 1
fi
echo "[smoke]   ok: stats JSON has required fields"

echo "[smoke] validating list JSON output contract"
LIST_JSON=$(coldkeep list --output json)
if ! validate_json_output "$LIST_JSON" "status files"; then
  echo "[smoke] ERROR: list JSON output has invalid structure"
  exit 1
fi
echo "[smoke]   ok: list JSON has required fields"

echo "[smoke] validating search JSON output contract"
SEARCH_JSON=$(coldkeep search --name test --output json)
if ! validate_json_output "$SEARCH_JSON" "status files"; then
  echo "[smoke] ERROR: search JSON output has invalid structure"
  exit 1
fi
echo "[smoke]   ok: search JSON has required fields"

echo ""
echo "[smoke] === ERROR HANDLING TEST ==="

# Test that invalid restore ID is rejected
echo "[smoke] testing invalid restore ID (should fail)"
test_invalid_command "restore with invalid ID" coldkeep restore "999999999"

# Test that invalid search flags are rejected
echo "[smoke] testing invalid search filters (should fail)"
test_invalid_command "search with invalid min-size" coldkeep search --min-size "not-a-number"

echo ""
echo "[smoke] === SEARCH FILTERS TEST ==="

# Reset and prepare for search filter tests
reset_smoke_state
coldkeep store-folder "$COLDKEEP_SAMPLES_DIR" > /dev/null

echo "[smoke] testing search with name filter"
SEARCH_BY_NAME=$(coldkeep search --name "hello" --output json)
HELLO_COUNT=$(echo "$SEARCH_BY_NAME" | jq '.files | length')
if [[ "$HELLO_COUNT" -gt 0 ]]; then
  echo "[smoke]   ok: search --name found $HELLO_COUNT files"
else
  echo "[smoke] ERROR: search --name should have found files"
  exit 1
fi

echo "[smoke] testing search with limit filter"
SEARCH_LIMIT=$(coldkeep search --limit 2 --output json)
LIMITED_COUNT=$(echo "$SEARCH_LIMIT" | jq '.files | length')
if [[ "$LIMITED_COUNT" -le 2 ]]; then
  echo "[smoke]   ok: search --limit 2 returned $LIMITED_COUNT files"
else
  echo "[smoke] ERROR: search --limit not respected"
  exit 1
fi

echo ""
echo "[smoke] === VERSION COMMAND TEST ==="

VERSION_OUTPUT=$(coldkeep version --output json 2>/dev/null)
VERSION_PAYLOAD=$(echo "$VERSION_OUTPUT" | grep -E '^\{.*\}$' | tail -n1)
if echo "$VERSION_PAYLOAD" | jq -e '.status == "ok" and .data.version' > /dev/null 2>&1; then
  VERSION=$(echo "$VERSION_PAYLOAD" | jq -r '.data.version')
  echo "[smoke]   ok: coldkeep version: $VERSION"
else
  echo "[smoke] ERROR: version command did not return valid structured JSON"
  echo "$VERSION_OUTPUT"
  exit 1
fi

echo ""
echo "[smoke] === MULTIPLE RESTORE DETERMINISM TEST ==="

# Verify that multiple restores of the same file produce identical results
SAMPLE_FILE_ID=$(coldkeep list --output json | jq -r '.files[0].id' 2>/dev/null)
if [[ -n "$SAMPLE_FILE_ID" && "$SAMPLE_FILE_ID" != "null" ]]; then
  echo "[smoke] testing determinism: restore file $SAMPLE_FILE_ID multiple times"
  
  RESTORE_DIR1="./_smoke_restore_1"
  RESTORE_DIR2="./_smoke_restore_2"
  mkdir -p "$RESTORE_DIR1" "$RESTORE_DIR2"
  
  coldkeep restore "$SAMPLE_FILE_ID" "$RESTORE_DIR1/" > /dev/null
  coldkeep restore "$SAMPLE_FILE_ID" "$RESTORE_DIR2/" > /dev/null
  
  # Find and compare the restored files
  RESTORED_FILE1=$(find "$RESTORE_DIR1" -type f -print -quit)
  RESTORED_FILE2=$(find "$RESTORE_DIR2" -type f -print -quit)
  
  if [[ -n "$RESTORED_FILE1" && -n "$RESTORED_FILE2" ]]; then
    HASH1=$(sha256sum "$RESTORED_FILE1" | awk '{print $1}')
    HASH2=$(sha256sum "$RESTORED_FILE2" | awk '{print $1}')
    
    if [[ "$HASH1" == "$HASH2" ]]; then
      echo "[smoke]   ok: multiple restores are deterministic (same hash: $HASH1)"
    else
      echo "[smoke] ERROR: multiple restores produced different hashes: $HASH1 vs $HASH2"
      exit 1
    fi
  fi
  
  rm -rf "$RESTORE_DIR1" "$RESTORE_DIR2"
fi

echo ""
echo "[smoke] === LARGE FILE HANDLING TEST ==="

# Disabled by default to keep normal smoke runs fast.
# Enable with: COLDKEEP_SMOKE_ENABLE_LARGE_FILE_TEST=1
: "${COLDKEEP_SMOKE_ENABLE_LARGE_FILE_TEST:=0}"
: "${COLDKEEP_SMOKE_LARGE_FILE_MB:=100}"

if [[ "${COLDKEEP_SMOKE_ENABLE_LARGE_FILE_TEST}" == "1" ]]; then
  TEST_LARGE_FILE="./_smoke_test_large_${COLDKEEP_SMOKE_LARGE_FILE_MB}mb.bin"
  LARGE_RESTORE_DIR="./_smoke_large_restored"

  echo "[smoke] generating ${COLDKEEP_SMOKE_LARGE_FILE_MB}MB test file..."
  dd if=/dev/zero of="$TEST_LARGE_FILE" bs=1M count="${COLDKEEP_SMOKE_LARGE_FILE_MB}" status=none

  echo "[smoke] storing large file"
  if ! LARGE_STORE_OUTPUT=$(coldkeep store "$TEST_LARGE_FILE" --output json 2>&1); then
    echo "[smoke] ERROR: failed to store large file"
    echo "$LARGE_STORE_OUTPUT"
    rm -rf "$TEST_LARGE_FILE" "$LARGE_RESTORE_DIR"
    exit 1
  fi

  LARGE_STORE_PAYLOAD=$(echo "$LARGE_STORE_OUTPUT" | grep -E '^\{.*\}$' | tail -n1)
  LARGE_ID=$(echo "$LARGE_STORE_PAYLOAD" | jq -r '.data.file_id // .file_id // empty' 2>/dev/null)
  if [[ -z "$LARGE_ID" ]]; then
    echo "[smoke] ERROR: store command did not return valid file_id for large file"
    echo "$LARGE_STORE_OUTPUT"
    rm -rf "$TEST_LARGE_FILE" "$LARGE_RESTORE_DIR"
    exit 1
  fi

  echo "[smoke] restoring large file id=${LARGE_ID}"
  mkdir -p "$LARGE_RESTORE_DIR"
  if ! coldkeep restore "$LARGE_ID" "$LARGE_RESTORE_DIR/" > /dev/null 2>&1; then
    echo "[smoke] ERROR: restore command failed for large file id=${LARGE_ID}"
    rm -rf "$TEST_LARGE_FILE" "$LARGE_RESTORE_DIR"
    exit 1
  fi

  RESTORED_LARGE=$(find "$LARGE_RESTORE_DIR" -type f -print -quit)
  if [[ -z "$RESTORED_LARGE" ]]; then
    echo "[smoke] ERROR: restore produced no output file for large file id=${LARGE_ID}"
    rm -rf "$TEST_LARGE_FILE" "$LARGE_RESTORE_DIR"
    exit 1
  fi

  ORIG_HASH=$(sha256sum "$TEST_LARGE_FILE" | awk '{print $1}')
  REST_HASH=$(sha256sum "$RESTORED_LARGE" | awk '{print $1}')
  if [[ "$ORIG_HASH" != "$REST_HASH" ]]; then
    echo "[smoke] ERROR: large file hash mismatch: want=${ORIG_HASH} got=${REST_HASH}"
    rm -rf "$TEST_LARGE_FILE" "$LARGE_RESTORE_DIR"
    exit 1
  fi

  echo "[smoke] large file handling test PASSED (${COLDKEEP_SMOKE_LARGE_FILE_MB}MB)"
  rm -rf "$TEST_LARGE_FILE" "$LARGE_RESTORE_DIR"
else
  echo "[smoke] large file handling test skipped (set COLDKEEP_SMOKE_ENABLE_LARGE_FILE_TEST=1 to enable)"
fi

echo ""
echo "[smoke] === CODEC VARIANT TEST ==="
CURRENT_CODEC="${COLDKEEP_CODEC:-plain}"
if [[ "$CURRENT_CODEC" == "plain" ]] && [[ -n "${COLDKEEP_KEY:-}" ]]; then
  echo "[smoke] testing aes-gcm codec variant (COLDKEEP_KEY is set)"

  AES_RESTORE_DIR="./_smoke_aes_restore"
  rm -rf "$AES_RESTORE_DIR"
  mkdir -p "$AES_RESTORE_DIR"

  # Store a known file with aes-gcm; already-stored is fine — we still get status+file_id
  aes_store_out=$(COLDKEEP_CODEC=aes-gcm coldkeep store samples/hello.txt --output json 2>/dev/null | grep '^\{' | tail -n1)
  aes_store_status=$(echo "$aes_store_out" | jq -r '.status // empty')
  aes_store_cmd=$(echo "$aes_store_out"   | jq -r '.command // empty')
  aes_file_id=$(echo "$aes_store_out"     | jq -r '.data.file_id // empty')

  if [[ "$aes_store_status" != "ok" ]]; then
    echo "[smoke] FAIL: aes-gcm store returned status='$aes_store_status' (expected 'ok')"
    exit 1
  fi
  if [[ "$aes_store_cmd" != "store" ]]; then
    echo "[smoke] FAIL: aes-gcm store returned command='$aes_store_cmd' (expected 'store')"
    exit 1
  fi
  if [[ -z "$aes_file_id" ]] || [[ "$aes_file_id" == "null" ]]; then
    echo "[smoke] FAIL: aes-gcm store did not return a valid file_id"
    exit 1
  fi
  echo "[smoke]   ok: aes-gcm store succeeded (file_id=$aes_file_id)"

  # Verify the full system with aes-gcm
  aes_verify_out=$(COLDKEEP_CODEC=aes-gcm coldkeep verify system --full --output json 2>/dev/null | grep '^\{' | tail -n1)
  aes_verify_status=$(echo "$aes_verify_out" | jq -r '.status // empty')
  aes_verify_cmd=$(echo "$aes_verify_out"    | jq -r '.command // empty')
  aes_verify_level=$(echo "$aes_verify_out"  | jq -r '.level // empty')

  if [[ "$aes_verify_status" != "ok" ]]; then
    echo "[smoke] FAIL: aes-gcm verify returned status='$aes_verify_status' (expected 'ok')"
    exit 1
  fi
  if [[ "$aes_verify_cmd" != "verify" ]]; then
    echo "[smoke] FAIL: aes-gcm verify returned command='$aes_verify_cmd' (expected 'verify')"
    exit 1
  fi
  if [[ "$aes_verify_level" != "full" ]]; then
    echo "[smoke] FAIL: aes-gcm verify returned level='$aes_verify_level' (expected 'full')"
    exit 1
  fi
  echo "[smoke]   ok: aes-gcm verify system --full passed"

  # Restore with aes-gcm and assert the restored_hash matches the original file
  aes_restore_out=$(COLDKEEP_CODEC=aes-gcm coldkeep restore "$aes_file_id" "$AES_RESTORE_DIR/" --output json 2>/dev/null | grep '^\{' | tail -n1)
  aes_restore_status=$(echo "$aes_restore_out" | jq -r '.status // empty')
  aes_restore_hash=$(echo "$aes_restore_out"   | jq -r '.data.restored_hash // empty')
  expected_hello_hash=$(sha256sum samples/hello.txt | awk '{print $1}')

  if [[ "$aes_restore_status" != "ok" ]]; then
    echo "[smoke] FAIL: aes-gcm restore returned status='$aes_restore_status' (expected 'ok')"
    exit 1
  fi
  if [[ "$aes_restore_hash" != "$expected_hello_hash" ]]; then
    echo "[smoke] FAIL: aes-gcm restore hash mismatch: got '$aes_restore_hash', expected '$expected_hello_hash'"
    exit 1
  fi
  echo "[smoke]   ok: aes-gcm restore hash matches original (${aes_restore_hash:0:16}…)"

  rm -rf "$AES_RESTORE_DIR"
  echo "[smoke] codec variant test PASSED (aes-gcm store/verify/restore round-trip)"
else
  echo "[smoke] codec variant test skipped (COLDKEEP_CODEC is not plain, or COLDKEEP_KEY is not set)"
fi

echo "[smoke] done"
