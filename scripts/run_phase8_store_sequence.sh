#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/run_phase8_store_sequence.sh <BLOCK_MB> <DATASET_PATH> <RUN_ID> [options]

Runs the Phase 8 store benchmark sequence for one candidate/dataset/run:
  1) initialize fresh repo context (fresh DB name + fresh storage dir)
  2) store dataset (store or store-folder)
  3) record store elapsed time
  4) collect stats (JSON)
  5) run verify system --standard
  6) record verify elapsed time

Options:
  --dataset-label <label>    Label used in artifact names (default: basename(DATASET_PATH))
  --expect-multichunk        Enforce avg_chunks_per_block > 1 check
  --output-dir <path>        Artifact output directory (default: tmp/bench_phase8_store_sequence)
  --bin <path>               coldkeep binary path (default: coldkeep from PATH)

Required env:
  DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE

Optional env:
  DB_NAME_BASE               Base name for per-run DB (default: coldkeep_bench)
  COLDKEEP_KEY               Encryption key (default: deterministic test key)
  COLDKEEP_DB_OPERATION_TIMEOUT_MS (default: 1800000)

This harness enforces no DB reuse by generating a unique DB name per run:
  <DB_NAME_BASE>_<BLOCK_MB>m_<DATASET_LABEL>_<RUN_ID>
EOF
}

if [[ $# -lt 3 ]]; then
  if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
  fi
	usage >&2
	exit 1
fi

BLOCK_MB="$1"
DATASET_PATH="$2"
RUN_ID="$3"
shift 3

DATASET_LABEL="$(basename "$DATASET_PATH")"
OUT_DIR="tmp/bench_phase8_store_sequence"
COLDKEEP_BIN="coldkeep"
EXPECT_MULTI_CHUNK=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dataset-label)
			DATASET_LABEL="$2"
			shift 2
			;;
		--expect-multichunk)
			EXPECT_MULTI_CHUNK=1
			shift
			;;
		--output-dir)
			OUT_DIR="$2"
			shift 2
			;;
		--bin)
			COLDKEEP_BIN="$2"
			shift 2
			;;
		--help|-h)
			usage
			exit 0
			;;
		*)
			echo "unknown option: $1" >&2
			usage >&2
			exit 1
			;;
	esac
done

for var_name in DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE; do
	if [[ -z "${!var_name:-}" ]]; then
		echo "missing required env: $var_name" >&2
		exit 1
	fi
done

if ! command -v "$COLDKEEP_BIN" >/dev/null 2>&1; then
	echo "coldkeep binary not found: $COLDKEEP_BIN" >&2
	exit 1
fi
if ! command -v psql >/dev/null 2>&1; then
	echo "psql is required for block_hash integrity check" >&2
	exit 1
fi
if [[ ! -e "$DATASET_PATH" ]]; then
	echo "dataset path not found: $DATASET_PATH" >&2
	exit 1
fi
if ! [[ "$BLOCK_MB" =~ ^[0-9]+$ ]] || [[ "$BLOCK_MB" -le 0 ]]; then
	echo "BLOCK_MB must be a positive integer" >&2
	exit 1
fi

DB_NAME_BASE_EFFECTIVE="${DB_NAME_BASE:-coldkeep_bench}"
RUN_DB_NAME="${DB_NAME_BASE_EFFECTIVE}_${BLOCK_MB}m_${DATASET_LABEL}_${RUN_ID}"
RUN_STORAGE_DIR="/tmp/coldkeep-bench-${BLOCK_MB}m-${DATASET_LABEL}-${RUN_ID}"

mkdir -p "$OUT_DIR"
ARTIFACT_PREFIX="$OUT_DIR/${DATASET_LABEL}-${BLOCK_MB}m-${RUN_ID}"
STATS_JSON_PATH="${ARTIFACT_PREFIX}-stats.json"
RESULT_JSON_PATH="${ARTIFACT_PREFIX}-result.json"

export COLDKEEP_BLOCK_TARGET_SIZE_MB="$BLOCK_MB"
export COLDKEEP_STORAGE_DIR="$RUN_STORAGE_DIR"
export COLDKEEP_TEST_DB="1"
export DB_NAME="$RUN_DB_NAME"
export COLDKEEP_DB_AUTO_BOOTSTRAP="true"
export COLDKEEP_CODEC="plain"
export COLDKEEP_KEY="${COLDKEEP_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"
export COLDKEEP_DB_OPERATION_TIMEOUT_MS="${COLDKEEP_DB_OPERATION_TIMEOUT_MS:-1800000}"

# 1) initialize fresh repo context
rm -rf "$RUN_STORAGE_DIR"

store_cmd=("$COLDKEEP_BIN")
if [[ -d "$DATASET_PATH" ]]; then
	store_cmd+=("store-folder" "$DATASET_PATH")
else
	store_cmd+=("store" "$DATASET_PATH")
fi

now_ms() {
	python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

# run_with_rss_kb: runs command, captures peak RSS into _last_rss_kb (kb).
# Requires GNU /usr/bin/time -v. Falls back to 0 when unavailable.
run_with_rss_kb() {
	local _rss_tmp
	_rss_tmp="$(mktemp)"
	if /usr/bin/time -v "$@" 2>"$_rss_tmp"; then
		_last_rss_kb="$(awk '/Maximum resident set size/{print $NF}' "$_rss_tmp")"
		_last_rss_kb="${_last_rss_kb:-0}"
	else
		local _exit_code=$?
		rm -f "$_rss_tmp"
		return "$_exit_code"
	fi
	rm -f "$_rss_tmp"
}
_last_rss_kb=0

# 2-3) store dataset and record elapsed
store_start_ms="$(now_ms)"
run_with_rss_kb "${store_cmd[@]}" >/dev/null
store_rss_kb="$_last_rss_kb"
store_end_ms="$(now_ms)"
store_elapsed_ms=$((store_end_ms - store_start_ms))

# 4) collect stats
"$COLDKEEP_BIN" stats --output json >"$STATS_JSON_PATH"

read -r storage_blocks_count avg_chunks_per_block <<<"$(python3 - "$STATS_JSON_PATH" <<'PY'
import json,sys
path=sys.argv[1]
with open(path,'r',encoding='utf-8') as f:
    doc=json.load(f)
layout=((doc.get('data') or {}).get('block_layout') or {})
print(int(layout.get('storage_blocks_count',0)), float(layout.get('avg_chunks_per_block',0.0)))
PY
)"

# 5-6) verify and record elapsed
verify_start_ms="$(now_ms)"
run_with_rss_kb "$COLDKEEP_BIN" verify system --standard >/dev/null
verify_rss_kb="$_last_rss_kb"
verify_end_ms="$(now_ms)"
verify_elapsed_ms=$((verify_end_ms - verify_start_ms))

# Expected checks
if [[ "$storage_blocks_count" -le 0 ]]; then
	echo "check failed: storage_blocks_count must be > 0, got $storage_blocks_count" >&2
	exit 1
fi

if [[ "$EXPECT_MULTI_CHUNK" -eq 1 ]]; then
	python3 - "$avg_chunks_per_block" <<'PY'
import sys
v=float(sys.argv[1])
if not (v > 1.0):
    raise SystemExit(f"check failed: avg_chunks_per_block must be > 1 for multi-chunk dataset, got {v}")
PY
fi

missing_block_hash_count="$(psql "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} password=${DB_PASSWORD} dbname=${DB_NAME} sslmode=${DB_SSLMODE}" -t -A -c "SELECT COUNT(*) FROM storage_blocks WHERE block_hash IS NULL OR octet_length(block_hash)=0")"
if [[ "$missing_block_hash_count" != "0" ]]; then
	echo "check failed: block_hash missing for $missing_block_hash_count packed blocks" >&2
	exit 1
fi

python3 - "$RESULT_JSON_PATH" "$BLOCK_MB" "$DATASET_LABEL" "$RUN_ID" "$RUN_DB_NAME" "$RUN_STORAGE_DIR" "$store_elapsed_ms" "$verify_elapsed_ms" "$storage_blocks_count" "$avg_chunks_per_block" "$missing_block_hash_count" "$EXPECT_MULTI_CHUNK" "$store_rss_kb" "$verify_rss_kb" "$RUN_ID" <<'PY'
import json,sys
(
    out_path,
    block_mb,
    dataset_label,
    run_id,
    db_name,
    storage_dir,
    store_elapsed_ms,
    verify_elapsed_ms,
    storage_blocks_count,
    avg_chunks_per_block,
    missing_block_hash_count,
    expect_multichunk,
    store_rss_kb,
    verify_rss_kb,
    dataset_seed,
)=sys.argv[1:]
payload={
    "status":"ok",
    "phase":"phase8_store_sequence",
    "data":{
        "block_target_size_mb":int(block_mb),
        "dataset":dataset_label,
        "run_id":run_id,
        "dataset_seed":dataset_seed,
        "db_name":db_name,
        "storage_dir":storage_dir,
        "store_elapsed_ms":int(store_elapsed_ms),
        "verify_elapsed_ms":int(verify_elapsed_ms),
        "memory":{
            "store_peak_rss_kb":int(store_rss_kb) if store_rss_kb else None,
            "verify_peak_rss_kb":int(verify_rss_kb) if verify_rss_kb else None,
        },
        "checks":{
            "store_succeeds":True,
            "verify_passes":True,
            "storage_blocks_count_gt_zero": int(storage_blocks_count) > 0,
            "avg_chunks_per_block": float(avg_chunks_per_block),
            "avg_chunks_per_block_gt_one_required": bool(int(expect_multichunk)),
            "missing_block_hash_count": int(missing_block_hash_count),
        },
    },
}
with open(out_path,'w',encoding='utf-8') as f:
    json.dump(payload,f,indent=2,sort_keys=True)
    f.write("\n")
PY

echo "phase8 store-sequence complete: ${RESULT_JSON_PATH}"
