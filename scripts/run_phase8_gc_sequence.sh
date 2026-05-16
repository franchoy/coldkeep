#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/run_phase8_gc_sequence.sh <BLOCK_MB> <DATASET_F_ROOT> <RUN_ID> [options]

Dataset F GC sequence:
  1) store many small files (DATASET_F_ROOT/files/)
  2) remove a random subset (~30% by default)
  3) run simulate gc --output json (dry-run; record expected reclaimable + retained dead bytes)
  4) run gc (live)
  5) verify system --standard
  6) restore remaining (non-removed) files and validate

Arguments:
  BLOCK_MB           block target size in MiB (e.g. 1 or 2)
  DATASET_F_ROOT     root containing files/ subdirectory with small files
  RUN_ID             run identifier

Options:
  --dataset-label <label>     label for artifact names (default: basename(DATASET_F_ROOT))
  --output-dir <path>         output directory (default: tmp/bench_phase8_gc_sequence)
  --bin <path>                coldkeep binary path (default: coldkeep)
  --remove-ratio <r>          fraction of stored files to remove, 0.0–1.0 (default: 0.30)
    --remove-filter <substr>    restrict removal candidates to paths containing substr (default: all files)
    --random-seed <n>           seed for removal subset selection (default: derived from run_id)

Required env:
  DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE

Optional env:
  DB_NAME_BASE                base DB prefix (default: coldkeep_bench)
  COLDKEEP_KEY                encryption key (default: deterministic test key)
  COLDKEEP_DB_OPERATION_TIMEOUT_MS (default: 1800000)

DB/storage isolation convention:
  DB_NAME=<DB_NAME_BASE>_<BLOCK_MB>m_<DATASET_LABEL>_<RUN_ID>
  COLDKEEP_STORAGE_DIR=/tmp/coldkeep-bench-<BLOCK_MB>m-<DATASET_LABEL>-<RUN_ID>
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
DATASET_F_ROOT="$2"
RUN_ID="$3"
shift 3

# Argument validation helpers (Phase 10 hardening)
validate_identifier() {
  local value="$1" name="$2"
  [[ -z "$value" ]] && { echo "error: $name is empty" >&2; return 1; }
  [[ "$value" =~ [[:space:]\;\|\&\<\>\$\`\(\)\[\]\{\}\\\*\?\!] ]] && \
    { echo "error: $name contains unsafe characters: $value" >&2; return 1; }
  [[ "$value" =~ \.\. ]] && \
    { echo "error: $name contains path traversal: $value" >&2; return 1; }
  return 0
}
validate_relative_path() {
  local value="$1" name="$2"
  [[ -z "$value" ]] && { echo "error: $name is empty" >&2; return 1; }
  [[ "$value" =~ ^/ ]] && \
    { echo "error: $name must be relative (not absolute): $value" >&2; return 1; }
  [[ "$value" =~ \.\. ]] && \
    { echo "error: $name contains path traversal: $value" >&2; return 1; }
  [[ "$value" =~ [[:space:]] ]] && \
    { echo "error: $name contains spaces: $value" >&2; return 1; }
  return 0
}

validate_identifier "$RUN_ID" "RUN_ID" || exit 1

DATASET_LABEL="$(basename "$DATASET_F_ROOT")"
OUT_DIR="tmp/bench_phase8_gc_sequence"
COLDKEEP_BIN="coldkeep"
REMOVE_RATIO="0.30"
RANDOM_SEED=""
REMOVE_FILTER=""

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dataset-label)
			DATASET_LABEL="$2"
			shift 2
			;;
		--output-dir)
			OUT_DIR="$2"
			validate_relative_path "$OUT_DIR" "--output-dir" || exit 1
			shift 2
			;;
		--bin)
			COLDKEEP_BIN="$2"
			shift 2
			;;
		--remove-ratio)
			REMOVE_RATIO="$2"
			shift 2
			;;
        --remove-filter)
            REMOVE_FILTER="$2"
            shift 2
            ;;
		--random-seed)
			RANDOM_SEED="$2"
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
if ! command -v python3 >/dev/null 2>&1; then
	echo "python3 is required" >&2
	exit 1
fi
if ! [[ "$BLOCK_MB" =~ ^[0-9]+$ ]] || [[ "$BLOCK_MB" -le 0 ]]; then
	echo "BLOCK_MB must be a positive integer" >&2
	exit 1
fi
FILES_DIR="$DATASET_F_ROOT/files"
if [[ ! -d "$FILES_DIR" ]]; then
	echo "dataset root must contain files/ subdirectory: $DATASET_F_ROOT" >&2
	exit 1
fi

DB_NAME_BASE_EFFECTIVE="${DB_NAME_BASE:-coldkeep_bench}"
RUN_DB_NAME="${DB_NAME_BASE_EFFECTIVE}_${BLOCK_MB}m_${DATASET_LABEL}_${RUN_ID}"
RUN_STORAGE_DIR="/tmp/coldkeep-bench-${BLOCK_MB}m-${DATASET_LABEL}-${RUN_ID}"

mkdir -p "$OUT_DIR"
ARTIFACT_PREFIX="$OUT_DIR/${DATASET_LABEL}-${BLOCK_MB}m-${RUN_ID}"
LIST_JSON_PATH="${ARTIFACT_PREFIX}-list.json"
REMOVAL_JSON_PATH="${ARTIFACT_PREFIX}-removal.json"
SIMULATE_GC_JSON_PATH="${ARTIFACT_PREFIX}-simulate-gc.json"
GC_LIVE_JSON_PATH="${ARTIFACT_PREFIX}-gc-live.json"
RESTORE_ROOT="${ARTIFACT_PREFIX}-restore"
RESULT_JSON_PATH="${ARTIFACT_PREFIX}-gc-result.json"

export COLDKEEP_BLOCK_TARGET_SIZE_MB="$BLOCK_MB"
export COLDKEEP_STORAGE_DIR="$RUN_STORAGE_DIR"
export COLDKEEP_TEST_DB="1"
export DB_NAME="$RUN_DB_NAME"
export COLDKEEP_DB_AUTO_BOOTSTRAP="true"
export COLDKEEP_CODEC="plain"
export COLDKEEP_KEY="${COLDKEEP_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"
export COLDKEEP_DB_OPERATION_TIMEOUT_MS="${COLDKEEP_DB_OPERATION_TIMEOUT_MS:-1800000}"

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

# Fresh isolated run context.
rm -rf "$RUN_STORAGE_DIR" "$RESTORE_ROOT"
PGPASSWORD="$DB_PASSWORD" dropdb --if-exists -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$RUN_DB_NAME" 2>/dev/null || true
PGPASSWORD="$DB_PASSWORD" createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$RUN_DB_NAME"
mkdir -p "$RESTORE_ROOT"

# 1) store many small files
store_start_ms="$(now_ms)"
run_with_rss_kb "$COLDKEEP_BIN" store-folder "$FILES_DIR" >/dev/null
store_rss_kb="$_last_rss_kb"
store_end_ms="$(now_ms)"
store_elapsed_ms=$((store_end_ms - store_start_ms))

"$COLDKEEP_BIN" list --output json >"$LIST_JSON_PATH"

seed_for_removal="$RUN_ID"
if [[ -n "$RANDOM_SEED" ]]; then
	seed_for_removal="$RANDOM_SEED"
fi

# 2) remove a random subset (~REMOVE_RATIO)
python3 - "$LIST_JSON_PATH" "$REMOVAL_JSON_PATH" "$REMOVE_RATIO" "$seed_for_removal" "$REMOVE_FILTER" <<'PY'
import json
import math
import os
import random
import sys

list_path, removal_out_path, remove_ratio_raw, seed_raw = sys.argv[1:5]
remove_ratio = float(remove_ratio_raw)
seed_int = 0
try:
    seed_int = int(seed_raw)
except ValueError:
    import hashlib
    seed_int = int(hashlib.sha256(seed_raw.encode()).hexdigest(), 16) % (2 ** 32)

with open(list_path, 'r', encoding='utf-8') as f:
    doc = json.load(f)

files = doc.get('files') or []
entries = sorted(
    {(int(x['id']), str(x.get('name', '')).strip()) for x in files
     if x.get('id') and str(x.get('name', '')).strip()},
    key=lambda e: e[1],
)
if not entries:
    raise SystemExit('no stored files found')

rng = random.Random(seed_int)
n_remove = max(1, math.floor(len(entries) * remove_ratio))
remove_filter = sys.argv[5] if len(sys.argv) > 5 else ''
candidates = [e for e in entries if not remove_filter or remove_filter in e[1]]
if not candidates:
    raise SystemExit(f'no stored paths match --remove-filter {remove_filter!r}')
n_remove = max(1, math.floor(len(candidates) * remove_ratio))
to_remove_entries = sorted(rng.sample(candidates, n_remove), key=lambda e: e[1])
to_remove_ids = [e[0] for e in to_remove_entries]
to_remove_paths = [e[1] for e in to_remove_entries]
to_keep = sorted({e[1] for e in entries} - set(to_remove_paths))

out = {
    'stored_paths': [e[1] for e in entries],
    'to_remove': to_remove_paths,
    'to_remove_ids': to_remove_ids,
    'to_keep': to_keep,
    'remove_ratio_requested': remove_ratio,
    'remove_count': n_remove,
    'keep_count': len(to_keep),
}

with open(removal_out_path, 'w', encoding='utf-8') as f:
    json.dump(out, f, indent=2, sort_keys=True)
    f.write('\n')
PY

remove_start_ms="$(now_ms)"
python3 - "$REMOVAL_JSON_PATH" <<'PY' | while IFS= read -r file_id; do
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as f:
    doc = json.load(f)
for fid in (doc.get('to_remove_ids') or []):
    print(fid)
PY
	"$COLDKEEP_BIN" remove "$file_id" --output json >/dev/null
done
remove_end_ms="$(now_ms)"
remove_elapsed_ms=$((remove_end_ms - remove_start_ms))

# 3) simulate gc (dry-run) — record expected reclaimable bytes and retained dead bytes
"$COLDKEEP_BIN" simulate gc --output json >"$SIMULATE_GC_JSON_PATH"

# 4) gc live
gc_start_ms="$(now_ms)"
"$COLDKEEP_BIN" gc --output json >"$GC_LIVE_JSON_PATH"
gc_end_ms="$(now_ms)"
gc_elapsed_ms=$((gc_end_ms - gc_start_ms))

# 5) verify
verify_start_ms="$(now_ms)"
run_with_rss_kb "$COLDKEEP_BIN" verify system --standard >/dev/null
verify_rss_kb="$_last_rss_kb"
verify_end_ms="$(now_ms)"
verify_elapsed_ms=$((verify_end_ms - verify_start_ms))

# 6) restore remaining files and validate
restore_start_ms="$(now_ms)"
python3 - "$REMOVAL_JSON_PATH" <<'PY' | while IFS= read -r stored_path; do
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as f:
    doc = json.load(f)
for p in (doc.get('to_keep') or []):
    print(p)
PY
	"$COLDKEEP_BIN" restore --stored-path "$stored_path" --mode prefix --destination "$RESTORE_ROOT" --overwrite --output json >/dev/null
done
restore_end_ms="$(now_ms)"
restore_elapsed_ms=$((restore_end_ms - restore_start_ms))

python3 - \
	"$RESULT_JSON_PATH" \
	"$REMOVAL_JSON_PATH" \
	"$SIMULATE_GC_JSON_PATH" \
	"$GC_LIVE_JSON_PATH" \
	"$RESTORE_ROOT" \
	"$BLOCK_MB" "$DATASET_LABEL" "$RUN_ID" "$RUN_DB_NAME" "$RUN_STORAGE_DIR" \
	"$store_elapsed_ms" "$remove_elapsed_ms" "$gc_elapsed_ms" "$verify_elapsed_ms" "$restore_elapsed_ms" \
	"$store_rss_kb" "$verify_rss_kb" "$seed_for_removal" <<'PY'
import hashlib
import json
import os
import sys

(
    out_path,
    removal_path,
    simulate_gc_path,
    gc_live_path,
    restore_root,
    block_mb,
    dataset_label,
    run_id,
    db_name,
    storage_dir,
    store_elapsed_ms,
    remove_elapsed_ms,
    gc_elapsed_ms,
    verify_elapsed_ms,
    restore_elapsed_ms,
    store_rss_kb,
    verify_rss_kb,
    dataset_seed,
) = sys.argv[1:]

with open(removal_path, 'r', encoding='utf-8') as f:
    removal = json.load(f)

with open(simulate_gc_path, 'r', encoding='utf-8') as f:
    sim_gc = json.load(f)

with open(gc_live_path, 'r', encoding='utf-8') as f:
    gc_live = json.load(f)

# extract simulate gc metrics
gc_summary = ((sim_gc.get('data') or sim_gc).get('gc') or {}).get('summary') or sim_gc.get('summary') or {}

def _int(d, *keys):
    for k in keys:
        v = d.get(k)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return 0

logically_reclaimable_bytes = _int(gc_summary, 'logically_reclaimable_bytes')
physically_reclaimable_bytes = _int(gc_summary, 'physically_reclaimable_bytes')
retained_dead_bytes = _int(gc_summary, 'retained_dead_bytes_due_to_packed_blocks')
packed_blocks_dead = _int(gc_summary, 'packed_blocks_dead')
packed_bytes_reclaimable = _int(gc_summary, 'packed_bytes_reclaimable')
fully_reclaimable_containers = _int(gc_summary, 'fully_reclaimable_containers')

# After GC the retained_dead_bytes represent exactly the space that cannot be
# reclaimed because packed blocks contain both live and dead chunks — the
# key block-size comparison metric.


def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def norm_restore(root, stored_path):
    rel = stored_path
    drive, tail = os.path.splitdrive(rel)
    if drive:
        rel = tail
    rel = rel.lstrip('/\\')
    return os.path.join(os.path.abspath(root), rel)


to_keep = removal.get('to_keep') or []
restore_mismatches = []
restore_missing = []
for stored in to_keep:
    if not os.path.isfile(stored):
        restore_mismatches.append({'stored_path': stored, 'reason': 'source_missing'})
        continue
    out = norm_restore(restore_root, stored)
    if not os.path.isfile(out):
        restore_missing.append(stored)
        continue
    src_hash = hash_file(stored)
    dst_hash = hash_file(out)
    if src_hash != dst_hash:
        restore_mismatches.append({'stored_path': stored, 'reason': 'hash_mismatch', 'source': src_hash, 'restored': dst_hash})

if restore_missing:
    raise SystemExit(f'restore incomplete: {len(restore_missing)} remaining files not found in restore root')
if restore_mismatches:
    raise SystemExit(f'restore validation failed: {len(restore_mismatches)} hash mismatches')

result = {
    'status': 'ok',
    'phase': 'phase8_gc_sequence',
    'data': {
        'block_target_size_mb': int(block_mb),
        'dataset': dataset_label,
        'run_id': run_id,
        'dataset_seed': dataset_seed,
        'db_name': db_name,
        'storage_dir': storage_dir,
        'timings_ms': {
            'store': int(store_elapsed_ms),
            'remove_subset': int(remove_elapsed_ms),
            'gc': int(gc_elapsed_ms),
            'verify': int(verify_elapsed_ms),
            'restore_remaining': int(restore_elapsed_ms),
        },
        'memory': {
            'store_peak_rss_kb': int(store_rss_kb) if store_rss_kb else None,
            'verify_peak_rss_kb': int(verify_rss_kb) if verify_rss_kb else None,
        },
        'removal': {
            'total_stored': len(removal.get('stored_paths') or []),
            'removed_count': int(removal.get('remove_count', 0)),
            'kept_count': int(removal.get('keep_count', 0)),
            'remove_ratio_requested': float(removal.get('remove_ratio_requested', 0.0)),
        },
        'gc_simulation': {
            'logically_reclaimable_bytes': logically_reclaimable_bytes,
            'physically_reclaimable_bytes': physically_reclaimable_bytes,
            'retained_dead_bytes_due_to_packed_blocks': retained_dead_bytes,
            'packed_blocks_dead': packed_blocks_dead,
            'packed_bytes_reclaimable': packed_bytes_reclaimable,
            'fully_reclaimable_containers': fully_reclaimable_containers,
        },
        'gc_live': {
            'affected_containers': _int(
                (gc_live.get('data') or {}), 'affected_containers'
            ),
        },
        'restore_validation': {
            'files_restored': len(to_keep),
            'no_hash_mismatch': len(restore_mismatches) == 0,
            'hash_mismatch_count': len(restore_mismatches),
        },
    },
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2, sort_keys=True)
    f.write('\n')
PY

echo "phase8 gc-sequence complete: ${RESULT_JSON_PATH}"
