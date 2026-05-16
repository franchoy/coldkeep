#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/run_phase8_restore_sequence.sh <BLOCK_MB> <DATASET_PATH> <RUN_ID> [options]

Runs the Phase 8 restore benchmark sequence for one stored repo tuple.
The script assumes the repo from the matching store run already exists.

Full restore sequence:
  1) restore full dataset (via stored paths)
  2) compare file hashes + tree hash
  3) record elapsed time
  4) collect read/cache signals when available

Selective restore sequence:
  - one small file
  - 100 random small files
  - one subdirectory

Expected checks:
  - restored bytes match original bytes
  - read amplification is measured
  - no hash mismatch

Options:
  --dataset-label <label>    Label used in artifact names (default: basename(DATASET_PATH))
  --output-dir <path>        Artifact output directory (default: tmp/bench_phase8_restore_sequence)
  --bin <path>               coldkeep binary path (default: coldkeep from PATH)
  --small-file-max-bytes <n> Small-file threshold (default: 65536)
  --random-seed <n>          Seed for random small-file selection (default: derived from run id)
  --allow-fewer-than-100     Allow selective 100-file case to run with fewer files

Required env:
  DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE

Optional env:
  DB_NAME_BASE               Base DB prefix from store sequence (default: coldkeep_bench)
  COLDKEEP_DB_OPERATION_TIMEOUT_MS (default: 1800000)

DB/storage isolation convention (must match store sequence):
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
DATASET_PATH="$2"
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

DATASET_LABEL="$(basename "$DATASET_PATH")"
OUT_DIR="tmp/bench_phase8_restore_sequence"
COLDKEEP_BIN="coldkeep"
SMALL_FILE_MAX_BYTES=65536
ALLOW_FEWER_THAN_100=0
RANDOM_SEED=""
SKIP_SELECTIVE=0

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
		--small-file-max-bytes)
			SMALL_FILE_MAX_BYTES="$2"
			shift 2
			;;
		--random-seed)
			RANDOM_SEED="$2"
			shift 2
			;;
		--allow-fewer-than-100)
			ALLOW_FEWER_THAN_100=1
			shift
			;;
		--skip-selective)
			SKIP_SELECTIVE=1
			shift
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
if [[ ! -e "$DATASET_PATH" ]]; then
	echo "dataset path not found: $DATASET_PATH" >&2
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
if ! [[ "$SMALL_FILE_MAX_BYTES" =~ ^[0-9]+$ ]] || [[ "$SMALL_FILE_MAX_BYTES" -le 0 ]]; then
	echo "--small-file-max-bytes must be a positive integer" >&2
	exit 1
fi

DB_NAME_BASE_EFFECTIVE="${DB_NAME_BASE:-coldkeep_bench}"
RUN_DB_NAME="${DB_NAME_BASE_EFFECTIVE}_${BLOCK_MB}m_${DATASET_LABEL}_${RUN_ID}"
RUN_STORAGE_DIR="/tmp/coldkeep-bench-${BLOCK_MB}m-${DATASET_LABEL}-${RUN_ID}"

export DB_NAME="$RUN_DB_NAME"
export COLDKEEP_STORAGE_DIR="$RUN_STORAGE_DIR"
export COLDKEEP_TEST_DB="1"
export COLDKEEP_DB_AUTO_BOOTSTRAP="true"
export COLDKEEP_DB_OPERATION_TIMEOUT_MS="${COLDKEEP_DB_OPERATION_TIMEOUT_MS:-1800000}"

mkdir -p "$OUT_DIR"
ARTIFACT_PREFIX="$OUT_DIR/${DATASET_LABEL}-${BLOCK_MB}m-${RUN_ID}"
LIST_JSON_PATH="${ARTIFACT_PREFIX}-list.json"
SELECTION_JSON_PATH="${ARTIFACT_PREFIX}-selection.json"
RESULT_JSON_PATH="${ARTIFACT_PREFIX}-restore-result.json"
FULL_IO_JSONL_PATH="${ARTIFACT_PREFIX}-full-io.jsonl"
SEL1_IO_JSONL_PATH="${ARTIFACT_PREFIX}-sel-single-io.jsonl"
SEL100_IO_JSONL_PATH="${ARTIFACT_PREFIX}-sel-100-io.jsonl"
SELDIR_IO_JSONL_PATH="${ARTIFACT_PREFIX}-sel-subdir-io.jsonl"

FULL_RESTORE_ROOT="${ARTIFACT_PREFIX}-restore-full"
SEL_SINGLE_ROOT="${ARTIFACT_PREFIX}-restore-sel-single"
SEL_100_ROOT="${ARTIFACT_PREFIX}-restore-sel-100"
SEL_SUBDIR_ROOT="${ARTIFACT_PREFIX}-restore-sel-subdir"

rm -rf "$FULL_RESTORE_ROOT" "$SEL_SINGLE_ROOT" "$SEL_100_ROOT" "$SEL_SUBDIR_ROOT"
mkdir -p "$FULL_RESTORE_ROOT" "$SEL_SINGLE_ROOT" "$SEL_100_ROOT" "$SEL_SUBDIR_ROOT"

"$COLDKEEP_BIN" list --output json >"$LIST_JSON_PATH"

seed_fallback=0
if [[ -n "$RANDOM_SEED" ]]; then
	seed_fallback="$RANDOM_SEED"
elif [[ "$RUN_ID" =~ ^[0-9]+$ ]]; then
	seed_fallback="$RUN_ID"
fi

python3 - "$DATASET_PATH" "$LIST_JSON_PATH" "$SELECTION_JSON_PATH" "$SMALL_FILE_MAX_BYTES" "$seed_fallback" "$ALLOW_FEWER_THAN_100" "$SKIP_SELECTIVE" <<'PY'
import json
import os
import random
import sys

(dataset_path, list_json_path, out_path, small_max_raw, seed_raw, allow_fewer_raw, skip_selective_raw) = sys.argv[1:]
small_max = int(small_max_raw)
seed = int(seed_raw)
allow_fewer = int(allow_fewer_raw) == 1
skip_selective = int(skip_selective_raw) == 1

with open(list_json_path, 'r', encoding='utf-8') as f:
    doc = json.load(f)

files = doc.get('files') or []
stored_paths = sorted({str(item.get('name', '')).strip() for item in files if str(item.get('name', '')).strip()})
if not stored_paths:
    raise SystemExit('no stored paths found in repository; restore sequence requires a stored repo')

source_meta = {}
small_candidates = []
dir_counts = {}
for p in stored_paths:
    if not os.path.isfile(p):
        raise SystemExit(f'source file missing for stored path: {p}')
    sz = os.path.getsize(p)
    source_meta[p] = {'size_bytes': sz}
    parent = os.path.dirname(p)
    if parent:
        dir_counts[parent] = dir_counts.get(parent, 0) + 1
    if sz <= small_max:
        small_candidates.append(p)

if not small_candidates:
    if skip_selective:
        payload = {
            'dataset_path': dataset_path,
            'stored_paths': stored_paths,
            'source_meta': source_meta,
            'selective': None,
            'skip_selective': True,
        }
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write('\n')
        raise SystemExit(0)
    raise SystemExit(f'no small files found (<= {small_max} bytes) for selective restore')

single_small = min(small_candidates, key=lambda p: (source_meta[p]['size_bytes'], p))

rng = random.Random(seed)
if len(small_candidates) < 100 and not allow_fewer:
    raise SystemExit(f'need at least 100 small files for selective restore; found {len(small_candidates)} (pass --allow-fewer-than-100 to override)')

if len(small_candidates) >= 100:
    random_100 = sorted(rng.sample(small_candidates, 100))
else:
    random_100 = sorted(small_candidates)

subdir = ''
subdir_files = []
for parent, _ in sorted(dir_counts.items(), key=lambda kv: (-kv[1], kv[0])):
    members = sorted([p for p in stored_paths if p.startswith(parent + os.sep)])
    if members:
        subdir = parent
        subdir_files = members
        break

if not subdir_files:
    raise SystemExit('could not derive subdirectory selection for selective restore')

payload = {
    'dataset_path': dataset_path,
    'stored_paths': stored_paths,
    'source_meta': source_meta,
    'selective': {
        'single_small': [single_small],
        'random_small_100': random_100,
        'subdirectory': {
            'path': subdir,
            'files': subdir_files,
        },
    },
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write('\n')
PY

restore_paths_with_prefix() {
	local selection_json="$1"
	local selection_key="$2"
	local destination_root="$3"
	local io_jsonl_path="$4"

	local _rss_peak_file
	_rss_peak_file="$(mktemp)"
	echo "0" >"$_rss_peak_file"

	python3 - "$selection_json" "$selection_key" <<'PY' | while IFS= read -r stored_path; do
import json
import sys

selection_path = sys.argv[1]
selection_key = sys.argv[2]
with open(selection_path, 'r', encoding='utf-8') as f:
    payload = json.load(f)

if selection_key == 'full':
    items = payload.get('stored_paths') or []
elif selection_key == 'single_small':
    items = ((payload.get('selective') or {}).get('single_small') or [])
elif selection_key == 'random_small_100':
    items = ((payload.get('selective') or {}).get('random_small_100') or [])
elif selection_key == 'subdirectory':
    items = (((payload.get('selective') or {}).get('subdirectory') or {}).get('files') or [])
else:
    raise SystemExit(f'unknown selection key: {selection_key}')

for item in items:
    print(str(item))
PY
		export COLDKEEP_IO_COUNTERS_FILE="$io_jsonl_path"
		run_with_rss_kb "$COLDKEEP_BIN" restore --stored-path "$stored_path" --mode prefix --destination "$destination_root" --overwrite --output json >/dev/null
		_cur_peak="$(cat "$_rss_peak_file" 2>/dev/null || echo 0)"
		if [[ "${_last_rss_kb:-0}" -gt "${_cur_peak:-0}" ]]; then
			echo "$_last_rss_kb" >"$_rss_peak_file"
		fi
	done
	unset COLDKEEP_IO_COUNTERS_FILE

	_restore_case_peak_rss_kb="$(cat "$_rss_peak_file" 2>/dev/null || echo 0)"
	rm -f "$_rss_peak_file"
}

capture_stats_optional() {
	local out_path="$1"
	if "$COLDKEEP_BIN" stats --output json >"$out_path" 2>/dev/null; then
		return 0
	fi
	echo '{}' >"$out_path"
	return 0
}

FULL_STATS_BEFORE="${ARTIFACT_PREFIX}-full-stats-before.json"
FULL_STATS_AFTER="${ARTIFACT_PREFIX}-full-stats-after.json"
SEL1_STATS_BEFORE="${ARTIFACT_PREFIX}-sel-single-stats-before.json"
SEL1_STATS_AFTER="${ARTIFACT_PREFIX}-sel-single-stats-after.json"
SEL100_STATS_BEFORE="${ARTIFACT_PREFIX}-sel-100-stats-before.json"
SEL100_STATS_AFTER="${ARTIFACT_PREFIX}-sel-100-stats-after.json"
SELDIR_STATS_BEFORE="${ARTIFACT_PREFIX}-sel-subdir-stats-before.json"
SELDIR_STATS_AFTER="${ARTIFACT_PREFIX}-sel-subdir-stats-after.json"

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

# Full restore
capture_stats_optional "$FULL_STATS_BEFORE"
full_start_ms="$(now_ms)"
restore_paths_with_prefix "$SELECTION_JSON_PATH" "full" "$FULL_RESTORE_ROOT" "$FULL_IO_JSONL_PATH"
full_rss_kb="$_restore_case_peak_rss_kb"
full_end_ms="$(now_ms)"
full_elapsed_ms=$((full_end_ms - full_start_ms))
capture_stats_optional "$FULL_STATS_AFTER"

# Determine whether selective restores should run
_has_selective="$(python3 -c "import json,sys; d=json.load(open('$SELECTION_JSON_PATH')); print(0 if d.get('skip_selective') else 1)")"

sel1_elapsed_ms=0; sel1_rss_kb=0
sel100_elapsed_ms=0; sel100_rss_kb=0
seldir_elapsed_ms=0; seldir_rss_kb=0

if [[ "$_has_selective" == "1" ]]; then
# Selective: single small file
capture_stats_optional "$SEL1_STATS_BEFORE"
sel1_start_ms="$(now_ms)"
restore_paths_with_prefix "$SELECTION_JSON_PATH" "single_small" "$SEL_SINGLE_ROOT" "$SEL1_IO_JSONL_PATH"
sel1_rss_kb="$_restore_case_peak_rss_kb"
sel1_end_ms="$(now_ms)"
sel1_elapsed_ms=$((sel1_end_ms - sel1_start_ms))
capture_stats_optional "$SEL1_STATS_AFTER"

# Selective: 100 random small files
capture_stats_optional "$SEL100_STATS_BEFORE"
sel100_start_ms="$(now_ms)"
restore_paths_with_prefix "$SELECTION_JSON_PATH" "random_small_100" "$SEL_100_ROOT" "$SEL100_IO_JSONL_PATH"
sel100_rss_kb="$_restore_case_peak_rss_kb"
sel100_end_ms="$(now_ms)"
sel100_elapsed_ms=$((sel100_end_ms - sel100_start_ms))
capture_stats_optional "$SEL100_STATS_AFTER"

# Selective: one subdirectory
capture_stats_optional "$SELDIR_STATS_BEFORE"
seldir_start_ms="$(now_ms)"
restore_paths_with_prefix "$SELECTION_JSON_PATH" "subdirectory" "$SEL_SUBDIR_ROOT" "$SELDIR_IO_JSONL_PATH"
seldir_rss_kb="$_restore_case_peak_rss_kb"
seldir_end_ms="$(now_ms)"
seldir_elapsed_ms=$((seldir_end_ms - seldir_start_ms))
capture_stats_optional "$SELDIR_STATS_AFTER"
fi # _has_selective

python3 - "$SELECTION_JSON_PATH" "$RESULT_JSON_PATH" \
	"$FULL_RESTORE_ROOT" "$SEL_SINGLE_ROOT" "$SEL_100_ROOT" "$SEL_SUBDIR_ROOT" \
	"$FULL_IO_JSONL_PATH" "$SEL1_IO_JSONL_PATH" "$SEL100_IO_JSONL_PATH" "$SELDIR_IO_JSONL_PATH" \
	"$FULL_STATS_BEFORE" "$FULL_STATS_AFTER" "$SEL1_STATS_BEFORE" "$SEL1_STATS_AFTER" "$SEL100_STATS_BEFORE" "$SEL100_STATS_AFTER" "$SELDIR_STATS_BEFORE" "$SELDIR_STATS_AFTER" \
	"$BLOCK_MB" "$DATASET_LABEL" "$RUN_ID" "$RUN_DB_NAME" "$RUN_STORAGE_DIR" \
	"$full_elapsed_ms" "$sel1_elapsed_ms" "$sel100_elapsed_ms" "$seldir_elapsed_ms" \
	"$full_rss_kb" "$sel1_rss_kb" "$sel100_rss_kb" "$seldir_rss_kb" \
	"$seed_fallback" <<'PY'
import hashlib
import json
import os
import sys

(
    selection_path,
    out_path,
    full_root,
    sel1_root,
    sel100_root,
    seldir_root,
    full_io,
    sel1_io,
    sel100_io,
    seldir_io,
    full_stats_before,
    full_stats_after,
    sel1_stats_before,
    sel1_stats_after,
    sel100_stats_before,
    sel100_stats_after,
    seldir_stats_before,
    seldir_stats_after,
    block_mb,
    dataset_label,
    run_id,
    db_name,
    storage_dir,
    full_elapsed_ms,
    sel1_elapsed_ms,
    sel100_elapsed_ms,
    seldir_elapsed_ms,
    full_rss_kb,
    sel1_rss_kb,
    sel100_rss_kb,
    seldir_rss_kb,
    dataset_seed,
) = sys.argv[1:]

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

payload = load_json(selection_path)
source_meta = payload['source_meta']


def normalize_stored_to_restore(root, stored_path):
    rel = stored_path
    drive, tail = os.path.splitdrive(rel)
    if drive:
        rel = tail
    rel = rel.lstrip('/\\')
    return os.path.join(os.path.abspath(root), rel)


def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def tree_hash(file_map):
    h = hashlib.sha256()
    for key in sorted(file_map):
        h.update(key.encode('utf-8'))
        h.update(b'\0')
        h.update(file_map[key].encode('ascii'))
        h.update(b'\n')
    return h.hexdigest()


def parse_io(path):
    out = {
        'container_append_count': 0,
        'fsync_count': 0,
        'container_open_count': 0,
        'container_close_count': 0,
        'bytes_written': 0,
        'bytes_read': 0,
        'snapshot_metadata_write_count': 0,
        'block_decode_count': 0,
    }
    if not os.path.exists(path):
        return out
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if str(rec.get('command', '')).strip() != 'restore':
                continue
            out['container_append_count'] += int(rec.get('container_append_count', 0))
            out['fsync_count'] += int(rec.get('fsync_count', 0))
            out['container_open_count'] += int(rec.get('container_open_count', 0))
            out['container_close_count'] += int(rec.get('container_close_count', 0))
            out['bytes_written'] += int(rec.get('bytes_written', 0))
            out['bytes_read'] += int(rec.get('bytes_read', 0))
            out['snapshot_metadata_write_count'] += int(rec.get('snapshot_metadata_write_count', 0))
            out['block_decode_count'] += int(rec.get('block_decode_count', 0))
    return out


METRIC_KEYS = {
    'read_amplification',
    'bytes_read_from_containers',
    'restored_bytes',
    'block_reads',
    'block_cache_hits',
    'block_cache_misses',
    'cache_hit_ratio',
    'block_decode_count',
}


def collect_optional_metrics(obj, out):
    if isinstance(obj, dict):
        for k, v in obj.items():
            key_lower = str(k).lower()
            if key_lower in METRIC_KEYS:
                out[str(k)] = v
            collect_optional_metrics(v, out)
    elif isinstance(obj, list):
        for v in obj:
            collect_optional_metrics(v, out)


def run_case(case_name, selected_paths, restore_root, io_path, stats_before_path, stats_after_path, elapsed_ms, peak_rss_kb=None):
    source_hashes = {}
    restored_hashes = {}
    restored_bytes = 0
    hash_mismatches = []

    for stored_path in selected_paths:
        source_path = stored_path
        if not os.path.isfile(source_path):
            raise SystemExit(f'{case_name}: source path missing: {source_path}')
        restored_path = normalize_stored_to_restore(restore_root, stored_path)
        if not os.path.isfile(restored_path):
            raise SystemExit(f'{case_name}: restored file missing: {restored_path}')

        src_hash = hash_file(source_path)
        dst_hash = hash_file(restored_path)
        source_hashes[stored_path] = src_hash
        restored_hashes[stored_path] = dst_hash

        src_size = int(source_meta[stored_path]['size_bytes'])
        dst_size = os.path.getsize(restored_path)
        restored_bytes += dst_size
        if dst_size != src_size:
            hash_mismatches.append({
                'stored_path': stored_path,
                'reason': 'size_mismatch',
                'source_size': src_size,
                'restored_size': dst_size,
            })
        elif src_hash != dst_hash:
            hash_mismatches.append({
                'stored_path': stored_path,
                'reason': 'hash_mismatch',
                'source_hash': src_hash,
                'restored_hash': dst_hash,
            })

    source_bytes = sum(int(source_meta[p]['size_bytes']) for p in selected_paths)
    no_hash_mismatch = len(hash_mismatches) == 0
    restored_bytes_match_original = restored_bytes == source_bytes

    io_totals = parse_io(io_path)
    bytes_read = int(io_totals.get('bytes_read', 0))
    read_amplification = None
    if source_bytes > 0:
        read_amplification = bytes_read / float(source_bytes)

    optional_metrics = {}
    collect_optional_metrics(load_json(stats_before_path), optional_metrics)
    collect_optional_metrics(load_json(stats_after_path), optional_metrics)

    return {
        'name': case_name,
        'elapsed_ms': int(elapsed_ms),
        'files_count': len(selected_paths),
        'bytes_source': int(source_bytes),
        'bytes_restored': int(restored_bytes),
        'restored_bytes_match_original': restored_bytes_match_original,
        'tree_hash_source': tree_hash(source_hashes),
        'tree_hash_restored': tree_hash(restored_hashes),
        'no_hash_mismatch': no_hash_mismatch,
        'hash_mismatch_count': len(hash_mismatches),
        'hash_mismatches': hash_mismatches,
        'io_debug': io_totals,
        'read_amplification': read_amplification,
        'read_amplification_measured': read_amplification is not None,
        'optional_block_read_cache_metrics': optional_metrics,
        'peak_rss_kb': int(peak_rss_kb) if peak_rss_kb else None,
    }

full_paths = payload['stored_paths']
skip_selective = bool(payload.get('skip_selective'))

cases = [
    run_case('full_restore', full_paths, full_root, full_io, full_stats_before, full_stats_after, full_elapsed_ms, full_rss_kb),
]

if not skip_selective:
    single_paths = (payload['selective'] or {}).get('single_small') or []
    rand100_paths = (payload['selective'] or {}).get('random_small_100') or []
    subdir_info = (payload['selective'] or {}).get('subdirectory') or {}
    subdir_paths = subdir_info.get('files') or []
    cases += [
        run_case('selective_single_small_file', single_paths, sel1_root, sel1_io, sel1_stats_before, sel1_stats_after, sel1_elapsed_ms, sel1_rss_kb),
        run_case('selective_random_100_small_files', rand100_paths, sel100_root, sel100_io, sel100_stats_before, sel100_stats_after, sel100_elapsed_ms, sel100_rss_kb),
        run_case('selective_one_subdirectory', subdir_paths, seldir_root, seldir_io, seldir_stats_before, seldir_stats_after, seldir_elapsed_ms, seldir_rss_kb),
    ]
else:
    subdir_info = {}

for case in cases:
    if not case['restored_bytes_match_original']:
        raise SystemExit(f"{case['name']}: restored bytes do not match original")
    if not case['no_hash_mismatch']:
        raise SystemExit(f"{case['name']}: hash mismatch detected")
    if not case['read_amplification_measured']:
        raise SystemExit(f"{case['name']}: read amplification not measured")

result = {
    'status': 'ok',
    'phase': 'phase8_restore_sequence',
    'data': {
        'block_target_size_mb': int(block_mb),
        'dataset': dataset_label,
        'run_id': run_id,
        'dataset_seed': dataset_seed,
        'db_name': db_name,
        'storage_dir': storage_dir,
        'selective_subdirectory': subdir_info.get('path', ''),
        'cases': cases,
    },
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2, sort_keys=True)
    f.write('\n')
PY

echo "phase8 restore-sequence complete: ${RESULT_JSON_PATH}"
