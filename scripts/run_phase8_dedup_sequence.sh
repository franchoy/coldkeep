#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/run_phase8_dedup_sequence.sh <BLOCK_MB> <DATASET_D_ROOT> <RUN_ID> [options]

Dataset D sequence (dedup-heavy):
  1) store folder_v1
  2) record blocks/chunks
  3) store folder_v2 (mostly duplicate)
  4) record incremental new chunks/blocks
  5) restore both
  6) verify

Expected checks:
  - second store creates far fewer chunks/blocks
  - existing chunks are not repacked

Cross-block comparison (1 MiB vs 2 MiB) is performed by:
  scripts/compare_phase8_dedup_results.py <1m-result.json> <2m-result.json>

Arguments:
  BLOCK_MB         block target size in MiB (e.g. 1 or 2)
  DATASET_D_ROOT   root containing folder_v1/ and folder_v2/
  RUN_ID           run identifier

Options:
  --dataset-label <label>     label used in artifact names (default: basename(DATASET_D_ROOT))
  --output-dir <path>         output directory (default: tmp/bench_phase8_dedup_sequence)
  --bin <path>                coldkeep binary path (default: coldkeep)
  --max-incremental-ratio <r> max allowed incremental ratio for chunks/blocks (default: 0.50)

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
DATASET_D_ROOT="$2"
RUN_ID="$3"
shift 3

DATASET_LABEL="$(basename "$DATASET_D_ROOT")"
OUT_DIR="tmp/bench_phase8_dedup_sequence"
COLDKEEP_BIN="coldkeep"
MAX_INCREMENTAL_RATIO="0.50"

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dataset-label)
			DATASET_LABEL="$2"
			shift 2
			;;
		--output-dir)
			OUT_DIR="$2"
			shift 2
			;;
		--bin)
			COLDKEEP_BIN="$2"
			shift 2
			;;
		--max-incremental-ratio)
			MAX_INCREMENTAL_RATIO="$2"
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
	echo "psql is required" >&2
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
if [[ ! -d "$DATASET_D_ROOT" ]]; then
	echo "dataset root not found: $DATASET_D_ROOT" >&2
	exit 1
fi
FOLDER_V1="$DATASET_D_ROOT/folder_v1"
FOLDER_V2="$DATASET_D_ROOT/folder_v2"
if [[ ! -d "$FOLDER_V1" || ! -d "$FOLDER_V2" ]]; then
	echo "dataset root must contain folder_v1/ and folder_v2/: $DATASET_D_ROOT" >&2
	exit 1
fi

DB_NAME_BASE_EFFECTIVE="${DB_NAME_BASE:-coldkeep_bench}"
RUN_DB_NAME="${DB_NAME_BASE_EFFECTIVE}_${BLOCK_MB}m_${DATASET_LABEL}_${RUN_ID}"
RUN_STORAGE_DIR="/tmp/coldkeep-bench-${BLOCK_MB}m-${DATASET_LABEL}-${RUN_ID}"

mkdir -p "$OUT_DIR"
ARTIFACT_PREFIX="$OUT_DIR/${DATASET_LABEL}-${BLOCK_MB}m-${RUN_ID}"
RESULT_JSON_PATH="${ARTIFACT_PREFIX}-dedup-result.json"
SELECTION_JSON_PATH="${ARTIFACT_PREFIX}-selection.json"
LIST_AFTER_V2_JSON_PATH="${ARTIFACT_PREFIX}-list-after-v2.json"
MAP_BEFORE_TSV_PATH="${ARTIFACT_PREFIX}-chunk-map-before-v2.tsv"
MAP_AFTER_TSV_PATH="${ARTIFACT_PREFIX}-chunk-map-after-v2.tsv"
RESTORE_V1_ROOT="${ARTIFACT_PREFIX}-restore-v1"
RESTORE_V2_ROOT="${ARTIFACT_PREFIX}-restore-v2"

export COLDKEEP_BLOCK_TARGET_SIZE_MB="$BLOCK_MB"
export COLDKEEP_STORAGE_DIR="$RUN_STORAGE_DIR"
export COLDKEEP_TEST_DB="1"
export DB_NAME="$RUN_DB_NAME"
export COLDKEEP_DB_AUTO_BOOTSTRAP="true"
export COLDKEEP_CODEC="plain"
export COLDKEEP_KEY="${COLDKEEP_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"
export COLDKEEP_DB_OPERATION_TIMEOUT_MS="${COLDKEEP_DB_OPERATION_TIMEOUT_MS:-1800000}"

PSQL_CONN="host=${DB_HOST} port=${DB_PORT} user=${DB_USER} password=${DB_PASSWORD} dbname=${DB_NAME} sslmode=${DB_SSLMODE}"

sql_scalar() {
	local query="$1"
	psql "$PSQL_CONN" -t -A -c "$query" | tr -d '[:space:]'
}

now_ms() {
	python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

# Fresh isolated run context.
rm -rf "$RUN_STORAGE_DIR" "$RESTORE_V1_ROOT" "$RESTORE_V2_ROOT"
PGPASSWORD="$DB_PASSWORD" dropdb --if-exists -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$RUN_DB_NAME" 2>/dev/null || true
PGPASSWORD="$DB_PASSWORD" createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$RUN_DB_NAME"
mkdir -p "$RESTORE_V1_ROOT" "$RESTORE_V2_ROOT"

# 1) store folder_v1
store_v1_start_ms="$(now_ms)"
"$COLDKEEP_BIN" store-folder "$FOLDER_V1" >/dev/null
store_v1_end_ms="$(now_ms)"
store_v1_elapsed_ms=$((store_v1_end_ms - store_v1_start_ms))

# 2) baseline blocks/chunks
chunks_after_v1="$(sql_scalar "SELECT COUNT(*) FROM chunk WHERE status='COMPLETED';")"
blocks_after_v1="$(sql_scalar "SELECT COUNT(*) FROM storage_blocks;")"

# Snapshot chunk->block mapping for existing chunks after v1.
psql "$PSQL_CONN" -t -A -F $'\t' -c "SELECT cbr.chunk_id, cbr.block_id, cbr.offset_in_block, cbr.size_in_block FROM chunk_block_refs cbr JOIN chunk c ON c.id=cbr.chunk_id WHERE c.status='COMPLETED' ORDER BY cbr.chunk_id" >"$MAP_BEFORE_TSV_PATH"

# 3) store folder_v2
store_v2_start_ms="$(now_ms)"
"$COLDKEEP_BIN" store-folder "$FOLDER_V2" >/dev/null
store_v2_end_ms="$(now_ms)"
store_v2_elapsed_ms=$((store_v2_end_ms - store_v2_start_ms))

# 4) incremental new chunks/blocks
chunks_after_v2="$(sql_scalar "SELECT COUNT(*) FROM chunk WHERE status='COMPLETED';")"
blocks_after_v2="$(sql_scalar "SELECT COUNT(*) FROM storage_blocks;")"
new_chunks_v2=$((chunks_after_v2 - chunks_after_v1))
new_blocks_v2=$((blocks_after_v2 - blocks_after_v1))

# Snapshot chunk->block mapping after v2 for repack detection.
psql "$PSQL_CONN" -t -A -F $'\t' -c "SELECT cbr.chunk_id, cbr.block_id, cbr.offset_in_block, cbr.size_in_block FROM chunk_block_refs cbr JOIN chunk c ON c.id=cbr.chunk_id WHERE c.status='COMPLETED' ORDER BY cbr.chunk_id" >"$MAP_AFTER_TSV_PATH"

"$COLDKEEP_BIN" list --output json >"$LIST_AFTER_V2_JSON_PATH"

python3 - "$FOLDER_V1" "$FOLDER_V2" "$LIST_AFTER_V2_JSON_PATH" "$SELECTION_JSON_PATH" <<'PY'
import hashlib
import json
import os
import sys

folder_v1, folder_v2, list_path, out_path = sys.argv[1:]

with open(list_path, 'r', encoding='utf-8') as f:
    payload = json.load(f)

files = payload.get('files') or []
stored_paths = sorted({str(x.get('name', '')).strip() for x in files if str(x.get('name', '')).strip()})

v1_prefix = os.path.abspath(folder_v1)
v2_prefix = os.path.abspath(folder_v2)

v1_paths = [p for p in stored_paths if p == v1_prefix or p.startswith(v1_prefix + os.sep)]
v2_paths = [p for p in stored_paths if p == v2_prefix or p.startswith(v2_prefix + os.sep)]

if not v1_paths:
    raise SystemExit('no stored paths found for folder_v1 in repository list output')
if not v2_paths:
    raise SystemExit('no stored paths found for folder_v2 in repository list output')

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

source_meta = {}
for p in sorted(set(v1_paths + v2_paths)):
    if not os.path.isfile(p):
        raise SystemExit(f'source path missing: {p}')
    source_meta[p] = {
        'size_bytes': os.path.getsize(p),
        'sha256': sha256_file(p),
    }

out = {
    'folder_v1_abs': v1_prefix,
    'folder_v2_abs': v2_prefix,
    'folder_v1_paths': v1_paths,
    'folder_v2_paths': v2_paths,
    'source_meta': source_meta,
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(out, f, indent=2, sort_keys=True)
    f.write('\n')
PY

restore_paths_prefix() {
	local selection_json="$1"
	local key="$2"
	local dest_root="$3"
	python3 - "$selection_json" "$key" <<'PY' | while IFS= read -r stored_path; do
import json
import sys
selection, key = sys.argv[1:]
with open(selection, 'r', encoding='utf-8') as f:
    doc = json.load(f)
items = doc.get(key) or []
for p in items:
    print(p)
PY
		"$COLDKEEP_BIN" restore --stored-path "$stored_path" --mode prefix --destination "$dest_root" --overwrite --output json >/dev/null
	done
}

# 5) restore both folder_v1 and folder_v2
restore_v1_start_ms="$(now_ms)"
restore_paths_prefix "$SELECTION_JSON_PATH" "folder_v1_paths" "$RESTORE_V1_ROOT"
restore_v1_end_ms="$(now_ms)"
restore_v1_elapsed_ms=$((restore_v1_end_ms - restore_v1_start_ms))

restore_v2_start_ms="$(now_ms)"
restore_paths_prefix "$SELECTION_JSON_PATH" "folder_v2_paths" "$RESTORE_V2_ROOT"
restore_v2_end_ms="$(now_ms)"
restore_v2_elapsed_ms=$((restore_v2_end_ms - restore_v2_start_ms))

# 6) verify
verify_start_ms="$(now_ms)"
"$COLDKEEP_BIN" verify system --standard >/dev/null
verify_end_ms="$(now_ms)"
verify_elapsed_ms=$((verify_end_ms - verify_start_ms))

python3 - "$RESULT_JSON_PATH" "$SELECTION_JSON_PATH" "$MAP_BEFORE_TSV_PATH" "$MAP_AFTER_TSV_PATH" \
	"$BLOCK_MB" "$DATASET_LABEL" "$RUN_ID" "$RUN_DB_NAME" "$RUN_STORAGE_DIR" \
	"$store_v1_elapsed_ms" "$store_v2_elapsed_ms" "$restore_v1_elapsed_ms" "$restore_v2_elapsed_ms" "$verify_elapsed_ms" \
	"$chunks_after_v1" "$chunks_after_v2" "$blocks_after_v1" "$blocks_after_v2" "$new_chunks_v2" "$new_blocks_v2" "$MAX_INCREMENTAL_RATIO" \
	"$RESTORE_V1_ROOT" "$RESTORE_V2_ROOT" "$RUN_ID" <<'PY'
import hashlib
import json
import math
import os
import sys

(
    out_path,
    selection_path,
    map_before_path,
    map_after_path,
    block_mb,
    dataset_label,
    run_id,
    db_name,
    storage_dir,
    store_v1_elapsed_ms,
    store_v2_elapsed_ms,
    restore_v1_elapsed_ms,
    restore_v2_elapsed_ms,
    verify_elapsed_ms,
    chunks_after_v1,
    chunks_after_v2,
    blocks_after_v1,
    blocks_after_v2,
    new_chunks_v2,
    new_blocks_v2,
    max_incremental_ratio,
    restore_v1_root,
    restore_v2_root,
    dataset_seed,
) = sys.argv[1:]

with open(selection_path, 'r', encoding='utf-8') as f:
    selection = json.load(f)

source_meta = selection['source_meta']
v1_paths = selection['folder_v1_paths']
v2_paths = selection['folder_v2_paths']


def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def tree_hash(file_map):
    h = hashlib.sha256()
    for k in sorted(file_map):
        h.update(k.encode('utf-8'))
        h.update(b'\0')
        h.update(file_map[k].encode('ascii'))
        h.update(b'\n')
    return h.hexdigest()


def restored_path(root, stored_path):
    rel = stored_path
    drive, tail = os.path.splitdrive(rel)
    if drive:
        rel = tail
    rel = rel.lstrip('/\\')
    return os.path.join(os.path.abspath(root), rel)


def validate_restore_case(case_name, paths, root):
    src_hashes = {}
    dst_hashes = {}
    mismatches = []
    src_bytes = 0
    dst_bytes = 0
    for p in paths:
        meta = source_meta[p]
        src_hash = str(meta['sha256'])
        src_size = int(meta['size_bytes'])
        out = restored_path(root, p)
        if not os.path.isfile(out):
            mismatches.append({'stored_path': p, 'reason': 'missing_restored_file'})
            continue
        dst_hash = hash_file(out)
        dst_size = os.path.getsize(out)
        src_hashes[p] = src_hash
        dst_hashes[p] = dst_hash
        src_bytes += src_size
        dst_bytes += dst_size
        if dst_size != src_size:
            mismatches.append({'stored_path': p, 'reason': 'size_mismatch', 'source_size': src_size, 'restored_size': dst_size})
        elif dst_hash != src_hash:
            mismatches.append({'stored_path': p, 'reason': 'hash_mismatch', 'source_hash': src_hash, 'restored_hash': dst_hash})

    return {
        'name': case_name,
        'files_count': len(paths),
        'source_bytes': src_bytes,
        'restored_bytes': dst_bytes,
        'restored_bytes_match_original': src_bytes == dst_bytes,
        'no_hash_mismatch': len(mismatches) == 0,
        'hash_mismatch_count': len(mismatches),
        'hash_mismatches': mismatches,
        'source_tree_hash': tree_hash(src_hashes),
        'restored_tree_hash': tree_hash(dst_hashes),
    }


case_v1 = validate_restore_case('restore_folder_v1', v1_paths, restore_v1_root)
case_v2 = validate_restore_case('restore_folder_v2', v2_paths, restore_v2_root)

if not case_v1['restored_bytes_match_original'] or not case_v1['no_hash_mismatch']:
    raise SystemExit('restore_folder_v1 validation failed')
if not case_v2['restored_bytes_match_original'] or not case_v2['no_hash_mismatch']:
    raise SystemExit('restore_folder_v2 validation failed')


def parse_map(path):
    out = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) != 4:
                raise SystemExit(f'invalid chunk map row: {line}')
            chunk_id = int(parts[0])
            out[chunk_id] = (int(parts[1]), int(parts[2]), int(parts[3]))
    return out

before = parse_map(map_before_path)
after = parse_map(map_after_path)

repack_changed = 0
repack_missing = 0
for chunk_id, loc_before in before.items():
    loc_after = after.get(chunk_id)
    if loc_after is None:
        repack_missing += 1
        continue
    if loc_after != loc_before:
        repack_changed += 1

if repack_changed != 0 or repack_missing != 0:
    raise SystemExit(f'existing chunks are repacked/missing: changed={repack_changed} missing={repack_missing}')

chunks_after_v1_i = int(chunks_after_v1)
chunks_after_v2_i = int(chunks_after_v2)
blocks_after_v1_i = int(blocks_after_v1)
blocks_after_v2_i = int(blocks_after_v2)
new_chunks_v2_i = int(new_chunks_v2)
new_blocks_v2_i = int(new_blocks_v2)
max_inc_ratio = float(max_incremental_ratio)

chunk_incremental_ratio = (new_chunks_v2_i / chunks_after_v1_i) if chunks_after_v1_i > 0 else 0.0
block_incremental_ratio = (new_blocks_v2_i / blocks_after_v1_i) if blocks_after_v1_i > 0 else 0.0

if chunk_incremental_ratio > max_inc_ratio:
    raise SystemExit(f'second store created too many new chunks: ratio={chunk_incremental_ratio:.4f} > {max_inc_ratio:.4f}')
if block_incremental_ratio > max_inc_ratio:
    raise SystemExit(f'second store created too many new blocks: ratio={block_incremental_ratio:.4f} > {max_inc_ratio:.4f}')

result = {
    'status': 'ok',
    'phase': 'phase8_dedup_sequence',
    'data': {
        'block_target_size_mb': int(block_mb),
        'dataset': dataset_label,
        'run_id': run_id,
        'dataset_seed': dataset_seed,
        'db_name': db_name,
        'storage_dir': storage_dir,
        'timings_ms': {
            'store_folder_v1': int(store_v1_elapsed_ms),
            'store_folder_v2': int(store_v2_elapsed_ms),
            'restore_folder_v1': int(restore_v1_elapsed_ms),
            'restore_folder_v2': int(restore_v2_elapsed_ms),
            'verify': int(verify_elapsed_ms),
        },
        'counts': {
            'chunks_after_v1': chunks_after_v1_i,
            'chunks_after_v2': chunks_after_v2_i,
            'blocks_after_v1': blocks_after_v1_i,
            'blocks_after_v2': blocks_after_v2_i,
            'new_chunks_v2': new_chunks_v2_i,
            'new_blocks_v2': new_blocks_v2_i,
        },
        'dedup': {
            'chunk_incremental_ratio': chunk_incremental_ratio,
            'block_incremental_ratio': block_incremental_ratio,
            'max_incremental_ratio': max_inc_ratio,
            'second_store_far_fewer_chunks_blocks': chunk_incremental_ratio <= max_inc_ratio and block_incremental_ratio <= max_inc_ratio,
        },
        'repack': {
            'existing_chunk_mapping_changed': repack_changed,
            'existing_chunk_mapping_missing': repack_missing,
            'existing_chunks_not_repacked': True,
        },
        'restore_validation': [case_v1, case_v2],
    },
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2, sort_keys=True)
    f.write('\n')
PY

echo "phase8 dedup-sequence complete: ${RESULT_JSON_PATH}"
