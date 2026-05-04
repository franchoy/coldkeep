#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT_DIR/tmp/bench_phase8"
BIN_PATH="$OUT_DIR/coldkeep-phase8"

DATASETS=(small medium)
WORKERS=(1 4)
BLOCK_SIZES=(1 2 3)
REPEATS=3
LIST_ONLY=0
RUN_ONLY_MISSING=1

DEFAULT_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

usage() {
	cat <<'EOF'
Usage: scripts/run_phase8_blocksize_matrix.sh [options]

Resumable Phase 8 benchmark matrix runner for packed block size experiments.
Builds the coldkeep binary once, writes each benchmark result to a per-run JSON
file, and skips only outputs that already contain a valid "status":"ok" JSON.

Options:
  --dataset LIST       Comma-separated datasets (default: small,medium)
  --workers LIST       Comma-separated worker counts (default: 1,4)
  --block-sizes LIST   Comma-separated block sizes in MiB (default: 1,2,3)
  --repeats N          Number of repeats per combination (default: 3)
  --output-dir PATH    Output directory (default: tmp/bench_phase8)
  --binary PATH        Binary output path (default: tmp/bench_phase8/coldkeep-phase8)
  --list-missing       Print planned runs and whether they are done/missing, then exit
  --rerun-all          Ignore existing outputs and rerun everything
  --help               Show this help text

Required DB env vars should already be set in the shell or Codespace:
	DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE

Optional base DB name source:
	DB_NAME_BASE (preferred) or DB_NAME (fallback)

The runner enforces per-run DB isolation using generated names:
	${DB_NAME_BASE}_${blockSize}m_${dataset}_w${workers}_r${repeat}

The runner forces deterministic/plain benchmark settings because the current
Phase 8 block-size comparison is about packed layout size, not codec variance.
EOF
}

split_csv() {
	local raw="$1"
	local -n out_ref=$2
	IFS=',' read -r -a out_ref <<<"$raw"
}

is_complete_json() {
	local path="$1"
	[[ -s "$path" ]] || return 1
	grep -q '"status":"ok"' "$path"
}

require_db_env() {
	local missing=0
	for var_name in DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE; do
		if [[ -z "${!var_name:-}" ]]; then
			echo "missing required env: $var_name" >&2
			missing=1
		fi
	done
	if [[ -z "${DB_NAME_BASE:-}" && -z "${DB_NAME:-}" ]]; then
		echo "missing required env: DB_NAME_BASE (or DB_NAME as fallback)" >&2
		missing=1
	fi
	if [[ "$missing" -ne 0 ]]; then
		exit 1
	fi
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dataset)
			split_csv "$2" DATASETS
			shift 2
			;;
		--workers)
			split_csv "$2" WORKERS
			shift 2
			;;
		--block-sizes)
			split_csv "$2" BLOCK_SIZES
			shift 2
			;;
		--repeats)
			REPEATS="$2"
			shift 2
			;;
		--output-dir)
			OUT_DIR="$2"
			shift 2
			;;
		--binary)
			BIN_PATH="$2"
			shift 2
			;;
		--list-missing)
			LIST_ONLY=1
			shift
			;;
		--rerun-all)
			RUN_ONLY_MISSING=0
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

if ! [[ "$REPEATS" =~ ^[0-9]+$ ]] || [[ "$REPEATS" -le 0 ]]; then
	echo "--repeats must be a positive integer" >&2
	exit 1
fi

require_db_env
mkdir -p "$OUT_DIR"

needs_run=0
for dataset in "${DATASETS[@]}"; do
	for workers in "${WORKERS[@]}"; do
		for size_mib in "${BLOCK_SIZES[@]}"; do
			for repeat_idx in $(seq 1 "$REPEATS"); do
				out_file="$OUT_DIR/phase8-${dataset}-w${workers}-r${repeat_idx}-${size_mib}m.json"
				status="missing"
				if is_complete_json "$out_file"; then
					status="done"
				elif [[ -e "$out_file" ]]; then
					status="incomplete"
				fi
				printf '%s\t%s\n' "$status" "$out_file"
				if [[ "$status" != "done" ]]; then
					needs_run=1
				fi
			done
		done
	done
done | sort

if [[ "$LIST_ONLY" -eq 1 ]]; then
	exit 0
fi

mkdir -p "$(dirname "$BIN_PATH")"
echo "building benchmark binary at $BIN_PATH"
(cd "$ROOT_DIR" && go build -o "$BIN_PATH" ./cmd/coldkeep)

export COLDKEEP_DB_AUTO_BOOTSTRAP="true"
export COLDKEEP_CODEC="plain"
export COLDKEEP_KEY="${COLDKEEP_KEY:-$DEFAULT_KEY}"
export COLDKEEP_DB_OPERATION_TIMEOUT_MS="${COLDKEEP_DB_OPERATION_TIMEOUT_MS:-1800000}"
export COLDKEEP_TEST_DB="1"

DB_NAME_BASE_EFFECTIVE="${DB_NAME_BASE:-${DB_NAME}}"

for dataset in "${DATASETS[@]}"; do
	for workers in "${WORKERS[@]}"; do
		for size_mib in "${BLOCK_SIZES[@]}"; do
			for repeat_idx in $(seq 1 "$REPEATS"); do
				out_file="$OUT_DIR/phase8-${dataset}-w${workers}-r${repeat_idx}-${size_mib}m.json"
				if [[ "$RUN_ONLY_MISSING" -eq 1 ]] && is_complete_json "$out_file"; then
					echo "skip completed $out_file"
					continue
				fi

				tmp_file="$out_file.tmp"
				rm -f "$tmp_file"
				echo "run dataset=$dataset workers=$workers block_size_mib=$size_mib repeat=$repeat_idx"
				run_id="w${workers}_r${repeat_idx}"
				run_storage_dir="/tmp/coldkeep-bench-${size_mib}m-${dataset}-${run_id}"
				run_db_name="${DB_NAME_BASE_EFFECTIVE}_${size_mib}m_${dataset}_${run_id}"

				# Do not reuse storage directories between runs.
				rm -rf "$run_storage_dir"
				(
					cd "$ROOT_DIR"
					COLDKEEP_BLOCK_TARGET_SIZE_MB="$size_mib" \
					COLDKEEP_STORAGE_DIR="$run_storage_dir" \
					DB_NAME="$run_db_name" \
					"$BIN_PATH" benchmark run --dataset "$dataset" --workers "$workers" --output json
				) >"$tmp_file"
				if ! is_complete_json "$tmp_file"; then
					echo "benchmark output missing status=ok for $out_file" >&2
					rm -f "$tmp_file"
					exit 1
				fi
				mv "$tmp_file" "$out_file"
			done
		done
	done
done

echo "phase 8 block-size benchmark matrix complete"