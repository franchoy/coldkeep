#!/usr/bin/env bash
# Phase 8 benchmark matrix runner
# Usage: bash scripts/run_phase8_matrix.sh [--bin <path>]
set -euo pipefail

COLDKEEP_BIN="${COLDKEEP_BIN:-coldkeep}"
DATASETS_ROOT=/tmp/phase8_datasets

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin) COLDKEEP_BIN="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

log() { echo "[matrix] $(date '+%H:%M:%S') $*"; }

log "Starting Phase 8 matrix with bin=$COLDKEEP_BIN"

for block_mb in 1 2; do
  for run in 1 2 3; do
    log "=== STORE datasetA ${block_mb}mb run${run} ==="
    bash scripts/run_phase8_store_sequence.sh "$block_mb" "$DATASETS_ROOT/datasetA" "$run" \
      --bin "$COLDKEEP_BIN" --dataset-label datasetA 2>&1 | grep -E 'complete|ERROR|FAIL' || true

    log "=== RESTORE datasetA ${block_mb}mb run${run} ==="
    bash scripts/run_phase8_restore_sequence.sh "$block_mb" "$DATASETS_ROOT/datasetA" "$run" \
      --bin "$COLDKEEP_BIN" --dataset-label datasetA --skip-selective 2>&1 | grep -E 'complete|ERROR|FAIL' || true

    log "=== STORE datasetB ${block_mb}mb run${run} ==="
    bash scripts/run_phase8_store_sequence.sh "$block_mb" "$DATASETS_ROOT/datasetB" "$run" \
      --bin "$COLDKEEP_BIN" --dataset-label datasetB 2>&1 | grep -E 'complete|ERROR|FAIL' || true

    log "=== RESTORE datasetB ${block_mb}mb run${run} ==="
    bash scripts/run_phase8_restore_sequence.sh "$block_mb" "$DATASETS_ROOT/datasetB" "$run" \
      --bin "$COLDKEEP_BIN" --dataset-label datasetB --allow-fewer-than-100 2>&1 | grep -E 'complete|ERROR|FAIL' || true

    log "=== DEDUP datasetD ${block_mb}mb run${run} ==="
    bash scripts/run_phase8_dedup_sequence.sh "$block_mb" "$DATASETS_ROOT/datasetD" "$run" \
      --bin "$COLDKEEP_BIN" --dataset-label datasetD 2>&1 | grep -E 'complete|ERROR|FAIL' || true

    log "=== GC datasetF ${block_mb}mb run${run} ==="
    bash scripts/run_phase8_gc_sequence.sh "$block_mb" "$DATASETS_ROOT/datasetF" "$run" \
      --bin "$COLDKEEP_BIN" --dataset-label datasetF 2>&1 | grep -E 'complete|ERROR|FAIL' || true
  done
done

log "Matrix COMPLETE"
log "Results in: tmp/bench_phase8_*/"
find tmp/bench_phase8_/ -mindepth 1 -maxdepth 1 2>/dev/null | wc -l
