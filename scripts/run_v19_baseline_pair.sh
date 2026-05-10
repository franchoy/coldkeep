#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_PATH="${ROOT_DIR}/coldkeep"
OUT_DIR="${ROOT_DIR}/benchmarks/v1.9/baselines"
DATASET="small"
WORKERS="1"
REPEAT="1"
THRESHOLD="20"
MANIFEST_PATH=""

usage() {
  cat <<'USAGE'
Usage: scripts/run_v19_baseline_pair.sh [options]

Capture and validate the official v1.9 baseline pair:
  A: packed + aes-gcm + none
  B: packed + aes-gcm + zstd

Options:
  --bin PATH         coldkeep binary path (default: ./coldkeep)
  --out-dir PATH     output directory (default: benchmarks/v1.9/baselines)
  --dataset NAME     benchmark dataset preset (default: small)
  --workers N        benchmark worker count (default: 1)
  --repeat N         benchmark repeat count (default: 1)
  --threshold PCT    compare threshold percentage (default: 20)
  --manifest PATH    output manifest path (default: worker-specific file in out-dir)
  -h, --help         show this help text

Environment prerequisites:
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_SSLMODE
  COLDKEEP_KEY (required for aes-gcm mode)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin)
      BIN_PATH="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --dataset)
      DATASET="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --repeat)
      REPEAT="$2"
      shift 2
      ;;
    --threshold)
      THRESHOLD="$2"
      shift 2
      ;;
    --manifest)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -x "$BIN_PATH" ]]; then
  echo "binary is not executable: $BIN_PATH" >&2
  exit 1
fi

if [[ -z "${COLDKEEP_KEY:-}" ]]; then
  echo "COLDKEEP_KEY must be set for aes-gcm benchmark baselines" >&2
  exit 1
fi

for required_var in DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME DB_SSLMODE; do
  if [[ -z "${!required_var:-}" ]]; then
    echo "${required_var} must be set for benchmark runs" >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR"

BASE_A="$OUT_DIR/benchmark-baseline-v1.9-packed-aes-gcm-none-${DATASET}-w${WORKERS}-r${REPEAT}.json"
BASE_B="$OUT_DIR/benchmark-baseline-v1.9-packed-aes-gcm-zstd-${DATASET}-w${WORKERS}-r${REPEAT}.json"
if [[ -z "$MANIFEST_PATH" ]]; then
  MANIFEST="$OUT_DIR/baseline-manifest-v1.9-${DATASET}-w${WORKERS}-r${REPEAT}.json"
else
  MANIFEST="$MANIFEST_PATH"
fi

echo "capturing Baseline A (packed + aes-gcm + none)"
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=none \
  "$BIN_PATH" benchmark run --dataset "$DATASET" --workers "$WORKERS" --repeat "$REPEAT" --output json \
  > "$BASE_A"

echo "capturing Baseline B (packed + aes-gcm + zstd)"
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=zstd \
  "$BIN_PATH" benchmark run --dataset "$DATASET" --workers "$WORKERS" --repeat "$REPEAT" --output json \
  > "$BASE_B"

python3 - "$BASE_A" "$BASE_B" "$MANIFEST" <<'PY'
import datetime
import hashlib
import json
import pathlib
import sys

base_a = pathlib.Path(sys.argv[1])
base_b = pathlib.Path(sys.argv[2])
manifest_path = pathlib.Path(sys.argv[3])
repo_root = manifest_path.parents[3]

def load_envelope(path: pathlib.Path):
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict) and 'data' in obj:
            return obj
    raise RuntimeError(f'no benchmark envelope in {path}')

def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()

env_a = load_envelope(base_a)
env_b = load_envelope(base_b)

# Normalize baseline files to a single canonical JSON envelope so
# benchmark --compare can parse them deterministically.
base_a.write_text(json.dumps(env_a, separators=(',', ':')) + '\n')
base_b.write_text(json.dumps(env_b, separators=(',', ':')) + '\n')

data_a = env_a['data']
data_b = env_b['data']
rows_a = {row['case']: row for row in data_a['rows']}
rows_b = {row['case']: row for row in data_b['rows']}

common_cases = sorted(set(rows_a).intersection(rows_b))
comparison = []
for case in common_cases:
    ra = rows_a[case]
    rb = rows_b[case]
    a_d = float(ra['duration_ms'])
    b_d = float(rb['duration_ms'])
    a_t = float(ra['throughput_mbps'])
    b_t = float(rb['throughput_mbps'])
    comparison.append({
        'case': case,
        'duration_ms_none': ra['duration_ms'],
        'duration_ms_zstd': rb['duration_ms'],
        'duration_delta_pct_zstd_vs_none': ((b_d - a_d) / a_d * 100.0) if a_d else 0.0,
        'throughput_mbps_none': a_t,
        'throughput_mbps_zstd': b_t,
        'throughput_delta_pct_zstd_vs_none': ((b_t - a_t) / a_t * 100.0) if a_t else 0.0,
    })

manifest = {
    'version': 'v1.9',
  'status': 'frozen',
    'generated_at_utc': datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
  'reference_for_releases': ['v1.10', 'v1.11', 'v1.12'],
    'baseline_modes': {
        'baseline_a_uncompressed': {
            'label': 'packed + aes-gcm + none',
      'file': str(base_a.relative_to(repo_root)),
            'sha256': sha256(base_a),
            'dataset': data_a.get('dataset'),
            'repeat': data_a.get('repeat'),
            'execution': data_a.get('execution'),
            'total_files': data_a.get('execution_stats', {}).get('total_files'),
            'total_bytes': data_a.get('execution_stats', {}).get('total_bytes'),
        },
        'baseline_b_compressed': {
            'label': 'packed + aes-gcm + zstd',
          'file': str(base_b.relative_to(repo_root)),
            'sha256': sha256(base_b),
            'dataset': data_b.get('dataset'),
            'repeat': data_b.get('repeat'),
            'execution': data_b.get('execution'),
            'total_files': data_b.get('execution_stats', {}).get('total_files'),
            'total_bytes': data_b.get('execution_stats', {}).get('total_bytes'),
        },
    },
    'comparability_validation': {
        'same_dataset': data_a.get('dataset') == data_b.get('dataset'),
        'same_repeat': data_a.get('repeat') == data_b.get('repeat'),
        'same_execution_profile': data_a.get('execution') == data_b.get('execution'),
        'same_logical_totals': {
            'total_files_equal': data_a.get('execution_stats', {}).get('total_files') == data_b.get('execution_stats', {}).get('total_files'),
            'total_bytes_equal': data_a.get('execution_stats', {}).get('total_bytes') == data_b.get('execution_stats', {}).get('total_bytes'),
        },
        'same_case_set': sorted(rows_a) == sorted(rows_b),
    },
    'case_comparison': comparison,
}

manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
PY

echo "running regression detection checks against the captured baselines"
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=none \
  "$BIN_PATH" benchmark run --dataset "$DATASET" --workers "$WORKERS" --repeat "$REPEAT" \
  --compare "$BASE_A" --threshold "$THRESHOLD" >/dev/null

COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=zstd \
  "$BIN_PATH" benchmark run --dataset "$DATASET" --workers "$WORKERS" --repeat "$REPEAT" \
  --compare "$BASE_B" --threshold "$THRESHOLD" >/dev/null

echo "baseline artifacts written:"
echo "  $BASE_A"
echo "  $BASE_B"
echo "  $MANIFEST"
echo "regression compare checks passed at threshold ${THRESHOLD}%"
