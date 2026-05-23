#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/critical_coverage.sh --report [--csv-output PATH]

Description:
  Generate critical-path Go coverage visibility for v1.10.7 tracked packages.

Modes:
  --report
      Report coverage for packages listed in docs/release/v1.10/v1.10.7-critical-package-inventory.csv.

Options:
  --csv-output PATH
      Write a CSV report with package, tier, domain, gate mode, release-blocking flag, and coverage percentage.

Phase 2 behavior:
  This script is report-only. It does not enforce coverage thresholds.
  It exits non-zero only for script/runtime failures, malformed inventory, or coverage tool failures.

Examples:
  scripts/critical_coverage.sh --report
  scripts/critical_coverage.sh --report --csv-output docs/release/v1.10/v1.10.7-coverage-report.csv
EOF
}

MODE=""
CSV_OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --report)
      MODE="report"
      shift
      ;;
    --csv-output)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --csv-output requires a path" >&2
        exit 2
      fi
      CSV_OUTPUT="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$MODE" != "report" ]]; then
  echo "error: Phase 2 supports only --report mode" >&2
  usage >&2
  exit 2
fi

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT_DIR"

INVENTORY="docs/release/v1.10/v1.10.7-critical-package-inventory.csv"

if [[ ! -f "$INVENTORY" ]]; then
  echo "error: critical package inventory missing: $INVENTORY" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

PKG_LIST="$TMP_DIR/packages.txt"
COVER_PROFILE="$TMP_DIR/critical.coverprofile"
COVER_FUNC="$TMP_DIR/critical.func.txt"
REPORT_ROWS="$TMP_DIR/report_rows.csv"

python3 - "$INVENTORY" "$PKG_LIST" <<'PY'
from pathlib import Path
import csv
import sys

inventory = Path(sys.argv[1])
out = Path(sys.argv[2])

required_columns = {
    "package",
    "tier",
    "domain",
    "primary_invariant",
    "coverage_role",
    "gate_mode",
    "release_blocking_in_v1107",
}

rows = list(csv.DictReader(inventory.open(newline="", encoding="utf-8")))
if not rows:
    raise SystemExit("critical package inventory is empty")

missing_columns = required_columns - set(rows[0].keys())
if missing_columns:
    raise SystemExit(f"critical package inventory missing columns: {sorted(missing_columns)}")

packages = []
seen = set()
for row in rows:
    pkg = row.get("package", "").strip()
    if not pkg:
        raise SystemExit("critical package inventory contains empty package")
    if pkg in seen:
        raise SystemExit(f"duplicate package in critical package inventory: {pkg}")
    seen.add(pkg)

    # Only Go packages are passed to go test. Non-Go/documentation helper
    # areas remain represented in the inventory but are not coverage packages.
    if pkg.startswith("internal/") or pkg.startswith("cmd/"):
        packages.append("./" + pkg)

if not packages:
    raise SystemExit("critical package inventory contains no Go packages")

out.write_text("\n".join(packages) + "\n", encoding="utf-8")
PY

echo "Coldkeep v1.10.7 critical-path coverage report"
echo "Inventory: $INVENTORY"
echo

echo "Tracked Go packages:"
sed 's/^/  /' "$PKG_LIST"
echo

echo "Running coverage..."
go test -covermode=atomic -coverprofile="$COVER_PROFILE" $(cat "$PKG_LIST")

echo
echo "Coverage by function/package:"
go tool cover -func="$COVER_PROFILE" | tee "$COVER_FUNC" >/dev/null

python3 - "$INVENTORY" "$COVER_FUNC" "$REPORT_ROWS" <<'PY'
from pathlib import Path
import csv
import re
import sys

inventory = Path(sys.argv[1])
cover_func = Path(sys.argv[2])
report_rows = Path(sys.argv[3])

rows = list(csv.DictReader(inventory.open(newline="", encoding="utf-8")))

coverage_by_pkg = {}
line_re = re.compile(r"^(.+?):\d+:\s+\S+\s+(\d+(?:\.\d+)?)%$")
for raw in cover_func.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("total:"):
        continue
    m = line_re.match(line)
    if not m:
        continue
    file_path, pct = m.groups()
    parts = file_path.split("/")
    if len(parts) < 2:
        continue

    # Extract local package path from internal/... or cmd/...
    pkg = None
    for anchor in ("internal", "cmd"):
        if anchor in parts:
            idx = parts.index(anchor)
            pkg = "/".join(parts[idx:-1])
            break
    if not pkg:
        continue

    coverage_by_pkg.setdefault(pkg, []).append(float(pct))

def package_coverage(pkg: str) -> str:
    values = coverage_by_pkg.get(pkg)
    if not values:
        return "n/a"
    return f"{sum(values) / len(values):.1f}"

with report_rows.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "package",
            "tier",
            "domain",
            "gate_mode",
            "release_blocking_in_v1107",
            "coverage_percent",
            "coverage_role",
        ],
    )
    writer.writeheader()
    for row in rows:
        pkg = row["package"].strip()
        writer.writerow({
            "package": pkg,
            "tier": row.get("tier", "").strip(),
            "domain": row.get("domain", "").strip(),
            "gate_mode": row.get("gate_mode", "").strip(),
            "release_blocking_in_v1107": row.get("release_blocking_in_v1107", "").strip(),
            "coverage_percent": package_coverage(pkg),
            "coverage_role": row.get("coverage_role", "").strip(),
        })
PY

echo
echo "Critical package coverage summary:"
python3 - "$REPORT_ROWS" <<'PY'
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1], newline="", encoding="utf-8")))

print(f"{'package':40} {'tier':8} {'gate':30} {'block':7} {'coverage':>9}")
print("-" * 104)
for row in rows:
    print(
        f"{row['package'][:40]:40} "
        f"{row['tier'][:8]:8} "
        f"{row['gate_mode'][:30]:30} "
        f"{row['release_blocking_in_v1107'][:7]:7} "
        f"{row['coverage_percent']:>9}"
    )
PY

if [[ -n "$CSV_OUTPUT" ]]; then
  mkdir -p "$(dirname "$CSV_OUTPUT")"
  cp "$REPORT_ROWS" "$CSV_OUTPUT"
  echo
  echo "CSV report written to: $CSV_OUTPUT"
fi

echo
echo "Phase 2 mode: report-only"
echo "No coverage thresholds were enforced."
