#!/usr/bin/env python3
"""Coldkeep legacy single-observation benchmark threshold validator.

Validates threshold configuration realism and applies thresholds to benchmark runs.
Supports mode-specific (uncompressed/compressed) and case-specific thresholds.
The required aggregate v2 release gate is owned by scripts/benchmark_gate.py.

Usage:
  # Validate that thresholds are realistic against baselines
  python3 scripts/validate_regression_thresholds.py --validate

  # Apply thresholds to a benchmark result  
  python3 scripts/validate_regression_thresholds.py --check result.json \
    --baseline baseline.json --mode uncompressed

  # Apply thresholds with custom config path
  python3 scripts/validate_regression_thresholds.py --check result.json \
    --baseline baseline.json --mode uncompressed \
    --thresholds benchmarks/v1.9/regression-thresholds.yaml
"""

import argparse
import hashlib
import json
import math
import pathlib
import sys
from typing import Any, Dict, List, Optional, Tuple
import yaml

try:
    from scripts import benchmark_gate as benchmark_contract
except ImportError:  # Direct execution from scripts/ places that directory on sys.path.
    import benchmark_gate as benchmark_contract

ADVISORY_REPORT_KIND = "benchmark_timing_advisory"
ADVISORY_EXIT_CODES = {
    "BENCHMARK_TIMING_WITHIN_REFERENCE": 0,
    "BENCHMARK_TIMING_WARNING": 10,
    "BENCHMARK_TIMING_UNSTABLE": 11,
    "BENCHMARK_TIMING_NOT_EVALUATED": 12,
}


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_yaml(path: pathlib.Path) -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def load_benchmark_envelope(path: pathlib.Path) -> Dict[str, Any]:
    """Extract benchmark envelope from file (handles multiple lines)."""
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get('report_kind') == 'benchmark_gate_aggregate':
            raise RuntimeError(
                f"Aggregate v2 evidence must be validated with scripts/benchmark_gate.py: {path}"
            )
        if isinstance(obj, dict) and 'data' in obj:
            return obj
    raise RuntimeError(f"No valid benchmark envelope found in {path}")


def load_baseline_manifest(path: pathlib.Path) -> Dict[str, Any]:
    """Load baseline manifest."""
    content = path.read_text().strip()
    return json.loads(content)


def extract_rows(envelope: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """Extract benchmark rows from envelope, keyed by case name."""
    rows = {}
    for row in envelope.get('data', {}).get('rows', []):
        case = row.get('case')
        if case:
            rows[case] = row
    return rows


EXPECTED_CASES = [
    "store-large-file", "store-many-small-files", "store-mixed-dataset",
    "restore-large-file", "restore-many-files", "snapshot-creation",
    "gc-after-churn", "stats-inspect", "verify-system-deep",
]
SMALL_FIXTURE = {
    "id": "small", "seed": 1701, "large_file_size_bytes": 16 * 1024 * 1024,
    "many_small_file_count": 100, "many_small_file_size_bytes": 1024,
    "mixed_file_count": 20, "mixed_min_file_size_bytes": 1024,
    "mixed_max_file_size_bytes": 256 * 1024, "remove_every": 4,
    "case_database_isolation": False,
}


def require_fields(value: Any, expected: set[str], label: str) -> Dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise RuntimeError(f"{label} fields mismatch")
    return value


def validate_execution(value: Any, workers: int, label: str) -> None:
    value = require_fields(
        value, {"store_folder_workers", "pipeline_depth", "deterministic"}, label
    )
    if value != {
        "store_folder_workers": workers, "pipeline_depth": 1, "deterministic": True
    }:
        raise RuntimeError(f"{label} policy mismatch")


def validate_execution_stats(value: Any, *, workers: int, label: str) -> Dict[str, Any]:
    required = {
        "total_files", "total_bytes", "workers_used", "container_append_count",
        "fsync_count", "container_open_count", "container_close_count", "io",
    }
    optional = {"snapshot_metadata_write_count"}
    if not isinstance(value, dict) or not required <= set(value) or set(value) - required - optional:
        raise RuntimeError(f"{label} fields mismatch")
    io = require_fields(
        value["io"],
        {"container_opens", "container_appends", "fsyncs", "bytes_written", "bytes_read"},
        f"{label} I/O",
    )
    counters = [
        *(value[field] for field in required - {"io"}),
        *io.values(),
        value.get("snapshot_metadata_write_count", 0),
    ]
    if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in counters):
        raise RuntimeError(f"{label} counter invalid")
    if value["workers_used"] != workers or value["container_open_count"] != value["container_close_count"]:
        raise RuntimeError(f"{label} counters inconsistent")
    if (
        io["container_opens"] != value["container_open_count"]
        or io["container_appends"] != value["container_append_count"]
        or io["fsyncs"] != value["fsync_count"]
    ):
        raise RuntimeError(f"{label} duplicated counters inconsistent")
    return value


def validate_timing_envelope(envelope: Dict[str, Any], *, workers: int, legacy: bool) -> None:
    require_fields(envelope, {"status", "command", "data"}, "benchmark envelope")
    if envelope["status"] != "ok" or envelope["command"] != "benchmark":
        raise RuntimeError("benchmark envelope is not successful")
    data_fields = {"generated_at_utc", "dataset", "repeat", "execution", "execution_stats", "rows"}
    if not legacy:
        data_fields |= {"schema_version", "fixture"}
    data = require_fields(envelope["data"], data_fields, "benchmark data")
    if data.get("dataset") != "small" or data.get("repeat") != 1:
        raise RuntimeError("benchmark dataset/repeat mismatch")
    if not legacy and data.get("schema_version") != 2:
        raise RuntimeError("candidate benchmark schema must be 2")
    validate_execution(data["execution"], workers, "benchmark execution")
    if not legacy:
        fixture = require_fields(data["fixture"], set(SMALL_FIXTURE) | {"ordered_cases"}, "fixture")
        for field, expected in SMALL_FIXTURE.items():
            if fixture[field] != expected:
                raise RuntimeError(f"fixture field {field} mismatch")
        ordered = fixture["ordered_cases"]
        if not isinstance(ordered, list) or [item.get("name") for item in ordered] != EXPECTED_CASES:
            raise RuntimeError("fixture case order mismatch")
        if [item.get("seed") for item in ordered] != [1712 + index * 10 for index in range(9)]:
            raise RuntimeError("fixture case seed mismatch")
    rows = data["rows"]
    if not isinstance(rows, list) or [row.get("case") for row in rows if isinstance(row, dict)] != EXPECTED_CASES:
        raise RuntimeError("benchmark row case order mismatch")
    row_fields = {
        "case", "duration_ms", "throughput_mbps", "execution", "execution_stats",
        "diagnostic_final_state",
    } if not legacy else {
        "case", "duration_ms", "throughput_mbps", "execution", "execution_stats",
    }
    row_stats: List[Dict[str, Any]] = []
    for row in rows:
        require_fields(row, row_fields, f"row {row.get('case')}")
        validate_execution(row["execution"], workers, f"row {row['case']} execution")
        duration = row["duration_ms"]
        throughput = row["throughput_mbps"]
        if (
            isinstance(duration, bool) or not isinstance(duration, (int, float))
            or not math.isfinite(float(duration)) or float(duration) <= 0
            or isinstance(throughput, bool) or not isinstance(throughput, (int, float))
            or not math.isfinite(float(throughput)) or float(throughput) <= 0
        ):
            raise RuntimeError(f"row {row['case']} timing values invalid")
        stats = validate_execution_stats(
            row["execution_stats"], workers=workers, label=f"row {row['case']} execution_stats"
        )
        if not legacy:
            benchmark_contract.hard_final_state(row)
        expected_throughput = stats["total_bytes"] / (1024 * 1024) / (float(duration) / 1000.0)
        if not legacy and not math.isclose(
            float(throughput), expected_throughput, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise RuntimeError(f"row {row['case']} throughput is not derived from bytes and duration")
        row_stats.append(stats)

    totals = validate_execution_stats(
        data["execution_stats"], workers=workers, label="benchmark execution_stats"
    )
    sum_fields = {
        "total_files", "total_bytes", "container_append_count", "fsync_count",
        "container_open_count", "container_close_count", "snapshot_metadata_write_count",
    }
    for field in sum_fields:
        if totals.get(field, 0) != sum(stats.get(field, 0) for stats in row_stats):
            raise RuntimeError(f"benchmark execution_stats {field} total mismatch")
    for field in totals["io"]:
        if totals["io"][field] != sum(stats["io"][field] for stats in row_stats):
            raise RuntimeError(f"benchmark execution_stats I/O {field} total mismatch")


def get_threshold(
    thresholds_config: Dict[str, Any],
    mode: str,
    case: str,
    metric: str,
    stage: str = "1_warnings_only",
) -> Optional[float]:
    """
    Get threshold for a specific mode, case, and metric.
    
    Args:
        thresholds_config: Loaded regression-thresholds.yaml config
        mode: "uncompressed" or "compressed"
        case: benchmark case name
        metric: "throughput" or "duration"
        stage: compression stage (ignored for uncompressed)
    
    Returns:
        Threshold percentage, or None if not applicable
    """
    # Check per-case override first
    per_case = thresholds_config.get('per_case_overrides', {}).get(mode, {})
    if case in per_case:
        case_config = per_case[case]
        
        if mode == "uncompressed":
            if metric == "throughput":
                return case_config.get('throughput_regression_pct')
            elif metric == "duration":
                return case_config.get('duration_regression_pct')
        else:  # compressed
            if metric == "throughput":
                if case_config.get('stage') == '1_warnings_only':
                    return case_config.get('throughput_regression_warning_pct')
            elif metric == "duration":
                if case_config.get('stage') == '1_warnings_only':
                    return case_config.get('duration_regression_warning_pct')
    
    # Fall back to defaults
    defaults = thresholds_config.get('defaults', {}).get(mode, {})
    
    if mode == "uncompressed":
        if metric == "throughput" or metric == "duration":
            return defaults.get(f'{metric}_regression_pct')
    else:  # compressed
        if metric == "throughput":
            return defaults.get('throughput_regression_warning_pct')
        elif metric == "duration":
            return defaults.get('duration_regression_warning_pct')
    
    return None


def is_hard_fail(
    thresholds_config: Dict[str, Any],
    mode: str,
    case: str = None,
) -> bool:
    """Determine if a violation is a hard fail or warning."""
    if mode == "uncompressed":
        return True  # Uncompressed violations always hard-fail
    
    # Compressed mode: check if in warning stage
    defaults = thresholds_config.get('defaults', {}).get('compressed', {})
    default_stage = defaults.get('stage', '1_warnings_only')
    
    if case:
        per_case = thresholds_config.get('per_case_overrides', {}).get('compressed', {})
        if case in per_case:
            case_stage = per_case[case].get('stage', default_stage)
            return case_stage != '1_warnings_only'
    
    return default_stage != '1_warnings_only'


def validate_thresholds_realistic(
    thresholds_config: Dict[str, Any],
    baseline_manifest: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """
    Validate that thresholds are realistic against baseline compression deltas.
    
    Returns:
        (is_realistic, issues)
    """
    issues = []
    case_comparison = baseline_manifest.get('case_comparison', [])
    
    # Check compressed thresholds against actual deltas
    for case_data in case_comparison:
        case_name = case_data.get('case')
        duration_delta = abs(case_data.get('duration_delta_pct_zstd_vs_none', 0))
        throughput_delta = abs(case_data.get('throughput_delta_pct_zstd_vs_none', 0))
        
        # Get compressed warning thresholds
        duration_threshold = get_threshold(
            thresholds_config, 'compressed', case_name, 'duration'
        )
        throughput_threshold = get_threshold(
            thresholds_config, 'compressed', case_name, 'throughput'
        )
        
        # Warn if baseline delta is close to or exceeds threshold
        # (thresholds should be higher than baseline natural variance)
        if duration_threshold and duration_delta > duration_threshold * 0.8:
            issues.append(
                f"Compressed duration threshold {duration_threshold}% may be too tight "
                f"for {case_name} (baseline delta: {duration_delta:.1f}%)"
            )
        
        if throughput_threshold and throughput_delta > throughput_threshold * 0.8:
            issues.append(
                f"Compressed throughput threshold {throughput_threshold}% may be too tight "
                f"for {case_name} (baseline delta: {throughput_delta:.1f}%)"
            )
    
    return len(issues) == 0, issues


def check_regression(
    result_envelope: Dict[str, Any],
    baseline_envelope: Dict[str, Any],
    thresholds_config: Dict[str, Any],
    mode: str,
) -> Tuple[bool, List[Dict[str, Any]], bool]:
    """
    Check regression against baseline using configured thresholds.
    
    Returns:
        (passed, violations, has_hard_fails)
        
    violations format:
        [{
            'case': str,
            'metric': 'throughput' or 'duration',
            'baseline_value': float,
            'result_value': float,
            'delta_pct': float,
            'threshold_pct': float,
            'is_hard_fail': bool,
        }, ...]
    """
    result_rows = extract_rows(result_envelope)
    baseline_rows = extract_rows(baseline_envelope)
    
    violations = []
    has_hard_fails = False
    
    common_cases = sorted(set(result_rows.keys()) & set(baseline_rows.keys()))
    
    for case in common_cases:
        result_row = result_rows[case]
        baseline_row = baseline_rows[case]
        
        result_duration = float(result_row.get('duration_ms', 0))
        baseline_duration = float(baseline_row.get('duration_ms', 0))
        result_throughput = float(result_row.get('throughput_mbps', 0))
        baseline_throughput = float(baseline_row.get('throughput_mbps', 0))
        
        # Check duration regression
        if baseline_duration > 0:
            duration_delta_pct = ((result_duration - baseline_duration) / baseline_duration) * 100
            if duration_delta_pct > 0:  # Only report regressions (positive delta)
                threshold = get_threshold(thresholds_config, mode, case, 'duration')
                if threshold is not None and duration_delta_pct > threshold:
                    is_hard_fail = is_hard_fail_check(
                        thresholds_config, mode, case
                    )
                    violations.append({
                        'case': case,
                        'metric': 'duration',
                        'baseline_value': baseline_duration,
                        'result_value': result_duration,
                        'delta_pct': duration_delta_pct,
                        'threshold_pct': threshold,
                        'is_hard_fail': is_hard_fail,
                    })
                    if is_hard_fail:
                        has_hard_fails = True
        
        # Check throughput regression (lower throughput = regression)
        if baseline_throughput > 0:
            throughput_delta_pct = ((baseline_throughput - result_throughput) / baseline_throughput) * 100
            if throughput_delta_pct > 0:  # Only report regressions (positive delta)
                threshold = get_threshold(thresholds_config, mode, case, 'throughput')
                if threshold is not None and throughput_delta_pct > threshold:
                    is_hard_fail = is_hard_fail_check(
                        thresholds_config, mode, case
                    )
                    violations.append({
                        'case': case,
                        'metric': 'throughput',
                        'baseline_value': baseline_throughput,
                        'result_value': result_throughput,
                        'delta_pct': throughput_delta_pct,
                        'threshold_pct': threshold,
                        'is_hard_fail': is_hard_fail,
                    })
                    if is_hard_fail:
                        has_hard_fails = True
    
    passed = len([v for v in violations if v['is_hard_fail']]) == 0
    return passed, violations, has_hard_fails


def is_hard_fail_check(
    thresholds_config: Dict[str, Any],
    mode: str,
    case: str = None,
) -> bool:
    """Helper for check_regression."""
    if mode == "uncompressed":
        return True  # Uncompressed violations always hard-fail
    
    # Compressed mode: check if in warning stage
    defaults = thresholds_config.get('defaults', {}).get('compressed', {})
    default_stage = defaults.get('stage', '1_warnings_only')
    
    if case:
        per_case = thresholds_config.get('per_case_overrides', {}).get('compressed', {})
        if case in per_case:
            case_stage = per_case[case].get('stage', default_stage)
            return case_stage != '1_warnings_only'
    
    return default_stage != '1_warnings_only'


def advisory_report(
    *,
    mode: str,
    result_path: pathlib.Path,
    baseline_path: pathlib.Path,
    violations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    classification = (
        "BENCHMARK_TIMING_WARNING" if violations else "BENCHMARK_TIMING_WITHIN_REFERENCE"
    )
    return {
        "schema_version": 1,
        "report_kind": ADVISORY_REPORT_KIND,
        "status": "complete",
        "classification": classification,
        "authority": "informational",
        "reference_kind": "historical_v1.9_absolute",
        "mode": mode,
        "candidate_sha256": sha256_file(result_path),
        "baseline_sha256": sha256_file(baseline_path),
        "violations_count": len(violations),
        "violations": violations,
    }


def validate_advisory_report(report: Any) -> Dict[str, Any]:
    report = require_fields(
        report,
        {
            "schema_version", "report_kind", "status", "classification", "authority",
            "reference_kind", "mode", "candidate_sha256", "baseline_sha256",
            "violations_count", "violations",
        },
        "timing advisory report",
    )
    if (
        report["schema_version"] != 1
        or report["report_kind"] != ADVISORY_REPORT_KIND
        or report["status"] not in {"complete", "not_evaluated"}
        or report["classification"] not in ADVISORY_EXIT_CODES
        or report["authority"] != "informational"
        or report["reference_kind"] != "historical_v1.9_absolute"
        or report["mode"] not in {"uncompressed", "compressed"}
    ):
        raise RuntimeError("timing advisory report identity mismatch")
    for field in ("candidate_sha256", "baseline_sha256"):
        value = report[field]
        if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
            raise RuntimeError(f"timing advisory {field} invalid")
    if (
        isinstance(report["violations_count"], bool)
        or not isinstance(report["violations_count"], int)
        or report["violations_count"] < 0
        or not isinstance(report["violations"], list)
        or report["violations_count"] != len(report["violations"])
    ):
        raise RuntimeError("timing advisory violation inventory mismatch")
    if report["classification"] == "BENCHMARK_TIMING_WITHIN_REFERENCE" and report["violations"]:
        raise RuntimeError("within-reference advisory contains violations")
    if report["classification"] == "BENCHMARK_TIMING_WARNING" and not report["violations"]:
        raise RuntimeError("timing warning contains no violations")
    return report


def verify_advisory_exit(report_path: pathlib.Path, observed_exit_code: int) -> int:
    report = validate_advisory_report(json.loads(report_path.read_text(encoding="utf-8")))
    expected = ADVISORY_EXIT_CODES[report["classification"]]
    if observed_exit_code != expected:
        raise RuntimeError(
            f"timing advisory exit mismatch: classification expects {expected}, got {observed_exit_code}"
        )
    return 0


def _main():
    parser = argparse.ArgumentParser(
        description="Validate benchmark regression thresholds"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Validate command
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate thresholds are realistic'
    )
    validate_parser.add_argument(
        '--manifest',
        type=pathlib.Path,
        default=pathlib.Path('benchmarks/v1.9/baselines/baseline-manifest-v1.9.json'),
        help='Path to baseline manifest'
    )
    validate_parser.add_argument(
        '--thresholds',
        type=pathlib.Path,
        default=pathlib.Path('benchmarks/v1.9/regression-thresholds.yaml'),
        help='Path to regression thresholds config'
    )
    
    # Check command
    check_parser = subparsers.add_parser(
        'check',
        help='Check regression against baseline'
    )
    check_parser.add_argument(
        'result',
        type=pathlib.Path,
        help='Path to benchmark result file'
    )
    check_parser.add_argument(
        '--baseline',
        type=pathlib.Path,
        required=True,
        help='Path to baseline benchmark file'
    )
    check_parser.add_argument(
        '--mode',
        required=True,
        choices=['uncompressed', 'compressed'],
        help='Benchmark mode'
    )
    check_parser.add_argument(
        '--thresholds',
        type=pathlib.Path,
        default=pathlib.Path('benchmarks/v1.9/regression-thresholds.yaml'),
        help='Path to regression thresholds config'
    )
    check_parser.add_argument(
        '--json-report',
        type=pathlib.Path,
        help='Write JSON report to file'
    )
    check_parser.add_argument(
        '--policy',
        choices=['legacy', 'hosted-advisory'],
        default='legacy',
        help='Select legacy failure behavior or hosted informational timing policy'
    )

    verify_parser = subparsers.add_parser(
        'verify-advisory-exit',
        help='Verify an advisory report classification against the observed comparator exit'
    )
    verify_parser.add_argument('--report', type=pathlib.Path, required=True)
    verify_parser.add_argument('--observed-exit-code', type=int, required=True)
    
    args = parser.parse_args()
    
    if args.command == 'validate':
        thresholds_config = load_yaml(args.thresholds)
        baseline_manifest = load_baseline_manifest(args.manifest)
        
        is_realistic, issues = validate_thresholds_realistic(
            thresholds_config, baseline_manifest
        )
        
        if is_realistic:
            print("✓ Thresholds are realistic relative to baseline compression deltas")
            return 0
        else:
            print("⚠ Threshold realism warnings:")
            for issue in issues:
                print(f"  {issue}")
            return 0  # Warning, not error
    
    elif args.command == 'check':
        thresholds_config = load_yaml(args.thresholds)
        result_envelope = load_benchmark_envelope(args.result)
        baseline_envelope = load_benchmark_envelope(args.baseline)
        advisory = args.policy == 'hosted-advisory'
        if advisory:
            result_execution = result_envelope.get('data', {}).get('execution', {})
            baseline_execution = baseline_envelope.get('data', {}).get('execution', {})
            workers = result_execution.get('store_folder_workers')
            if workers not in {1, 4} or baseline_execution.get('store_folder_workers') != workers:
                raise RuntimeError('candidate/baseline worker profile mismatch')
            validate_timing_envelope(result_envelope, workers=workers, legacy=False)
            validate_timing_envelope(baseline_envelope, workers=workers, legacy=True)
        
        passed, violations, has_hard_fails = check_regression(
            result_envelope, baseline_envelope, thresholds_config, args.mode
        )
        
        # Print report
        if not violations:
            print(f"✓ No regressions detected (mode: {args.mode})")
        else:
            label = 'TIMING WARNING' if advisory else ('HARD FAIL' if has_hard_fails else 'WARNINGS')
            print(f"\n{label}: "
                  f"{len(violations)} regression(s) detected (mode: {args.mode})\n")
            
            # Group by severity
            hard_fails = [v for v in violations if v['is_hard_fail']]
            warnings = [v for v in violations if not v['is_hard_fail']]
            
            def print_violations(label, vlist):
                if not vlist:
                    return
                print(f"\n{label} ({len(vlist)}):")
                print(f"  {'Case':<30} {'Metric':<12} {'Baseline':<12} {'Result':<12} "
                      f"{'Delta':<10} {'Threshold':<10}")
                print("  " + "-" * 100)
                for v in vlist:
                    delta_str = f"{v['delta_pct']:+.1f}%"
                    threshold_str = f"{v['threshold_pct']:.1f}%"
                    base_str = f"{v['baseline_value']:.2f}"
                    result_str = f"{v['result_value']:.2f}"
                    print(f"  {v['case']:<30} {v['metric']:<12} {base_str:<12} "
                          f"{result_str:<12} {delta_str:<10} {threshold_str:<10}")
            
            if advisory:
                print_violations("TIMING ADVISORY", violations)
            else:
                if hard_fails:
                    print_violations("HARD FAIL", hard_fails)
                if warnings:
                    print_violations("WARNING", warnings)
        
        # Write JSON report if requested
        if args.json_report:
            report = advisory_report(
                mode=args.mode,
                result_path=args.result,
                baseline_path=args.baseline,
                violations=violations,
            ) if advisory else {
                'mode': args.mode,
                'passed': passed,
                'has_hard_fails': has_hard_fails,
                'violations_count': len(violations),
                'violations': violations,
            }
            if advisory:
                validate_advisory_report(report)
            args.json_report.write_text(json.dumps(report, indent=2) + '\n')
            print(f"\nReport written to {args.json_report}")

        if advisory:
            if not args.json_report:
                raise RuntimeError('hosted-advisory policy requires --json-report')
            return ADVISORY_EXIT_CODES[report['classification']]
        return 0 if passed else 1

    elif args.command == 'verify-advisory-exit':
        return verify_advisory_exit(args.report, args.observed_exit_code)
    
    else:
        parser.print_help()
        return 2


def main() -> int:
    try:
        return _main()
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError, yaml.YAMLError) as exc:
        print(f"benchmark timing policy error: {exc}", file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
