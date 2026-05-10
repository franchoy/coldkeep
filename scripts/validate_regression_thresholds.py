#!/usr/bin/env python3
"""
Coldkeep Benchmark Regression Threshold Validator

Validates threshold configuration realism and applies thresholds to benchmark runs.
Supports mode-specific (uncompressed/compressed) and case-specific thresholds.

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
import json
import pathlib
import sys
from typing import Any, Dict, List, Optional, Tuple
import yaml


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


def main():
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
        
        passed, violations, has_hard_fails = check_regression(
            result_envelope, baseline_envelope, thresholds_config, args.mode
        )
        
        # Print report
        if not violations:
            print(f"✓ No regressions detected (mode: {args.mode})")
        else:
            print(f"\n{'HARD FAIL' if has_hard_fails else 'WARNINGS'}: "
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
            
            if hard_fails:
                print_violations("HARD FAIL", hard_fails)
            if warnings:
                print_violations("WARNING", warnings)
        
        # Write JSON report if requested
        if args.json_report:
            report = {
                'mode': args.mode,
                'passed': passed,
                'has_hard_fails': has_hard_fails,
                'violations_count': len(violations),
                'violations': violations,
            }
            args.json_report.write_text(json.dumps(report, indent=2) + '\n')
            print(f"\nReport written to {args.json_report}")
        
        return 0 if passed else 1
    
    else:
        parser.print_help()
        return 2


if __name__ == '__main__':
    sys.exit(main())
