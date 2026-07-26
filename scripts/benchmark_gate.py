#!/usr/bin/env python3
"""Capture and validate statistically bounded Coldkeep benchmark evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
import time
from decimal import Decimal
from typing import Any, Iterable

SCHEMA_VERSION = 2
REPORT_KIND = "benchmark_gate_aggregate"
MANIFEST_KIND = "benchmark_gate_manifest"
FIXTURE_ID = "ci-stable-v1"
FIXTURE_FIELDS = {
    "id": FIXTURE_ID,
    "seed": 1701,
    "large_file_size_bytes": 96 * 1024 * 1024,
    "many_small_file_count": 600,
    "many_small_file_size_bytes": 1024,
    "mixed_file_count": 400,
    "mixed_min_file_size_bytes": 1024,
    "mixed_max_file_size_bytes": 256 * 1024,
    "remove_every": 4,
    "case_database_isolation": True,
}
EXPECTED_CASES = [
    "store-large-file",
    "store-many-small-files",
    "store-mixed-dataset",
    "restore-large-file",
    "restore-many-files",
    "snapshot-creation",
    "gc-after-churn",
    "stats-inspect",
    "verify-system-deep",
]
HARD_ENV_FIELDS = (
    "runner_os",
    "runner_arch",
    "cpu_count",
    "go_version",
    "postgres_version",
    "database_image_digest",
)
REQUIRED_PROVENANCE_FIELDS = (
    "source_commit",
    "generated_at_utc",
    "workflow_run_id",
    "workflow_job_id",
    "workflow_run_attempt",
    "runner_os",
    "runner_image",
    "runner_arch",
    "cpu_count",
    "go_version",
    "postgres_version",
    "database_image_digest",
    "binary_sha256",
)
MANIFEST_PROFILES = {
    "none-w1": ("none", 1),
    "none-w4": ("none", 4),
    "zstd-w1": ("zstd", 1),
    "zstd-w4": ("zstd", 4),
}
DIAGNOSTIC_SCHEMA_VERSION = 1


class GateError(RuntimeError):
    """A deterministic benchmark-gate validation failure."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json_strict(path: pathlib.Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    decoder = json.JSONDecoder(parse_constant=lambda value: (_ for _ in ()).throw(
        GateError(f"non-finite JSON value {value!r} in {path}")
    ))
    try:
        value, end = decoder.raw_decode(text)
    except (json.JSONDecodeError, GateError) as exc:
        raise GateError(f"malformed JSON in {path}: {exc}") from exc
    if text[end:].strip():
        raise GateError(f"trailing JSON or content in {path}")
    if not isinstance(value, dict):
        raise GateError(f"top-level JSON value in {path} must be an object")
    return value


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def require_number(value: Any, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise GateError(f"{label} must be finite")
    if positive and result <= 0:
        raise GateError(f"{label} must be positive")
    if not positive and result < 0:
        raise GateError(f"{label} must not be negative")
    return result


def percentile_nearest_rank(values: list[float], percentile: float) -> float:
    if not values:
        raise GateError("cannot calculate percentile of empty values")
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def summarize(values: Iterable[float]) -> dict[str, float]:
    samples = [float(value) for value in values]
    if not samples:
        raise GateError("cannot summarize an empty sample set")
    if any(not math.isfinite(value) or value <= 0 for value in samples):
        raise GateError("sample durations must be finite and positive")
    median = float(statistics.median(samples))
    mean = float(statistics.mean(samples))
    mad = float(statistics.median(abs(value - median) for value in samples))
    stddev = float(statistics.stdev(samples)) if len(samples) > 1 else 0.0
    return {
        "median_duration_ms": median,
        "mean_duration_ms": mean,
        "min_duration_ms": min(samples),
        "max_duration_ms": max(samples),
        "sample_stddev_ms": stddev,
        "mad_ms": mad,
        "mad_ratio_pct": mad / median * 100.0,
        "coefficient_of_variation_pct": stddev / mean * 100.0 if mean else 0.0,
    }


def fixture_stats(row: dict[str, Any]) -> dict[str, Any]:
    stats = row.get("execution_stats")
    execution = row.get("execution")
    if not isinstance(stats, dict) or not isinstance(execution, dict):
        raise GateError(f"case {row.get('case')!r} lacks execution metadata")
    return {"execution": execution, "execution_stats": stats}


def require_nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{label} must be a non-negative integer")
    return value


def require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise GateError(f"{label} must be lowercase SHA-256")
    return value


def validate_diagnostic_final_state(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema_version") != DIAGNOSTIC_SCHEMA_VERSION:
        raise GateError(f"{label} diagnostic final state schema mismatch")
    expected_keys = {
        "schema_version", "logical_files", "logical_statuses", "chunk_graph",
        "restored_tree", "snapshots", "snapshot_count", "gc", "verification",
        "physical", "physical_layout_sha256",
    }
    if set(value) != expected_keys:
        raise GateError(f"{label} diagnostic final state fields mismatch")
    for section_name in ("logical_files", "chunk_graph", "restored_tree", "snapshots"):
        section = value.get(section_name)
        if not isinstance(section, dict) or set(section) != {"count", "total_bytes", "sha256"}:
            raise GateError(f"{label} diagnostic {section_name} fields mismatch")
        require_nonnegative_integer(section["count"], f"{label} diagnostic {section_name} count")
        require_nonnegative_integer(
            section["total_bytes"], f"{label} diagnostic {section_name} total_bytes"
        )
        require_sha256(section["sha256"], f"{label} diagnostic {section_name} digest")
    statuses = value.get("logical_statuses")
    if not isinstance(statuses, dict) or set(statuses) != {"completed", "processing", "aborted"}:
        raise GateError(f"{label} diagnostic logical status fields mismatch")
    for field in statuses:
        require_nonnegative_integer(statuses[field], f"{label} diagnostic logical status {field}")
    require_nonnegative_integer(value["snapshot_count"], f"{label} diagnostic snapshot_count")
    gc_totals = value.get("gc")
    expected_gc = {
        "total_chunks", "reachable_chunks", "unreachable_chunks",
        "logically_reclaimable_bytes", "physically_reclaimable_bytes",
        "packed_blocks_live", "packed_blocks_dead", "packed_bytes_live",
        "packed_bytes_reclaimable", "retained_dead_bytes",
    }
    if not isinstance(gc_totals, dict) or set(gc_totals) != expected_gc:
        raise GateError(f"{label} diagnostic GC fields mismatch")
    for field in gc_totals:
        require_nonnegative_integer(gc_totals[field], f"{label} diagnostic GC {field}")
    verification = value.get("verification")
    expected_verification = {
        "blocks_checked", "physical_hashes_checked", "compressed_hashes_checked",
        "logical_hashes_checked", "compressed_blocks_checked", "physical_file_issues",
        "snapshot_membership_rows", "snapshot_reachability_issues",
    }
    if not isinstance(verification, dict) or set(verification) != expected_verification:
        raise GateError(f"{label} diagnostic verification fields mismatch")
    for field in verification:
        require_nonnegative_integer(
            verification[field], f"{label} diagnostic verification {field}"
        )
    physical = value.get("physical")
    expected_physical = {
        "container_count", "storage_block_count", "legacy_block_count",
        "chunk_reference_count", "payload_bytes", "container_bytes", "canonical_sha256",
    }
    if not isinstance(physical, dict) or set(physical) != expected_physical:
        raise GateError(f"{label} diagnostic physical fields mismatch")
    for field in expected_physical - {"canonical_sha256"}:
        require_nonnegative_integer(physical[field], f"{label} diagnostic physical {field}")
    require_sha256(physical["canonical_sha256"], f"{label} diagnostic canonical physical digest")
    require_sha256(value["physical_layout_sha256"], f"{label} diagnostic physical layout digest")
    return value


def hard_final_state(row: dict[str, Any]) -> dict[str, Any]:
    state = validate_diagnostic_final_state(
        row.get("diagnostic_final_state"), f"case {row.get('case')!r}"
    )
    physical = state["physical"]
    return {
        "logical_files": state["logical_files"],
        "logical_statuses": state["logical_statuses"],
        "chunk_graph": state["chunk_graph"],
        "restored_tree": state["restored_tree"],
        "snapshots": state["snapshots"],
        "snapshot_count": state["snapshot_count"],
        "gc": state["gc"],
        "verification": state["verification"],
        "physical_content": {
            "chunk_reference_count": physical["chunk_reference_count"],
            "canonical_sha256": physical["canonical_sha256"],
        },
    }


def validate_fixture(fixture: Any) -> list[dict[str, Any]]:
    if not isinstance(fixture, dict):
        raise GateError("fixture must be an object")
    for field, expected in FIXTURE_FIELDS.items():
        if fixture.get(field) != expected:
            raise GateError(f"fixture field {field!r} does not match {FIXTURE_ID}")
    ordered = fixture.get("ordered_cases")
    if not isinstance(ordered, list) or len(ordered) != len(EXPECTED_CASES):
        raise GateError("fixture ordered case count mismatch")
    for index, (descriptor, expected_name) in enumerate(zip(ordered, EXPECTED_CASES)):
        if not isinstance(descriptor, dict):
            raise GateError(f"fixture case at index {index} must be an object")
        expected_seed = 1712 + index * 10
        if descriptor.get("name") != expected_name or descriptor.get("seed") != expected_seed:
            raise GateError(f"fixture case descriptor mismatch at index {index}")
    return ordered


def validate_provenance(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GateError("aggregate provenance must be an object")
    for field in REQUIRED_PROVENANCE_FIELDS:
        if value.get(field) in (None, "", "unknown"):
            raise GateError(f"aggregate provenance field {field!r} is missing")
    if not re.fullmatch(r"[0-9a-f]{40}", str(value["source_commit"])):
        raise GateError("aggregate source_commit must be a full lowercase commit SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", str(value["binary_sha256"])):
        raise GateError("aggregate binary_sha256 must be lowercase SHA-256")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(value["database_image_digest"])):
        raise GateError("aggregate database_image_digest must be a sha256 digest")
    if (
        isinstance(value["cpu_count"], bool)
        or not isinstance(value["cpu_count"], int)
        or value["cpu_count"] <= 0
    ):
        raise GateError("aggregate cpu_count must be a positive integer")
    generated = str(value["generated_at_utc"])
    if not generated.endswith("Z"):
        raise GateError("aggregate generated_at_utc must be UTC")
    try:
        dt.datetime.fromisoformat(generated[:-1] + "+00:00")
    except ValueError as exc:
        raise GateError("aggregate generated_at_utc must be RFC3339") from exc
    return value


def validate_raw_report(
    envelope: dict[str, Any],
    *,
    workers: int,
    compression: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if envelope.get("status") != "ok" or envelope.get("command") != "benchmark":
        raise GateError("raw report must be a successful benchmark envelope")
    data = envelope.get("data")
    if not isinstance(data, dict):
        raise GateError("raw report data must be an object")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise GateError(f"raw report schema must be {SCHEMA_VERSION}")
    if data.get("dataset") != FIXTURE_ID or data.get("repeat") != 1:
        raise GateError("raw report has the wrong dataset or repeat count")

    execution = data.get("execution")
    if not isinstance(execution, dict):
        raise GateError("raw report execution must be an object")
    if (
        execution.get("store_folder_workers") != workers
        or execution.get("pipeline_depth") != 1
        or execution.get("deterministic") is not True
    ):
        raise GateError("raw report execution policy mismatch")

    validate_fixture(data.get("fixture"))

    rows = data.get("rows")
    if not isinstance(rows, list) or not rows:
        raise GateError("raw report rows must be a non-empty array")
    names = [row.get("case") for row in rows if isinstance(row, dict)]
    if names != EXPECTED_CASES:
        raise GateError("raw report case set/order mismatch")
    if len(set(names)) != len(names):
        raise GateError("raw report contains duplicate cases")

    for row in rows:
        if not isinstance(row, dict):
            raise GateError("raw report row must be an object")
        duration = require_number(row.get("duration_ms"), f"{row.get('case')} duration", positive=True)
        throughput = require_number(
            row.get("throughput_mbps"),
            f"{row.get('case')} throughput",
            positive=True,
        )
        row_execution = row.get("execution")
        if row_execution != execution:
            raise GateError(f"{row.get('case')} execution policy mismatch")
        stats = row.get("execution_stats")
        if not isinstance(stats, dict):
            raise GateError(f"{row.get('case')} execution_stats must be an object")
        logical_files = stats.get("total_files")
        if isinstance(logical_files, bool) or not isinstance(logical_files, int) or logical_files <= 0:
            raise GateError(f"{row.get('case')} logical files must be a positive integer")
        logical_bytes = require_number(
            stats.get("total_bytes"),
            f"{row.get('case')} logical bytes",
            positive=True,
        )
        expected_throughput = logical_bytes / (1024.0 * 1024.0) / (duration / 1000.0)
        if not math.isclose(throughput, expected_throughput, rel_tol=1e-12, abs_tol=1e-12):
            raise GateError(f"{row.get('case')} derived throughput is inconsistent")
        if stats.get("workers_used") != workers:
            raise GateError(f"{row.get('case')} workers_used mismatch")
        io_stats = stats.get("io")
        if not isinstance(io_stats, dict):
            raise GateError(f"{row.get('case')} I/O counters must be an object")
        for field in ("container_opens", "container_appends", "fsyncs", "bytes_written", "bytes_read"):
            require_number(io_stats.get(field), f"{row.get('case')} I/O counter {field}")
        hard_final_state(row)

    # Compression is supplied by the controlled environment rather than the v2
    # raw payload. Recording it here makes that ownership explicit.
    if compression not in {"none", "zstd"}:
        raise GateError(f"unsupported compression profile {compression!r}")
    return data, rows


def command_output(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout.strip()


def git_value(args: list[str], default: str = "unknown") -> str:
    try:
        return command_output(["git", *args])
    except (OSError, subprocess.CalledProcessError):
        return default


def host_load() -> dict[str, Any]:
    load = os.getloadavg() if hasattr(os, "getloadavg") else (0.0, 0.0, 0.0)
    disk = shutil.disk_usage(pathlib.Path.cwd())
    return {
        "load_1m": load[0],
        "load_5m": load[1],
        "load_15m": load[2],
        "free_disk_bytes": disk.free,
    }


def provenance(args: argparse.Namespace, binary_hash: str) -> dict[str, Any]:
    return {
        "source_commit": args.source_commit or os.environ.get("GITHUB_SHA") or git_value(["rev-parse", "HEAD"]),
        "source_tag": args.source_tag or None,
        "generated_at_utc": utc_now(),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "workflow_job_id": os.environ.get("GITHUB_JOB", "local"),
        "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
        "runner_os": os.environ.get("RUNNER_OS", sys.platform),
        "runner_image": os.environ.get("ImageVersion", "local"),
        "runner_arch": os.environ.get("RUNNER_ARCH", os.uname().machine if hasattr(os, "uname") else "unknown"),
        "cpu_count": os.cpu_count() or 0,
        "go_version": args.go_version or command_output(["go", "version"]),
        "postgres_version": args.postgres_version,
        "database_image_digest": args.database_image_digest,
        "binary_sha256": binary_hash,
    }


def capture_sample(
    args: argparse.Namespace,
    output_path: pathlib.Path,
    expected_binary_hash: str,
) -> tuple[dict[str, Any], float, dict[str, Any]]:
    if sha256_file(args.binary) != expected_binary_hash:
        raise GateError("benchmark binary hash changed during sampling")
    before = host_load()
    started = time.monotonic()
    completed = subprocess.run(
        [
            str(args.binary),
            "benchmark",
            "run",
            "--dataset",
            args.dataset,
            "--workers",
            str(args.workers),
            "--repeat",
            "1",
            "--output",
            "json",
        ],
        text=True,
        capture_output=True,
        env={**os.environ, "COLDKEEP_COMPRESSION": args.compression},
    )
    elapsed_ms = (time.monotonic() - started) * 1000.0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(completed.stdout, encoding="utf-8")
    output_path.with_suffix(".stderr").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise GateError(f"benchmark sample failed with exit {completed.returncode}")
    envelope = load_json_strict(output_path)
    validate_raw_report(envelope, workers=args.workers, compression=args.compression)
    return envelope, elapsed_ms, {"before": before, "after": host_load()}


def sample_command(args: argparse.Namespace) -> int:
    if args.dataset != FIXTURE_ID:
        raise GateError(f"gate sampling requires dataset {FIXTURE_ID!r}")
    if args.warmups < 0 or args.samples <= 0:
        raise GateError("warmups must be non-negative and samples must be positive")
    if not args.binary.is_file():
        raise GateError(f"binary does not exist: {args.binary}")
    args.binary = args.binary.resolve()
    if os.environ.get("COLDKEEP_CODEC") != "aes-gcm":
        raise GateError("gate sampling requires COLDKEEP_CODEC=aes-gcm")
    for name in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE"):
        if not os.environ.get(name):
            raise GateError(f"gate sampling requires {name}")
    args.output_dir.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(args.output_dir.parent).free < args.minimum_free_disk_bytes:
        raise GateError("insufficient free disk for benchmark sampling")

    args.output_dir.mkdir(parents=True, exist_ok=False)
    binary_hash = sha256_file(args.binary)
    raw_reports: list[dict[str, Any]] = []
    command_durations: list[float] = []
    loads: list[dict[str, Any]] = []

    for index in range(args.warmups):
        capture_sample(
            args,
            args.output_dir / "raw" / f"warmup-{index + 1:02d}.json",
            binary_hash,
        )
    for index in range(args.samples):
        envelope, elapsed_ms, load = capture_sample(
            args,
            args.output_dir / "raw" / f"sample-{index + 1:02d}.json",
            binary_hash,
        )
        raw_reports.append(envelope)
        command_durations.append(elapsed_ms)
        loads.append(load)

    first_data, first_rows = validate_raw_report(
        raw_reports[0], workers=args.workers, compression=args.compression
    )
    cases = []
    fixture_cases = first_data["fixture"]["ordered_cases"]
    for case_index, first_row in enumerate(first_rows):
        case_name = first_row["case"]
        expected_stats = fixture_stats(first_row)
        expected_final_state = hard_final_state(first_row)
        diagnostic_final_state = first_row["diagnostic_final_state"]
        durations = []
        for sample_index, envelope in enumerate(raw_reports):
            _, rows = validate_raw_report(
                envelope, workers=args.workers, compression=args.compression
            )
            row = rows[case_index]
            if row["case"] != case_name:
                raise GateError(f"case order changed in sample {sample_index + 1}")
            if hard_final_state(row) != expected_final_state:
                raise GateError(
                    f"hard final state changed for {case_name} in sample {sample_index + 1}"
                )
            if fixture_stats(row) != expected_stats:
                raise GateError(
                    f"fixture counters changed for {case_name} in sample {sample_index + 1}"
                )
            durations.append(float(row["duration_ms"]))
        summary = summarize(durations)
        logical_bytes = first_row["execution_stats"]["total_bytes"]
        summary["throughput_mbps"] = (
            logical_bytes / (1024.0 * 1024.0) / (summary["median_duration_ms"] / 1000.0)
        )
        cases.append(
            {
                "case": case_name,
                "seed": fixture_cases[case_index]["seed"],
                "logical_files": first_row["execution_stats"]["total_files"],
                "logical_bytes": logical_bytes,
                "sample_durations_ms": durations,
                "diagnostic_final_state": diagnostic_final_state,
                "fixture_stats": expected_stats,
                **summary,
            }
        )

    aggregate = {
        "schema_version": SCHEMA_VERSION,
        "report_kind": REPORT_KIND,
        "status": "ok",
        "provenance": provenance(args, binary_hash),
        "profile": {
            "codec": os.environ.get("COLDKEEP_CODEC", ""),
            "compression": args.compression,
            "dataset": args.dataset,
            "workers": args.workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "fixture": first_data["fixture"],
        "warmup_count": args.warmups,
        "sample_count": args.samples,
        "sample_order": list(range(1, args.samples + 1)),
        "command_durations_ms": command_durations,
        "command_p95_ms": percentile_nearest_rank(command_durations, 0.95),
        "host_observations": loads,
        "cases": cases,
    }
    write_json(args.output_dir / "aggregate.json", aggregate)
    print(args.output_dir / "aggregate.json")
    return 0


def validate_aggregate(report: dict[str, Any], *, require_gate_count: bool) -> None:
    if (
        report.get("schema_version") != SCHEMA_VERSION
        or report.get("report_kind") != REPORT_KIND
        or report.get("status") != "ok"
    ):
        raise GateError("aggregate schema/report kind/status mismatch")
    sample_count = report.get("sample_count")
    warmup_count = report.get("warmup_count")
    if not isinstance(sample_count, int) or sample_count <= 0:
        raise GateError("aggregate sample_count must be a positive integer")
    if not isinstance(warmup_count, int) or warmup_count < 0:
        raise GateError("aggregate warmup_count must be a non-negative integer")
    if require_gate_count and (sample_count != 5 or warmup_count != 1):
        raise GateError("required gate expects one warmup and five samples")
    if report.get("sample_order") != list(range(1, sample_count + 1)):
        raise GateError("aggregate sample order mismatch")
    profile = report.get("profile")
    if not isinstance(profile, dict):
        raise GateError("aggregate profile must be an object")
    fixture_cases = validate_fixture(report.get("fixture"))
    validate_provenance(report.get("provenance"))
    if (
        profile.get("dataset") != FIXTURE_ID
        or profile.get("codec") != "aes-gcm"
        or profile.get("compression") not in {"none", "zstd"}
        or profile.get("workers") not in {1, 4}
        or profile.get("pipeline_depth") != 1
        or profile.get("deterministic") is not True
    ):
        raise GateError("aggregate fixture/profile contract mismatch")
    command_durations = report.get("command_durations_ms")
    if not isinstance(command_durations, list) or len(command_durations) != sample_count:
        raise GateError("aggregate command duration count mismatch")
    command_durations = [
        require_number(value, "aggregate command duration", positive=True)
        for value in command_durations
    ]
    expected_p95 = percentile_nearest_rank(command_durations, 0.95)
    actual_p95 = require_number(report.get("command_p95_ms"), "aggregate command p95", positive=True)
    if not math.isclose(actual_p95, expected_p95, rel_tol=1e-12, abs_tol=1e-9):
        raise GateError("aggregate command p95 is inconsistent")
    if not isinstance(report.get("host_observations"), list):
        raise GateError("aggregate host_observations must be an array")

    cases = report.get("cases")
    if not isinstance(cases, list) or not cases:
        raise GateError("aggregate cases must be non-empty")
    names = [case.get("case") for case in cases if isinstance(case, dict)]
    if names != EXPECTED_CASES or len(set(names)) != len(names):
        raise GateError("aggregate case set/order mismatch")
    expected_execution = {
        "store_folder_workers": profile["workers"],
        "pipeline_depth": 1,
        "deterministic": True,
    }
    for index, case in enumerate(cases):
        if case.get("seed") != fixture_cases[index]["seed"]:
            raise GateError(f"{case.get('case')} seed mismatch")
        logical_files = case.get("logical_files")
        if isinstance(logical_files, bool) or not isinstance(logical_files, int) or logical_files <= 0:
            raise GateError(f"{case.get('case')} logical_files must be a positive integer")
        durations = case.get("sample_durations_ms")
        if not isinstance(durations, list) or len(durations) != sample_count:
            raise GateError(f"{case.get('case')} sample count mismatch")
        expected = summarize(durations)
        for field, value in expected.items():
            actual = require_number(case.get(field), f"{case.get('case')} {field}")
            if not math.isclose(actual, value, rel_tol=1e-12, abs_tol=1e-9):
                raise GateError(f"{case.get('case')} statistic {field} is inconsistent")
        logical_bytes = require_number(
            case.get("logical_bytes"), f"{case.get('case')} logical_bytes", positive=True
        )
        validate_diagnostic_final_state(
            case.get("diagnostic_final_state"), f"aggregate case {case.get('case')!r}"
        )
        stats = case.get("fixture_stats")
        if not isinstance(stats, dict) or stats.get("execution") != expected_execution:
            raise GateError(f"{case.get('case')} fixture execution mismatch")
        execution_stats = stats.get("execution_stats")
        if not isinstance(execution_stats, dict):
            raise GateError(f"{case.get('case')} fixture counters must be an object")
        if (
            execution_stats.get("total_files") != logical_files
            or execution_stats.get("total_bytes") != case.get("logical_bytes")
        ):
            raise GateError(f"{case.get('case')} logical totals do not match fixture counters")
        io_stats = execution_stats.get("io")
        if not isinstance(io_stats, dict):
            raise GateError(f"{case.get('case')} fixture I/O counters must be an object")
        for field in ("container_opens", "container_appends", "fsyncs", "bytes_written", "bytes_read"):
            require_number(io_stats.get(field), f"{case.get('case')} fixture I/O counter {field}")
        expected_throughput = (
            logical_bytes
            / (1024.0 * 1024.0)
            / (expected["median_duration_ms"] / 1000.0)
        )
        actual_throughput = require_number(
            case.get("throughput_mbps"),
            f"{case.get('case')} throughput",
            positive=True,
        )
        if not math.isclose(actual_throughput, expected_throughput, rel_tol=1e-12, abs_tol=1e-12):
            raise GateError(f"{case.get('case')} aggregate throughput is inconsistent")


def threshold_policy(path: pathlib.Path, mode: str) -> tuple[Decimal, dict[str, Decimal], bool]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if mode == "uncompressed":
        default_key = "duration_regression_pct"
        hard_fail = True
    else:
        default_key = "duration_regression_warning_pct"
        hard_fail = False

    default: Decimal | None = None
    overrides: dict[str, Decimal] = {}
    section = ""
    current_mode = ""
    current_case = ""
    in_defaults = False
    in_overrides = False
    for raw in lines:
        text = raw.split("#", 1)[0].rstrip()
        if not text.strip():
            continue
        indent = len(text) - len(text.lstrip())
        stripped = text.strip()
        if indent == 0 and stripped.endswith(":"):
            section = stripped[:-1]
            in_defaults = section == "defaults"
            in_overrides = section == "per_case_overrides"
            current_mode = ""
            current_case = ""
            continue
        if (in_defaults or in_overrides) and indent == 2 and stripped.endswith(":"):
            current_mode = stripped[:-1]
            current_case = ""
            continue
        if in_overrides and current_mode == mode and indent == 4 and stripped.endswith(":"):
            current_case = stripped[:-1]
            continue
        match = re.fullmatch(r"([a-z_]+):\s*([0-9]+(?:\.[0-9]+)?)", stripped)
        if not match or match.group(1) != default_key or current_mode != mode:
            continue
        value = Decimal(match.group(2))
        if in_defaults and indent == 4:
            default = value
        elif in_overrides and indent == 6 and current_case:
            overrides[current_case] = value
    if default is None:
        raise GateError(f"cannot locate {mode} default duration threshold in {path}")
    return default, overrides, hard_fail


def compare_command(args: argparse.Namespace) -> int:
    candidate = load_json_strict(args.candidate)
    baseline = load_json_strict(args.baseline)
    validate_aggregate(candidate, require_gate_count=True)
    validate_aggregate(baseline, require_gate_count=True)
    if args.manifest:
        manifest = validate_manifest(args.manifest)
        baseline_hash = sha256_file(args.baseline)
        profile_key = f"{baseline['profile']['compression']}-w{baseline['profile']['workers']}"
        if manifest["artifacts"][profile_key].get("sha256") != baseline_hash:
            raise GateError("selected baseline is not owned by the authoritative manifest")
        if manifest["thresholds"]["sha256"] != sha256_file(args.thresholds):
            raise GateError("threshold file hash does not match the authoritative manifest")

    if candidate["profile"] != baseline["profile"]:
        raise GateError("candidate and baseline profile metadata differ")
    expected_mode = "uncompressed" if baseline["profile"]["compression"] == "none" else "compressed"
    if args.mode != expected_mode:
        raise GateError("comparison mode does not match compression profile")
    if candidate["fixture"] != baseline["fixture"]:
        raise GateError("candidate and baseline fixture metadata differ")
    for field in HARD_ENV_FIELDS:
        if candidate["provenance"][field] != baseline["provenance"][field]:
            raise GateError(f"environment provenance mismatch for {field}")
    warnings = []
    if candidate["provenance"].get("runner_image") != baseline["provenance"].get("runner_image"):
        warnings.append("resolved runner image differs from baseline")

    default_threshold, overrides, hard_fail_policy = threshold_policy(args.thresholds, args.mode)
    outcomes = []
    has_instability = False
    has_hard_regression = False
    for baseline_case, candidate_case in zip(baseline["cases"], candidate["cases"]):
        case_name = baseline_case["case"]
        if hard_final_state(candidate_case) != hard_final_state(baseline_case):
            raise GateError(f"hard final state differs for {case_name}")
        if candidate_case["fixture_stats"] != baseline_case["fixture_stats"]:
            raise GateError(f"fixture counters differ for {case_name}")
        threshold = overrides.get(case_name, default_threshold)
        variability_limit = min(Decimal("2"), threshold / Decimal("2"))
        base_median = Decimal(str(baseline_case["median_duration_ms"]))
        candidate_median = Decimal(str(candidate_case["median_duration_ms"]))
        baseline_variability = Decimal(str(baseline_case["mad_ratio_pct"]))
        candidate_variability = Decimal(str(candidate_case["mad_ratio_pct"]))
        if base_median < Decimal("5000"):
            raise GateError(f"baseline case {case_name} is shorter than 5000 ms")
        classification = "pass"
        if baseline_variability > variability_limit or candidate_variability > variability_limit:
            classification = "BENCHMARK_UNSTABLE"
            has_instability = True
        delta = (candidate_median - base_median) * Decimal("100") / base_median
        if classification == "pass" and delta > threshold:
            classification = "PERFORMANCE_REGRESSION" if hard_fail_policy else "PERFORMANCE_WARNING"
            has_hard_regression = has_hard_regression or hard_fail_policy
        outcomes.append(
            {
                "case": case_name,
                "classification": classification,
                "baseline_median_ms": float(base_median),
                "candidate_median_ms": float(candidate_median),
                "delta_pct": float(delta),
                "threshold_pct": float(threshold),
                "variability_limit_pct": float(variability_limit),
            }
        )

    passed = not has_instability and not has_hard_regression
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_kind": "benchmark_gate_comparison",
        "status": "ok" if passed else "failed",
        "manifest_sha256": sha256_file(args.manifest) if args.manifest else None,
        "warnings": warnings,
        "outcomes": outcomes,
    }
    write_json(args.output, report)
    return 0 if passed else 1


def calibration_command(args: argparse.Namespace) -> int:
    reports: dict[tuple[str, int, int], dict[str, Any]] = {}
    for item in args.aggregate:
        if "=" not in item:
            raise GateError("--aggregate values must use compression-wN-rN=path")
        label, raw_path = item.split("=", 1)
        match = re.fullmatch(r"(none|zstd)-w(1|4)-r(1|2)", label)
        if not match:
            raise GateError(f"invalid calibration label {label!r}")
        key = (match.group(1), int(match.group(2)), int(match.group(3)))
        if key in reports:
            raise GateError(f"duplicate calibration profile {label!r}")
        report = load_json_strict(pathlib.Path(raw_path))
        validate_aggregate(report, require_gate_count=False)
        if report["sample_count"] != 10 or report["warmup_count"] != 1:
            raise GateError(f"calibration profile {label!r} requires one warmup and ten samples")
        expected_profile = {
            "codec": "aes-gcm",
            "compression": key[0],
            "dataset": FIXTURE_ID,
            "workers": key[1],
            "pipeline_depth": 1,
            "deterministic": True,
        }
        if report["profile"] != expected_profile:
            raise GateError(f"calibration profile metadata mismatch for {label!r}")
        reports[key] = report
    expected_keys = {
        (compression, workers, replicate)
        for compression in ("none", "zstd")
        for workers in (1, 4)
        for replicate in (1, 2)
    }
    if set(reports) != expected_keys:
        raise GateError("calibration requires exactly two replicas of all four profiles")

    failures = []
    profile_results = []
    for compression in ("none", "zstd"):
        mode = "uncompressed" if compression == "none" else "compressed"
        default_threshold, overrides, _ = threshold_policy(args.thresholds, mode)
        for workers in (1, 4):
            first = reports[(compression, workers, 1)]
            second = reports[(compression, workers, 2)]
            for field in HARD_ENV_FIELDS:
                if first["provenance"][field] != second["provenance"][field]:
                    failures.append(
                        f"{compression}-w{workers}: replica environment mismatch for {field}"
                    )
            for report in (first, second):
                if report["command_p95_ms"] > 120_000:
                    failures.append(f"{compression}-w{workers}: command p95 exceeds 120 seconds")
            for first_case, second_case in zip(first["cases"], second["cases"]):
                case_name = first_case["case"]
                threshold = overrides.get(case_name, default_threshold)
                variability_limit = float(min(Decimal("2"), threshold / Decimal("2")))
                if hard_final_state(first_case) != hard_final_state(second_case):
                    failures.append(f"{compression}-w{workers}/{case_name}: replica hard final-state mismatch")
                if first_case["fixture_stats"] != second_case["fixture_stats"]:
                    failures.append(f"{compression}-w{workers}/{case_name}: replica fixture mismatch")
                replica_medians = []
                for replicate, case in ((1, first_case), (2, second_case)):
                    median = float(case["median_duration_ms"])
                    replica_medians.append(median)
                    if median < 5000:
                        failures.append(
                            f"{compression}-w{workers}-r{replicate}/{case_name}: median below 5000 ms"
                        )
                    if float(case["mad_ratio_pct"]) > variability_limit:
                        failures.append(
                            f"{compression}-w{workers}-r{replicate}/{case_name}: ten-sample MAD ratio exceeds {variability_limit}%"
                        )
                    durations = case["sample_durations_ms"]
                    for subset_name, subset in (
                        ("odd", durations[0::2]),
                        ("even", durations[1::2]),
                    ):
                        subset_stats = summarize(subset)
                        if subset_stats["mad_ratio_pct"] > variability_limit:
                            failures.append(
                                f"{compression}-w{workers}-r{replicate}/{case_name}: {subset_name} five-sample MAD ratio exceeds {variability_limit}%"
                            )
                replica_delta = (
                    abs(replica_medians[1] - replica_medians[0])
                    / min(replica_medians)
                    * 100.0
                )
                if replica_delta > 5.0:
                    failures.append(
                        f"{compression}-w{workers}/{case_name}: replica medians differ by {replica_delta:.3f}%"
                    )
                profile_results.append(
                    {
                        "profile": f"{compression}-w{workers}",
                        "case": case_name,
                        "replica_medians_ms": replica_medians,
                        "replica_delta_pct": replica_delta,
                        "variability_limit_pct": variability_limit,
                    }
                )
    result = {
        "schema_version": SCHEMA_VERSION,
        "report_kind": "benchmark_gate_calibration",
        "status": "ok" if not failures else "failed",
        "failures": failures,
        "profiles": profile_results,
    }
    write_json(args.output, result)
    return 0 if not failures else 1


def manifest_command(args: argparse.Namespace) -> int:
    repository_root = pathlib.Path.cwd().resolve()

    def repository_relative(path: pathlib.Path) -> str:
        try:
            return path.resolve().relative_to(repository_root).as_posix()
        except ValueError as exc:
            raise GateError(f"manifest path must be repository-relative: {path}") from exc

    artifacts: dict[str, Any] = {}
    source_commits: set[str] = set()
    for item in args.baseline:
        if "=" not in item:
            raise GateError("--baseline values must use profile=path")
        profile, raw_path = item.split("=", 1)
        if profile not in MANIFEST_PROFILES:
            raise GateError(f"invalid manifest profile {profile!r}")
        path = pathlib.Path(raw_path)
        report = load_json_strict(path)
        validate_aggregate(report, require_gate_count=True)
        if profile in artifacts:
            raise GateError(f"duplicate manifest profile {profile!r}")
        expected_compression, expected_workers = MANIFEST_PROFILES[profile]
        if (
            report["profile"]["compression"] != expected_compression
            or report["profile"]["workers"] != expected_workers
        ):
            raise GateError(f"manifest profile {profile!r} does not match aggregate metadata")
        source_commits.add(report["provenance"]["source_commit"])
        artifacts[profile] = {
            "path": repository_relative(path),
            "sha256": sha256_file(path),
            "source_commit": report["provenance"]["source_commit"],
        }
    if set(artifacts) != set(MANIFEST_PROFILES):
        raise GateError("authoritative manifest requires exactly four profiles")
    if len(source_commits) != 1:
        raise GateError("authoritative baselines must share one governed source commit")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "manifest_kind": MANIFEST_KIND,
        "generated_at_utc": utc_now(),
        "thresholds": {
            "path": repository_relative(args.thresholds),
            "sha256": sha256_file(args.thresholds),
        },
        "artifacts": dict(sorted(artifacts.items())),
    }
    write_json(args.output, manifest)
    return 0


def validate_manifest_command(args: argparse.Namespace) -> int:
    validate_manifest(args.manifest)
    print("benchmark gate manifest is valid")
    return 0


def validate_manifest(path: pathlib.Path) -> dict[str, Any]:
    manifest = load_json_strict(path)
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("manifest_kind") != MANIFEST_KIND
    ):
        raise GateError("manifest schema/kind mismatch")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(MANIFEST_PROFILES):
        raise GateError("manifest must contain exactly four artifacts")
    source_commits: set[str] = set()
    entries = {**artifacts, "_thresholds": manifest.get("thresholds")}
    for label, entry in entries.items():
        if not isinstance(entry, dict):
            raise GateError(f"manifest entry {label!r} must be an object")
        path = pathlib.Path(entry.get("path", ""))
        if path.is_absolute() or ".." in path.parts:
            raise GateError(f"manifest path for {label} must be repository-relative")
        if not path.is_file() or sha256_file(path) != entry.get("sha256"):
            raise GateError(f"manifest hash mismatch for {label}")
        if label != "_thresholds":
            report = load_json_strict(path)
            validate_aggregate(report, require_gate_count=True)
            expected_compression, expected_workers = MANIFEST_PROFILES[label]
            if (
                report["profile"]["compression"] != expected_compression
                or report["profile"]["workers"] != expected_workers
                or entry.get("source_commit") != report["provenance"]["source_commit"]
            ):
                raise GateError(f"manifest profile metadata mismatch for {label}")
            source_commits.add(report["provenance"]["source_commit"])
    if len(source_commits) != 1:
        raise GateError("manifest artifacts do not share one governed source commit")
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("sample", help="capture independent benchmark samples")
    sample.add_argument("--binary", type=pathlib.Path, required=True)
    sample.add_argument("--output-dir", type=pathlib.Path, required=True)
    sample.add_argument("--compression", choices=("none", "zstd"), required=True)
    sample.add_argument("--workers", type=int, choices=(1, 4), required=True)
    sample.add_argument("--dataset", default=FIXTURE_ID)
    sample.add_argument("--warmups", type=int, default=1)
    sample.add_argument("--samples", type=int, default=5)
    sample.add_argument("--minimum-free-disk-bytes", type=int, default=10 * 1024**3)
    sample.add_argument("--source-commit")
    sample.add_argument("--source-tag")
    sample.add_argument("--go-version")
    sample.add_argument("--postgres-version", required=True)
    sample.add_argument("--database-image-digest", required=True)
    sample.set_defaults(handler=sample_command)

    compare = subparsers.add_parser("compare", help="compare aggregate evidence")
    compare.add_argument("--candidate", type=pathlib.Path, required=True)
    compare.add_argument("--baseline", type=pathlib.Path, required=True)
    compare.add_argument("--thresholds", type=pathlib.Path, required=True)
    compare.add_argument("--mode", choices=("uncompressed", "compressed"), required=True)
    compare.add_argument("--manifest", type=pathlib.Path)
    compare.add_argument("--output", type=pathlib.Path, required=True)
    compare.set_defaults(handler=compare_command)

    calibrate = subparsers.add_parser("calibrate", help="evaluate fixed calibration evidence")
    calibrate.add_argument("--aggregate", action="append", default=[], required=True)
    calibrate.add_argument("--thresholds", type=pathlib.Path, required=True)
    calibrate.add_argument("--output", type=pathlib.Path, required=True)
    calibrate.set_defaults(handler=calibration_command)

    manifest = subparsers.add_parser("manifest", help="generate an authoritative manifest")
    manifest.add_argument("--baseline", action="append", default=[], required=True)
    manifest.add_argument("--thresholds", type=pathlib.Path, required=True)
    manifest.add_argument("--output", type=pathlib.Path, required=True)
    manifest.set_defaults(handler=manifest_command)

    validate_manifest = subparsers.add_parser(
        "validate-manifest", help="validate manifest hashes and aggregates"
    )
    validate_manifest.add_argument("--manifest", type=pathlib.Path, required=True)
    validate_manifest.set_defaults(handler=validate_manifest_command)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return args.handler(args)
    except (GateError, OSError, subprocess.CalledProcessError) as exc:
        print(f"benchmark gate error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
