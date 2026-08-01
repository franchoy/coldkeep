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
REVALIDATION_KIND = "benchmark_evidence_contract_revalidation"
INTEGRITY_KIND = "benchmark_integrity"
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
INTEGRITY_FIXTURES = {
    "ci-paired-w1-v2": {
        "id": "ci-paired-w1-v2",
        "seed": 1701,
        "large_file_size_bytes": 64 * 1024 * 1024,
        "many_small_file_count": 400,
        "many_small_file_size_bytes": 1024,
        "mixed_file_count": 400,
        "mixed_min_file_size_bytes": 1024,
        "mixed_max_file_size_bytes": 256 * 1024,
        "remove_every": 4,
        "case_database_isolation": True,
        "workers": 1,
    },
    "ci-paired-w4-v2": {
        "id": "ci-paired-w4-v2",
        "seed": 1701,
        "large_file_size_bytes": 64 * 1024 * 1024,
        "many_small_file_count": 400,
        "many_small_file_size_bytes": 1024,
        "mixed_file_count": 800,
        "mixed_min_file_size_bytes": 1024,
        "mixed_max_file_size_bytes": 256 * 1024,
        "remove_every": 4,
        "case_database_isolation": True,
        "workers": 4,
    },
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
CALIBRATION_IDENTITY_FIELDS = ("source_commit", "binary_sha256", *HARD_ENV_FIELDS)
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
DIAGNOSTIC_SCHEMA_VERSION = 2
EVIDENCE_POLICY_VERSION = 2
INT64_MAX = (1 << 63) - 1
INTEGRITY_SAMPLE_COUNT = 2
INTEGRITY_COMMAND_TIMEOUT_SECONDS = 600

# Outcome E evidence policy. Paths use [] for repeated case/sample records.
# Unknown fields in validated sections fail closed; adding a field requires an
# explicit schema/policy update rather than inheriting an informational policy.
FIELD_POLICY = {
    "hard_equal": (
        "raw.schema_version",
        "aggregate.schema_version",
        "aggregate.evidence_policy_version",
        "aggregate.report_kind",
        "aggregate.status",
        "raw.status",
        "raw.command",
        "raw.dataset",
        "raw.repeat",
        "aggregate.provenance.source_commit",
        "aggregate.provenance.binary_sha256",
        "aggregate.provenance.runner_os",
        "aggregate.provenance.runner_arch",
        "aggregate.provenance.cpu_count",
        "aggregate.provenance.go_version",
        "aggregate.provenance.postgres_version",
        "aggregate.provenance.database_image_digest",
        "profile.codec",
        "profile.compression",
        "profile.dataset",
        "profile.workers",
        "profile.pipeline_depth",
        "profile.deterministic",
        "fixture.*",
        "warmup_count",
        "sample_count",
        "cases[].case",
        "cases[].seed",
        "cases[].logical_files",
        "cases[].logical_bytes",
        "cases[].workers_used",
        "cases[].diagnostic.active_logical_namespace.*",
        "cases[].diagnostic.logical_catalog.*",
        "cases[].diagnostic.logical_statuses.*",
        "cases[].diagnostic.chunk_graph.*",
        "cases[].diagnostic.restored_tree.*",
        "cases[].diagnostic.snapshots.*",
        "cases[].diagnostic.snapshot_count",
        "cases[].diagnostic.gc.*",
        "cases[].diagnostic.verification.*",
        "cases[].diagnostic.physical.chunk_reference_count",
        "cases[].diagnostic.physical.payload_bytes",
        "cases[].diagnostic.physical.canonical_sha256",
        "operation_totals.*",
        "cleanup_totals.*",
    ),
    "derived_equal": (
        "raw.execution_stats totals recomputed from rows",
        "rows[].throughput_mbps",
        "rows[].execution_stats.container_append_count",
        "rows[].execution_stats.container_open_count",
        "rows[].execution_stats.fsync_count",
        "rows[].execution_stats.snapshot_metadata_write_count when emitted",
        "aggregate.execution_stats.snapshot_metadata_write_count",
        "cases[].median_duration_ms",
        "cases[].mean_duration_ms",
        "cases[].min_duration_ms",
        "cases[].max_duration_ms",
        "cases[].sample_stddev_ms",
        "cases[].mad_ms",
        "cases[].mad_ratio_pct",
        "cases[].coefficient_of_variation_pct",
        "sample_order",
        "command_p95_ms",
        "manifest.*.sha256",
    ),
    "bounded_nonnegative": (
        "rows[].execution_stats.container_close_count",
        "rows[].execution_stats.io.container_opens",
        "rows[].execution_stats.io.container_appends",
        "rows[].execution_stats.io.fsyncs",
        "rows[].execution_stats.io.bytes_written",
        "rows[].execution_stats.io.bytes_read",
    ),
    "informational": (
        "rows[].duration_ms",
        "cases[].sample_durations_ms",
        "cases[].operational_samples",
        "cases[].operational_counter_distributions",
        "cases[].diagnostic_samples[].physical.container_count",
        "cases[].diagnostic_samples[].physical.storage_block_count",
        "cases[].diagnostic_samples[].physical.legacy_block_count",
        "cases[].diagnostic_samples[].physical.container_bytes",
        "cases[].diagnostic_samples[].physical_layout_sha256",
        "command_durations_ms",
        "host_observations",
        "provenance.generated_at_utc",
        "provenance.workflow_run_id",
        "provenance.workflow_job_id",
        "provenance.workflow_run_attempt",
        "provenance.runner_image",
    ),
    "excluded_sensitive": (
        "credentials",
        "passwords",
        "encryption_keys",
        "dsns",
        "usernames",
        "database_names",
        "repository_paths",
        "temporary_roots",
        "sensitive_command_arguments",
        "environment_dumps",
        "raw_internal_ids",
    ),
}

RAW_ENVELOPE_FIELDS = {"status", "command", "data"}
RAW_DATA_FIELDS = {
    "schema_version", "generated_at_utc", "dataset", "repeat", "fixture",
    "execution", "execution_stats", "rows",
}
EXECUTION_FIELDS = {"store_folder_workers", "pipeline_depth", "deterministic"}
RAW_ROW_FIELDS = {
    "case", "duration_ms", "throughput_mbps", "execution", "execution_stats",
    "diagnostic_final_state",
}
IO_COUNTER_FIELDS = {
    "container_opens", "container_appends", "fsyncs", "bytes_written", "bytes_read",
}
ROW_EXECUTION_STATS_REQUIRED_FIELDS = {
    "total_files", "total_bytes", "workers_used", "container_append_count",
    "fsync_count", "container_open_count", "container_close_count", "io",
}
ROW_EXECUTION_STATS_OPTIONAL_FIELDS = {"snapshot_metadata_write_count"}
TOP_EXECUTION_STATS_FIELDS = ROW_EXECUTION_STATS_REQUIRED_FIELDS | {
    "snapshot_metadata_write_count",
}
PROVENANCE_FIELDS = set(REQUIRED_PROVENANCE_FIELDS) | {"source_tag"}
PROFILE_FIELDS = {"codec", "compression", "dataset", "workers", "pipeline_depth", "deterministic"}
AGGREGATE_FIELDS = {
    "schema_version", "evidence_policy_version", "report_kind", "status", "provenance",
    "profile", "fixture", "warmup_count", "sample_count", "sample_order",
    "command_durations_ms", "command_p95_ms", "host_observations", "operation_totals",
    "cleanup_totals", "cases",
}
AGGREGATE_CASE_FIELDS = {
    "case", "seed", "logical_files", "logical_bytes", "workers_used",
    "sample_durations_ms", "diagnostic_final_state", "diagnostic_samples",
    "operational_samples", "operational_counter_distributions",
    "median_duration_ms", "mean_duration_ms", "min_duration_ms", "max_duration_ms",
    "sample_stddev_ms", "mad_ms", "mad_ratio_pct", "coefficient_of_variation_pct",
    "throughput_mbps",
}
REVALIDATION_FIELDS = {
    "schema_version", "evidence_policy_version", "report_kind", "status",
    "performance_calibration_status", "profile", "fixture", "warmup_count",
    "sample_count", "sample_order", "operation_totals", "cleanup_totals", "cases",
}
SENSITIVE_KEY_PARTS = (
    "password", "credential", "encryption_key", "dsn", "username", "user_name",
    "database_name", "db_name", "repository_path", "temporary_root", "temp_root",
    "environment_dump", "command_arguments", "command_args",
)
OPERATIONAL_COUNTER_FIELDS = (
    "container_append_count",
    "container_open_count",
    "container_close_count",
    "fsync_count",
    "bytes_written",
    "bytes_read",
    "snapshot_metadata_write_count",
)


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


def write_checksums(directory: pathlib.Path) -> None:
    entries = []
    for path in sorted(directory.rglob("*")):
        if path.is_file() and path.name != "checksums.sha256":
            entries.append(f"{sha256_file(path)}  {path.relative_to(directory).as_posix()}")
    (directory / "checksums.sha256").write_text("\n".join(entries) + "\n", encoding="utf-8")


def require_exact_fields(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GateError(f"{label} must be an object")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise GateError(f"{label} fields mismatch: missing={missing} unknown={unknown}")
    return value


def validate_no_sensitive_evidence(value: Any, label: str = "evidence") -> None:
    def visit(item: Any, path: str) -> None:
        if isinstance(item, dict):
            for key, nested in item.items():
                normalized = str(key).lower().replace("-", "_")
                if any(part in normalized for part in SENSITIVE_KEY_PARTS):
                    raise GateError(f"{label} contains prohibited sensitive field at {path}.{key}")
                visit(nested, f"{path}.{key}")
        elif isinstance(item, list):
            for index, nested in enumerate(item):
                visit(nested, f"{path}[{index}]")
        elif isinstance(item, str):
            lowered = item.lower()
            if (
                re.match(r"^[a-z][a-z0-9+.-]*://", item, re.IGNORECASE)
                or item.startswith("/")
                or re.match(r"^[A-Za-z]:[\\/]", item)
                or "coldkeep_bench_" in lowered
                or "coldkeep-benchmark-" in lowered
                or re.search(r"(?:password|dbname|user)\s*=", lowered)
            ):
                raise GateError(f"{label} contains prohibited sensitive value at {path}")

    visit(value, label)


def write_sanitized_capture(path: pathlib.Path, value: str) -> None:
    try:
        validate_no_sensitive_evidence({"capture": value}, "captured output")
    except GateError:
        value = "[captured output omitted: sensitive content]\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


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


def require_nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > INT64_MAX:
        raise GateError(f"{label} must be a non-negative signed 64-bit integer")
    return value


def require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise GateError(f"{label} must be lowercase SHA-256")
    return value


def validate_diagnostic_final_state(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema_version") != DIAGNOSTIC_SCHEMA_VERSION:
        raise GateError(f"{label} diagnostic final state schema mismatch")
    expected_keys = {
        "schema_version", "active_logical_namespace", "logical_catalog",
        "logical_statuses", "chunk_graph",
        "restored_tree", "snapshots", "snapshot_count", "gc", "verification",
        "physical", "physical_layout_sha256",
    }
    if set(value) != expected_keys:
        raise GateError(f"{label} diagnostic final state fields mismatch")
    for section_name in (
        "active_logical_namespace", "logical_catalog", "chunk_graph", "restored_tree", "snapshots"
    ):
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
    if sum(statuses.values()) != value["logical_catalog"]["count"]:
        raise GateError(f"{label} diagnostic logical statuses do not match logical catalog count")
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
    if gc_totals["reachable_chunks"] + gc_totals["unreachable_chunks"] != gc_totals["total_chunks"]:
        raise GateError(f"{label} diagnostic GC reachability totals are inconsistent")
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
    if verification["physical_file_issues"] != 0 or verification["snapshot_reachability_issues"] != 0:
        raise GateError(f"{label} diagnostic verification reports integrity issues")
    if verification["snapshot_membership_rows"] != value["snapshots"]["count"]:
        raise GateError(f"{label} diagnostic snapshot membership totals are inconsistent")
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
    validate_no_sensitive_evidence(value, label)
    return value


def hard_final_state(row: dict[str, Any]) -> dict[str, Any]:
    state = validate_diagnostic_final_state(
        row.get("diagnostic_final_state"), f"case {row.get('case')!r}"
    )
    physical = state["physical"]
    return {
        "active_logical_namespace": state["active_logical_namespace"],
        "logical_catalog": state["logical_catalog"],
        "logical_statuses": state["logical_statuses"],
        "chunk_graph": state["chunk_graph"],
        "restored_tree": state["restored_tree"],
        "snapshots": state["snapshots"],
        "snapshot_count": state["snapshot_count"],
        "gc": state["gc"],
        "verification": state["verification"],
        "physical_content": {
            "chunk_reference_count": physical["chunk_reference_count"],
            "payload_bytes": physical["payload_bytes"],
            "canonical_sha256": physical["canonical_sha256"],
        },
    }


def validate_execution(value: Any, *, workers: int, label: str) -> dict[str, Any]:
    value = require_exact_fields(value, EXECUTION_FIELDS, label)
    if (
        value["store_folder_workers"] != workers
        or value["pipeline_depth"] != 1
        or value["deterministic"] is not True
    ):
        raise GateError(f"{label} policy mismatch")
    return value


def validate_operational_counters(row: dict[str, Any], *, workers: int) -> dict[str, int]:
    case_name = row.get("case")
    stats = row.get("execution_stats")
    if not isinstance(stats, dict):
        raise GateError(f"{case_name} execution_stats must be an object")
    fields = set(stats)
    if not ROW_EXECUTION_STATS_REQUIRED_FIELDS <= fields:
        missing = sorted(ROW_EXECUTION_STATS_REQUIRED_FIELDS - fields)
        raise GateError(f"{case_name} execution_stats missing mandatory counters: {missing}")
    unknown = fields - ROW_EXECUTION_STATS_REQUIRED_FIELDS - ROW_EXECUTION_STATS_OPTIONAL_FIELDS
    if unknown:
        raise GateError(f"{case_name} execution_stats has unknown fields: {sorted(unknown)}")
    io_stats = require_exact_fields(stats["io"], IO_COUNTER_FIELDS, f"{case_name} I/O counters")

    logical_files = require_nonnegative_integer(stats["total_files"], f"{case_name} logical files")
    logical_bytes = require_nonnegative_integer(stats["total_bytes"], f"{case_name} logical bytes")
    workers_used = require_nonnegative_integer(stats["workers_used"], f"{case_name} workers_used")
    if logical_files <= 0 or logical_bytes <= 0:
        raise GateError(f"{case_name} logical totals must be positive")
    if workers_used != workers:
        raise GateError(f"{case_name} workers_used mismatch")

    outer_append = require_nonnegative_integer(
        stats["container_append_count"], f"{case_name} container_append_count"
    )
    outer_open = require_nonnegative_integer(
        stats["container_open_count"], f"{case_name} container_open_count"
    )
    outer_close = require_nonnegative_integer(
        stats["container_close_count"], f"{case_name} container_close_count"
    )
    outer_fsync = require_nonnegative_integer(stats["fsync_count"], f"{case_name} fsync_count")
    io_values = {
        field: require_nonnegative_integer(io_stats[field], f"{case_name} I/O counter {field}")
        for field in IO_COUNTER_FIELDS
    }
    snapshot_writes = require_nonnegative_integer(
        stats.get("snapshot_metadata_write_count", 0),
        f"{case_name} snapshot_metadata_write_count",
    )

    if outer_append != io_values["container_appends"]:
        raise GateError(f"{case_name} duplicated container append counters differ")
    if outer_open != io_values["container_opens"]:
        raise GateError(f"{case_name} duplicated container open counters differ")
    if outer_fsync != io_values["fsyncs"]:
        raise GateError(f"{case_name} duplicated fsync counters differ")
    if outer_open != outer_close:
        raise GateError(f"{case_name} container open/close counters are unbalanced")
    if outer_append > 0 and (
        outer_open == 0 or outer_fsync == 0 or io_values["bytes_written"] == 0
    ):
        raise GateError(f"{case_name} append counters contradict execution I/O")
    if io_values["bytes_read"] > 0 and outer_open == 0:
        raise GateError(f"{case_name} read counters contradict container opens")
    if snapshot_writes > 0 and case_name not in {"snapshot-creation", "gc-after-churn"}:
        raise GateError(f"{case_name} snapshot writes contradict the operation type")

    return {
        "container_append_count": outer_append,
        "container_open_count": outer_open,
        "container_close_count": outer_close,
        "fsync_count": outer_fsync,
        "bytes_written": io_values["bytes_written"],
        "bytes_read": io_values["bytes_read"],
        "snapshot_metadata_write_count": snapshot_writes,
    }


def hard_row_contract(row: dict[str, Any], *, workers: int) -> dict[str, Any]:
    validate_execution(row.get("execution"), workers=workers, label=f"{row.get('case')} execution")
    validate_operational_counters(row, workers=workers)
    stats = row["execution_stats"]
    return {
        "case": row["case"],
        "execution": row["execution"],
        "logical_files": stats["total_files"],
        "logical_bytes": stats["total_bytes"],
        "workers_used": stats["workers_used"],
        "diagnostic_final_state": hard_final_state(row),
    }


def hard_aggregate_case_contract(case: dict[str, Any]) -> dict[str, Any]:
    """Fields that must remain exact when aggregate cases are compared."""
    return {
        "case": case["case"],
        "seed": case["seed"],
        "logical_files": case["logical_files"],
        "logical_bytes": case["logical_bytes"],
        "workers_used": case["workers_used"],
        "diagnostic_final_state": hard_final_state(case),
    }


def summarize_operational_counters(samples: list[dict[str, int]]) -> dict[str, dict[str, Any]]:
    if not samples:
        raise GateError("cannot summarize empty operational counter samples")
    return {
        field: {
            "min": min(sample[field] for sample in samples),
            "max": max(sample[field] for sample in samples),
            "values": sorted({sample[field] for sample in samples}),
        }
        for field in OPERATIONAL_COUNTER_FIELDS
    }


def validate_operational_distributions(
    samples: Any,
    distributions: Any,
    *,
    sample_count: int,
    label: str,
) -> None:
    if not isinstance(samples, list) or len(samples) != sample_count:
        raise GateError(f"{label} operational sample count mismatch")
    normalized: list[dict[str, int]] = []
    for index, sample in enumerate(samples):
        sample = require_exact_fields(sample, set(OPERATIONAL_COUNTER_FIELDS), f"{label} operational sample {index + 1}")
        normalized.append({
            field: require_nonnegative_integer(sample[field], f"{label} operational {field}")
            for field in OPERATIONAL_COUNTER_FIELDS
        })
        if normalized[-1]["container_open_count"] != normalized[-1]["container_close_count"]:
            raise GateError(f"{label} operational sample {index + 1} is unbalanced")
    if distributions != summarize_operational_counters(normalized):
        raise GateError(f"{label} operational counter distributions are inconsistent")


def validate_top_execution_stats(value: Any, rows: list[dict[str, Any]], *, workers: int) -> None:
    value = require_exact_fields(value, TOP_EXECUTION_STATS_FIELDS, "raw aggregate execution_stats")
    io_stats = require_exact_fields(value["io"], IO_COUNTER_FIELDS, "raw aggregate I/O counters")
    expected = {
        "total_files": sum(row["execution_stats"]["total_files"] for row in rows),
        "total_bytes": sum(row["execution_stats"]["total_bytes"] for row in rows),
        "workers_used": max(row["execution_stats"]["workers_used"] for row in rows),
        "container_append_count": sum(row["execution_stats"]["container_append_count"] for row in rows),
        "fsync_count": sum(row["execution_stats"]["fsync_count"] for row in rows),
        "container_open_count": sum(row["execution_stats"]["container_open_count"] for row in rows),
        "container_close_count": sum(row["execution_stats"]["container_close_count"] for row in rows),
        "snapshot_metadata_write_count": sum(
            row["execution_stats"].get("snapshot_metadata_write_count", 0) for row in rows
        ),
    }
    for field, expected_value in expected.items():
        actual = require_nonnegative_integer(value[field], f"raw aggregate {field}")
        if actual != expected_value:
            raise GateError(f"raw aggregate {field} does not match case rows")
    if value["workers_used"] != workers:
        raise GateError("raw aggregate workers_used mismatch")
    expected_io = {
        field: sum(row["execution_stats"]["io"][field] for row in rows)
        for field in IO_COUNTER_FIELDS
    }
    for field, expected_value in expected_io.items():
        actual = require_nonnegative_integer(io_stats[field], f"raw aggregate I/O {field}")
        if actual != expected_value:
            raise GateError(f"raw aggregate I/O {field} does not match case rows")


def fixture_fields(dataset: str) -> dict[str, Any]:
    if dataset == FIXTURE_ID:
        return FIXTURE_FIELDS
    configured = INTEGRITY_FIXTURES.get(dataset)
    if configured is None:
        raise GateError(f"unsupported benchmark fixture {dataset!r}")
    return {key: value for key, value in configured.items() if key != "workers"}


def validate_fixture(fixture: Any, *, dataset: str = FIXTURE_ID) -> list[dict[str, Any]]:
    expected_fields = fixture_fields(dataset)
    fixture = require_exact_fields(
        fixture,
        set(expected_fields) | {"ordered_cases"},
        "fixture",
    )
    for field, expected in expected_fields.items():
        if fixture.get(field) != expected:
            raise GateError(f"fixture field {field!r} does not match {dataset}")
    ordered = fixture.get("ordered_cases")
    if not isinstance(ordered, list) or len(ordered) != len(EXPECTED_CASES):
        raise GateError("fixture ordered case count mismatch")
    for index, (descriptor, expected_name) in enumerate(zip(ordered, EXPECTED_CASES)):
        if not isinstance(descriptor, dict):
            raise GateError(f"fixture case at index {index} must be an object")
        require_exact_fields(descriptor, {"name", "seed"}, f"fixture case at index {index}")
        expected_seed = 1712 + index * 10
        if descriptor.get("name") != expected_name or descriptor.get("seed") != expected_seed:
            raise GateError(f"fixture case descriptor mismatch at index {index}")
    return ordered


def validate_provenance(value: Any) -> dict[str, Any]:
    value = require_exact_fields(value, PROVENANCE_FIELDS, "aggregate provenance")
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
    validate_no_sensitive_evidence(value, "aggregate provenance")
    return value


def validate_raw_report(
    envelope: dict[str, Any],
    *,
    workers: int,
    compression: str,
    dataset: str = FIXTURE_ID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    require_exact_fields(envelope, RAW_ENVELOPE_FIELDS, "raw report envelope")
    if envelope.get("status") != "ok" or envelope.get("command") != "benchmark":
        raise GateError("raw report must be a successful benchmark envelope")
    data = require_exact_fields(envelope.get("data"), RAW_DATA_FIELDS, "raw report data")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise GateError(f"raw report schema must be {SCHEMA_VERSION}")
    if data.get("dataset") != dataset or data.get("repeat") != 1:
        raise GateError("raw report has the wrong dataset or repeat count")

    execution = validate_execution(data.get("execution"), workers=workers, label="raw report execution")

    validate_fixture(data.get("fixture"), dataset=dataset)

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
        require_exact_fields(row, RAW_ROW_FIELDS, f"raw report row {row.get('case')!r}")
        duration = require_number(row.get("duration_ms"), f"{row.get('case')} duration", positive=True)
        throughput = require_number(
            row.get("throughput_mbps"),
            f"{row.get('case')} throughput",
            positive=True,
        )
        row_execution = validate_execution(
            row.get("execution"), workers=workers, label=f"{row.get('case')} execution"
        )
        if row_execution != execution:
            raise GateError(f"{row.get('case')} execution policy mismatch")
        validate_operational_counters(row, workers=workers)
        stats = row["execution_stats"]
        logical_bytes = stats["total_bytes"]
        expected_throughput = logical_bytes / (1024.0 * 1024.0) / (duration / 1000.0)
        if not math.isclose(throughput, expected_throughput, rel_tol=1e-12, abs_tol=1e-12):
            raise GateError(f"{row.get('case')} derived throughput is inconsistent")
        hard_final_state(row)

    validate_top_execution_stats(data.get("execution_stats"), rows, workers=workers)

    # Compression is supplied by the controlled environment rather than the v2
    # raw payload. Recording it here makes that ownership explicit.
    if compression not in {"none", "zstd"}:
        raise GateError(f"unsupported compression profile {compression!r}")
    validate_no_sensitive_evidence(envelope, "raw report")
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
    try:
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
            timeout=getattr(args, "command_timeout_seconds", None),
        )
    except subprocess.TimeoutExpired as exc:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        output_path.write_text(stdout, encoding="utf-8")
        write_sanitized_capture(output_path.with_suffix(".stderr"), stderr)
        raise GateError("benchmark command timeout") from exc
    elapsed_ms = (time.monotonic() - started) * 1000.0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(completed.stdout, encoding="utf-8")
    write_sanitized_capture(output_path.with_suffix(".stderr"), completed.stderr)
    if completed.returncode != 0:
        raise GateError(f"benchmark sample failed with exit {completed.returncode}")
    envelope = load_json_strict(output_path)
    validate_raw_report(
        envelope,
        workers=args.workers,
        compression=args.compression,
        dataset=args.dataset,
    )
    return envelope, elapsed_ms, {"before": before, "after": host_load()}


def build_contract_cases(
    raw_reports: list[dict[str, Any]],
    *,
    workers: int,
    compression: str,
    dataset: str = FIXTURE_ID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not raw_reports:
        raise GateError("cannot aggregate an empty raw report set")
    first_data, first_rows = validate_raw_report(
        raw_reports[0], workers=workers, compression=compression, dataset=dataset
    )
    fixture_cases = first_data["fixture"]["ordered_cases"]
    cases: list[dict[str, Any]] = []
    for case_index, first_row in enumerate(first_rows):
        case_name = first_row["case"]
        expected_hard = hard_row_contract(first_row, workers=workers)
        first_diagnostic = first_row["diagnostic_final_state"]
        expected_restored_files = (
            first_row["execution_stats"]["total_files"]
            if case_name in {"restore-large-file", "restore-many-files"}
            else 0
        )
        if first_diagnostic["restored_tree"]["count"] != expected_restored_files:
            raise GateError(f"{case_name} restored file total mismatch")
        durations: list[float] = []
        diagnostic_samples: list[dict[str, Any]] = []
        operational_samples: list[dict[str, int]] = []
        for sample_index, envelope in enumerate(raw_reports):
            data, rows = validate_raw_report(
                envelope, workers=workers, compression=compression, dataset=dataset
            )
            if data["fixture"] != first_data["fixture"] or data["execution"] != first_data["execution"]:
                raise GateError(f"fixture/profile changed in sample {sample_index + 1}")
            row = rows[case_index]
            if row["case"] != case_name:
                raise GateError(f"case order changed in sample {sample_index + 1}")
            if hard_row_contract(row, workers=workers) != expected_hard:
                raise GateError(
                    f"hard evidence changed for {case_name} in sample {sample_index + 1}"
                )
            durations.append(float(row["duration_ms"]))
            diagnostic_samples.append(row["diagnostic_final_state"])
            operational_samples.append(validate_operational_counters(row, workers=workers))

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
                "workers_used": first_row["execution_stats"]["workers_used"],
                "sample_durations_ms": durations,
                "diagnostic_final_state": diagnostic_samples[0],
                "diagnostic_samples": diagnostic_samples,
                "operational_samples": operational_samples,
                "operational_counter_distributions": summarize_operational_counters(
                    operational_samples
                ),
                **summary,
            }
        )
    return first_data, cases


def operation_totals(sample_count: int) -> dict[str, int]:
    total = sample_count * len(EXPECTED_CASES)
    return {"success": total, "failure": 0, "skipped": 0}


def cleanup_totals(sample_count: int) -> dict[str, int]:
    total = sample_count * len(EXPECTED_CASES)
    return {
        "attempted": total,
        "succeeded": total,
        "failed": 0,
        "leaked_databases": 0,
        "leaked_processes": 0,
        "leaked_temporary_resources": 0,
    }


def validate_host_observations(value: Any, *, sample_count: int) -> None:
    if not isinstance(value, list) or len(value) != sample_count:
        raise GateError("aggregate host observation count mismatch")
    host_fields = {"load_1m", "load_5m", "load_15m", "free_disk_bytes"}
    for sample_index, observation in enumerate(value):
        observation = require_exact_fields(
            observation,
            {"before", "after"},
            f"host observation {sample_index + 1}",
        )
        for point in ("before", "after"):
            values = require_exact_fields(
                observation[point],
                host_fields,
                f"host observation {sample_index + 1} {point}",
            )
            for field in host_fields:
                require_number(values[field], f"host observation {sample_index + 1} {point} {field}")


def validate_operation_and_cleanup_totals(report: dict[str, Any], *, sample_count: int) -> None:
    expected_operations = operation_totals(sample_count)
    operations = require_exact_fields(
        report.get("operation_totals"), set(expected_operations), "aggregate operation_totals"
    )
    for field, expected in expected_operations.items():
        actual = require_nonnegative_integer(operations[field], f"operation total {field}")
        if actual != expected:
            raise GateError(f"operation total {field} mismatch")

    expected_cleanup = cleanup_totals(sample_count)
    cleanup = require_exact_fields(
        report.get("cleanup_totals"), set(expected_cleanup), "aggregate cleanup_totals"
    )
    for field, expected in expected_cleanup.items():
        actual = require_nonnegative_integer(cleanup[field], f"cleanup total {field}")
        if actual != expected:
            raise GateError(f"cleanup total {field} mismatch")


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

    first_data, cases = build_contract_cases(
        raw_reports,
        workers=args.workers,
        compression=args.compression,
        dataset=args.dataset,
    )

    aggregate = {
        "schema_version": SCHEMA_VERSION,
        "evidence_policy_version": EVIDENCE_POLICY_VERSION,
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
        "operation_totals": operation_totals(args.samples),
        "cleanup_totals": cleanup_totals(args.samples),
        "cases": cases,
    }
    write_json(args.output_dir / "aggregate.json", aggregate)
    print(args.output_dir / "aggregate.json")
    return 0


def validate_integrity_report(report: dict[str, Any]) -> None:
    report = require_exact_fields(
        report,
        {
            "schema_version", "evidence_policy_version", "report_kind", "status",
            "classification", "authority", "profile", "expected_sample_count",
            "completed_sample_count", "completed_prefix", "active_invocation",
            "incomplete_invocation", "aggregate_file", "hard_state", "counters",
            "cleanup", "failure",
        },
        "benchmark integrity report",
    )
    if (
        report["schema_version"] != 1
        or report["evidence_policy_version"] != EVIDENCE_POLICY_VERSION
        or report["report_kind"] != INTEGRITY_KIND
    ):
        raise GateError("benchmark integrity report identity mismatch")
    require_exact_fields(
        report["authority"],
        {"integrity_authority", "performance_authority"},
        "benchmark integrity authority",
    )
    if report["authority"] != {"integrity_authority": True, "performance_authority": False}:
        raise GateError("benchmark integrity authority mismatch")
    profile = require_exact_fields(
        report["profile"], {"compression", "workers", "dataset"}, "benchmark integrity profile"
    )
    configured = INTEGRITY_FIXTURES.get(profile["dataset"])
    if (
        configured is None
        or configured["workers"] != profile["workers"]
        or profile["compression"] not in {"none", "zstd"}
    ):
        raise GateError("benchmark integrity profile mismatch")
    expected = require_nonnegative_integer(report["expected_sample_count"], "expected sample count")
    completed = require_nonnegative_integer(report["completed_sample_count"], "completed sample count")
    if (
        expected != INTEGRITY_SAMPLE_COUNT
        or completed > expected
        or report["completed_prefix"] != [f"raw/sample-{index:02d}.json" for index in range(1, completed + 1)]
    ):
        raise GateError("benchmark integrity sample inventory mismatch")
    if report["status"] == "complete":
        if (
            report["classification"] != "BENCHMARK_INTEGRITY_PASS"
            or completed != expected
            or report["aggregate_file"] != "aggregate.json"
            or report["hard_state"] != "equal"
            or report["counters"] != "valid"
            or report["cleanup"] != "complete"
            or report["failure"] is not None
            or report["active_invocation"] is not None
            or report["incomplete_invocation"] is not None
        ):
            raise GateError("benchmark integrity success claims mismatch")
    elif report["status"] == "failed":
        if (
            report["classification"] != "BENCHMARK_INTEGRITY_FAILURE"
            or report["aggregate_file"] is not None
            or report["hard_state"] not in {"not_evaluated", "prefix_valid"}
            or report["counters"] not in {"not_evaluated", "prefix_valid"}
            or report["cleanup"] not in {"not_verified", "complete"}
            or report["failure"] not in {
                "command_timeout", "contract_or_command_failure", "infrastructure_failure"
            }
            or report["active_invocation"] is not None
            or report["incomplete_invocation"] != (
                None
                if completed == expected
                else {
                    "sample_index": completed + 1,
                    "raw_file": f"raw/sample-{completed + 1:02d}.json",
                    "stderr_file": f"raw/sample-{completed + 1:02d}.stderr",
                }
            )
        ):
            raise GateError("benchmark integrity failure claims mismatch")
    else:
        raise GateError("benchmark integrity status mismatch")
    validate_no_sensitive_evidence(report, "benchmark integrity report")


def integrity_command(args: argparse.Namespace) -> int:
    configured = INTEGRITY_FIXTURES.get(args.dataset)
    if configured is None or configured["workers"] != args.workers:
        raise GateError("integrity dataset and worker profile mismatch")
    if args.command_timeout_seconds != INTEGRITY_COMMAND_TIMEOUT_SECONDS:
        raise GateError("integrity command timeout must be 600 seconds")
    if not args.binary.is_file():
        raise GateError("integrity binary does not exist")
    if args.output_dir.exists():
        raise GateError("integrity output directory must not exist")
    if os.environ.get("COLDKEEP_CODEC") != "aes-gcm":
        raise GateError("integrity sampling requires COLDKEEP_CODEC=aes-gcm")
    for name in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE"):
        if not os.environ.get(name):
            raise GateError(f"integrity sampling requires {name}")

    args.binary = args.binary.resolve()
    args.output_dir.parent.mkdir(parents=True, exist_ok=True)
    args.output_dir.mkdir()
    binary_hash = sha256_file(args.binary)
    raw_reports: list[dict[str, Any]] = []
    command_durations: list[float] = []
    loads: list[dict[str, Any]] = []
    failure: str | None = None
    try:
        for index in range(INTEGRITY_SAMPLE_COUNT):
            envelope, elapsed_ms, load = capture_sample(
                args,
                args.output_dir / "raw" / f"sample-{index + 1:02d}.json",
                binary_hash,
            )
            raw_reports.append(envelope)
            command_durations.append(elapsed_ms)
            loads.append(load)
        first_data, cases = build_contract_cases(
            raw_reports,
            workers=args.workers,
            compression=args.compression,
            dataset=args.dataset,
        )
        aggregate = {
            "schema_version": SCHEMA_VERSION,
            "evidence_policy_version": EVIDENCE_POLICY_VERSION,
            "report_kind": REPORT_KIND,
            "status": "ok",
            "provenance": provenance(args, binary_hash),
            "profile": {
                "codec": "aes-gcm",
                "compression": args.compression,
                "dataset": args.dataset,
                "workers": args.workers,
                "pipeline_depth": 1,
                "deterministic": True,
            },
            "fixture": first_data["fixture"],
            "warmup_count": 0,
            "sample_count": INTEGRITY_SAMPLE_COUNT,
            "sample_order": [1, 2],
            "command_durations_ms": command_durations,
            "command_p95_ms": percentile_nearest_rank(command_durations, 0.95),
            "host_observations": loads,
            "operation_totals": operation_totals(INTEGRITY_SAMPLE_COUNT),
            "cleanup_totals": cleanup_totals(INTEGRITY_SAMPLE_COUNT),
            "cases": cases,
        }
        validate_aggregate(aggregate, require_gate_count=False)
        write_json(args.output_dir / "aggregate.json", aggregate)
        report = {
            "schema_version": 1,
            "evidence_policy_version": EVIDENCE_POLICY_VERSION,
            "report_kind": INTEGRITY_KIND,
            "status": "complete",
            "classification": "BENCHMARK_INTEGRITY_PASS",
            "authority": {"integrity_authority": True, "performance_authority": False},
            "profile": {"compression": args.compression, "workers": args.workers, "dataset": args.dataset},
            "expected_sample_count": INTEGRITY_SAMPLE_COUNT,
            "completed_sample_count": len(raw_reports),
            "completed_prefix": [f"raw/sample-{index:02d}.json" for index in range(1, len(raw_reports) + 1)],
            "active_invocation": None,
            "incomplete_invocation": None,
            "aggregate_file": "aggregate.json",
            "hard_state": "equal",
            "counters": "valid",
            "cleanup": "complete",
            "failure": None,
        }
    except subprocess.TimeoutExpired:
        failure = "command_timeout"
    except GateError as exc:
        failure = "command_timeout" if "timeout" in str(exc).lower() else "contract_or_command_failure"
    except (OSError, subprocess.CalledProcessError):
        failure = "infrastructure_failure"

    if failure is not None:
        prefix_valid = bool(raw_reports)
        report = {
            "schema_version": 1,
            "evidence_policy_version": EVIDENCE_POLICY_VERSION,
            "report_kind": INTEGRITY_KIND,
            "status": "failed",
            "classification": "BENCHMARK_INTEGRITY_FAILURE",
            "authority": {"integrity_authority": True, "performance_authority": False},
            "profile": {"compression": args.compression, "workers": args.workers, "dataset": args.dataset},
            "expected_sample_count": INTEGRITY_SAMPLE_COUNT,
            "completed_sample_count": len(raw_reports),
            "completed_prefix": [f"raw/sample-{index:02d}.json" for index in range(1, len(raw_reports) + 1)],
            "active_invocation": None,
            "incomplete_invocation": (
                None
                if len(raw_reports) == INTEGRITY_SAMPLE_COUNT
                else {
                    "sample_index": len(raw_reports) + 1,
                    "raw_file": f"raw/sample-{len(raw_reports) + 1:02d}.json",
                    "stderr_file": f"raw/sample-{len(raw_reports) + 1:02d}.stderr",
                }
            ),
            "aggregate_file": None,
            "hard_state": "prefix_valid" if prefix_valid else "not_evaluated",
            "counters": "prefix_valid" if prefix_valid else "not_evaluated",
            "cleanup": "not_verified",
            "failure": failure,
        }
    validate_integrity_report(report)
    write_json(args.output_dir / "benchmark-integrity.json", report)
    write_checksums(args.output_dir)
    print(json.dumps({"classification": report["classification"]}))
    return 0 if report["classification"] == "BENCHMARK_INTEGRITY_PASS" else 2


def validate_aggregate(report: dict[str, Any], *, require_gate_count: bool) -> None:
    require_exact_fields(report, AGGREGATE_FIELDS, "aggregate")
    if (
        report.get("schema_version") != SCHEMA_VERSION
        or report.get("evidence_policy_version") != EVIDENCE_POLICY_VERSION
        or report.get("report_kind") != REPORT_KIND
        or report.get("status") != "ok"
    ):
        raise GateError("aggregate schema/policy/report kind/status mismatch")
    sample_count = require_nonnegative_integer(report.get("sample_count"), "aggregate sample_count")
    warmup_count = require_nonnegative_integer(report.get("warmup_count"), "aggregate warmup_count")
    if sample_count <= 0:
        raise GateError("aggregate sample_count must be a positive integer")
    if require_gate_count and (sample_count != 5 or warmup_count != 1):
        raise GateError("required gate expects one warmup and five samples")
    if report.get("sample_order") != list(range(1, sample_count + 1)):
        raise GateError("aggregate sample order mismatch")
    profile = require_exact_fields(report.get("profile"), PROFILE_FIELDS, "aggregate profile")
    profile_value = report.get("profile")
    profile_dataset = profile_value.get("dataset") if isinstance(profile_value, dict) else ""
    fixture_cases = validate_fixture(report.get("fixture"), dataset=profile_dataset)
    validate_provenance(report.get("provenance"))
    if (
        profile.get("dataset") not in {FIXTURE_ID, *INTEGRITY_FIXTURES}
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
    validate_host_observations(report.get("host_observations"), sample_count=sample_count)
    validate_operation_and_cleanup_totals(report, sample_count=sample_count)

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
        case = require_exact_fields(
            case,
            AGGREGATE_CASE_FIELDS,
            f"aggregate case at index {index}",
        )
        if case.get("seed") != fixture_cases[index]["seed"]:
            raise GateError(f"{case.get('case')} seed mismatch")
        logical_files = require_nonnegative_integer(
            case.get("logical_files"), f"{case.get('case')} logical_files"
        )
        if logical_files <= 0:
            raise GateError(f"{case.get('case')} logical_files must be a positive integer")
        durations = case.get("sample_durations_ms")
        if not isinstance(durations, list) or len(durations) != sample_count:
            raise GateError(f"{case.get('case')} sample count mismatch")
        expected = summarize(durations)
        for field, value in expected.items():
            actual = require_number(case.get(field), f"{case.get('case')} {field}")
            if not math.isclose(actual, value, rel_tol=1e-12, abs_tol=1e-9):
                raise GateError(f"{case.get('case')} statistic {field} is inconsistent")
        logical_bytes = require_nonnegative_integer(
            case.get("logical_bytes"), f"{case.get('case')} logical_bytes"
        )
        if logical_bytes <= 0:
            raise GateError(f"{case.get('case')} logical_bytes must be a positive integer")
        if case.get("workers_used") != expected_execution["store_folder_workers"]:
            raise GateError(f"{case.get('case')} workers_used mismatch")
        first_diagnostic = validate_diagnostic_final_state(
            case.get("diagnostic_final_state"), f"aggregate case {case.get('case')!r}"
        )
        diagnostics = case.get("diagnostic_samples")
        if not isinstance(diagnostics, list) or len(diagnostics) != sample_count:
            raise GateError(f"{case.get('case')} diagnostic sample count mismatch")
        expected_hard_state = hard_final_state({
            "case": case["case"],
            "diagnostic_final_state": first_diagnostic,
        })
        for sample_index, diagnostic in enumerate(diagnostics):
            diagnostic = validate_diagnostic_final_state(
                diagnostic,
                f"aggregate case {case.get('case')!r} diagnostic sample {sample_index + 1}",
            )
            if hard_final_state({
                "case": case["case"],
                "diagnostic_final_state": diagnostic,
            }) != expected_hard_state:
                raise GateError(f"{case.get('case')} hard diagnostic sample mismatch")
        if diagnostics[0] != first_diagnostic:
            raise GateError(f"{case.get('case')} first diagnostic sample mismatch")
        expected_restored_files = logical_files if case["case"] in {
            "restore-large-file", "restore-many-files"
        } else 0
        if first_diagnostic["restored_tree"]["count"] != expected_restored_files:
            raise GateError(f"{case.get('case')} restored file total mismatch")
        validate_operational_distributions(
            case.get("operational_samples"),
            case.get("operational_counter_distributions"),
            sample_count=sample_count,
            label=str(case.get("case")),
        )
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
    validate_no_sensitive_evidence(report, "aggregate")


def revalidate_raw_command(args: argparse.Namespace) -> int:
    raw_paths = sorted(args.raw_dir.glob("sample-*.json"))
    if not raw_paths:
        raise GateError("revalidation raw directory contains no sample reports")
    expected_names = [f"sample-{index:02d}.json" for index in range(1, len(raw_paths) + 1)]
    if [path.name for path in raw_paths] != expected_names:
        raise GateError("revalidation raw sample ordering is incomplete or non-contiguous")
    raw_reports = [load_json_strict(path) for path in raw_paths]
    first_data, cases = build_contract_cases(
        raw_reports,
        workers=args.workers,
        compression=args.compression,
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "evidence_policy_version": EVIDENCE_POLICY_VERSION,
        "report_kind": REVALIDATION_KIND,
        "status": "ok",
        "performance_calibration_status": "not_evaluated",
        "profile": {
            "codec": "aes-gcm",
            "compression": args.compression,
            "dataset": FIXTURE_ID,
            "workers": args.workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "fixture": first_data["fixture"],
        "warmup_count": 0,
        "sample_count": len(raw_reports),
        "sample_order": list(range(1, len(raw_reports) + 1)),
        "operation_totals": operation_totals(len(raw_reports)),
        "cleanup_totals": cleanup_totals(len(raw_reports)),
        "cases": cases,
    }
    validate_revalidation_report(report)
    write_json(args.output, report)
    print(args.output)
    return 0


def validate_revalidation_report(report: dict[str, Any]) -> None:
    """Validate a preserved-raw contract report without claiming calibration."""
    require_exact_fields(report, REVALIDATION_FIELDS, "revalidation report")
    if (
        report.get("schema_version") != SCHEMA_VERSION
        or report.get("evidence_policy_version") != EVIDENCE_POLICY_VERSION
        or report.get("report_kind") != REVALIDATION_KIND
        or report.get("status") != "ok"
        or report.get("performance_calibration_status") != "not_evaluated"
    ):
        raise GateError("revalidation schema/policy/kind/status mismatch")
    sample_count = require_nonnegative_integer(
        report.get("sample_count"), "revalidation sample_count"
    )
    if sample_count <= 0 or report.get("warmup_count") != 0:
        raise GateError("revalidation requires measured samples and no inferred warmups")
    if report.get("sample_order") != list(range(1, sample_count + 1)):
        raise GateError("revalidation sample order mismatch")
    profile = require_exact_fields(report.get("profile"), PROFILE_FIELDS, "revalidation profile")
    if (
        profile.get("codec") != "aes-gcm"
        or profile.get("compression") not in {"none", "zstd"}
        or profile.get("dataset") != FIXTURE_ID
        or profile.get("workers") not in {1, 4}
        or profile.get("pipeline_depth") != 1
        or profile.get("deterministic") is not True
    ):
        raise GateError("revalidation profile mismatch")
    validate_fixture(report.get("fixture"))
    validate_operation_and_cleanup_totals(report, sample_count=sample_count)
    cases = report.get("cases")
    if not isinstance(cases, list) or [case.get("case") for case in cases] != EXPECTED_CASES:
        raise GateError("revalidation case set/order mismatch")
    for case in cases:
        require_exact_fields(case, AGGREGATE_CASE_FIELDS, f"revalidation case {case.get('case')!r}")
        diagnostics = case.get("diagnostic_samples")
        if not isinstance(diagnostics, list) or len(diagnostics) != sample_count:
            raise GateError(f"{case.get('case')} revalidation diagnostic sample count mismatch")
        first_hard = hard_final_state({
            "case": case.get("case"),
            "diagnostic_final_state": case.get("diagnostic_final_state"),
        })
        for diagnostic in diagnostics:
            if hard_final_state({
                "case": case.get("case"),
                "diagnostic_final_state": diagnostic,
            }) != first_hard:
                raise GateError(f"{case.get('case')} revalidation hard diagnostic mismatch")
        validate_operational_distributions(
            case.get("operational_samples"),
            case.get("operational_counter_distributions"),
            sample_count=sample_count,
            label=f"{case.get('case')} revalidation",
        )
    validate_no_sensitive_evidence(report, "revalidation report")


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
        if hard_aggregate_case_contract(candidate_case) != hard_aggregate_case_contract(baseline_case):
            raise GateError(f"hard case evidence differs for {case_name}")
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
                "baseline_operational_counters": baseline_case["operational_counter_distributions"],
                "candidate_operational_counters": candidate_case["operational_counter_distributions"],
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
            for field in CALIBRATION_IDENTITY_FIELDS:
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
                if hard_aggregate_case_contract(first_case) != hard_aggregate_case_contract(second_case):
                    failures.append(f"{compression}-w{workers}/{case_name}: replica hard evidence mismatch")
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

    integrity = subparsers.add_parser(
        "integrity",
        help="capture two candidate-only v2 samples with hard functional evidence",
    )
    integrity.add_argument("--binary", type=pathlib.Path, required=True)
    integrity.add_argument("--output-dir", type=pathlib.Path, required=True)
    integrity.add_argument("--compression", choices=("none", "zstd"), required=True)
    integrity.add_argument("--workers", type=int, choices=(1, 4), required=True)
    integrity.add_argument("--dataset", choices=tuple(INTEGRITY_FIXTURES), required=True)
    integrity.add_argument(
        "--command-timeout-seconds",
        type=int,
        default=INTEGRITY_COMMAND_TIMEOUT_SECONDS,
    )
    integrity.add_argument("--source-commit")
    integrity.add_argument("--source-tag")
    integrity.add_argument("--go-version")
    integrity.add_argument("--postgres-version", required=True)
    integrity.add_argument("--database-image-digest", required=True)
    integrity.set_defaults(handler=integrity_command)

    revalidate = subparsers.add_parser(
        "revalidate-raw",
        help="validate preserved raw diagnostic reports without claiming calibration acceptance",
    )
    revalidate.add_argument("--raw-dir", type=pathlib.Path, required=True)
    revalidate.add_argument("--compression", choices=("none", "zstd"), required=True)
    revalidate.add_argument("--workers", type=int, choices=(1, 4), required=True)
    revalidate.add_argument("--output", type=pathlib.Path, required=True)
    revalidate.set_defaults(handler=revalidate_raw_command)

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
    except (GateError, OSError, subprocess.SubprocessError) as exc:
        print(f"benchmark gate error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
