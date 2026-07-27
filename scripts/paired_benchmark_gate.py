#!/usr/bin/env python3
"""Run and validate Coldkeep's same-job reference/candidate benchmark gate.

Raw benchmark and diagnostic-final-state payloads remain schema version 2.  This
module adds the independent ``benchmark_paired_comparison`` schema version 1.
It intentionally contains no production threshold values and no default
reference: both are governed inputs that must be added in later stages.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from decimal import Decimal
from typing import Any

import benchmark_gate as raw_gate


REPORT_KIND = "benchmark_paired_comparison"
DECISION_KIND = "benchmark_paired_decision"
REFERENCE_MANIFEST_KIND = "benchmark_paired_reference_manifest"
THRESHOLD_POLICY_KIND = "benchmark_paired_threshold_policy"
SCHEMA_VERSION = 1
EVIDENCE_POLICY_VERSION = 2
CONTRACT_VERSION = "coldkeep-paired-v1"
RAW_SCHEMA_VERSION = 2
DIAGNOSTIC_SCHEMA_VERSION = 2
GOVERNED_MANIFEST_RELATIVE = pathlib.PurePosixPath(
    "benchmarks/paired/reference-v1.13.json"
)
GOVERNED_THRESHOLD_RELATIVE = pathlib.PurePosixPath(
    "benchmarks/paired/threshold-policy-v1.13.json"
)
# A later, separately authorized governance commit must enable production only
# after adding the fixed manifest/policy and trusted-base workflow integration.
PRODUCTION_SAMPLING_AUTHORIZED = False

SENSITIVE_CAPTURE_PATTERNS = (
    re.compile(r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb)://"),
    re.compile(
        r"(?i)\b(?:password|passwd|credential|encryption_key|dsn|dbname|"
        r"database_name|db_name|username|user_name)\s*[:=]"
    ),
    re.compile(r"(?i)\b(?:DB_PASSWORD|COLDKEEP_KEY|DATABASE_URL|PGPASSWORD)\b"),
    re.compile(r"(?i)(?:^|[\s'\"=])/(?:home|tmp|var|workspaces)/[^\s'\"]+"),
    re.compile(r"(?i)(?:^|[\s'\"=])[A-Z]:[\\/][^\s'\"]+"),
    re.compile(r"(?i)coldkeep[_-]bench(?:mark)?[_-]"),
)

ORDERED_CASES = tuple(raw_gate.EXPECTED_CASES)
PERFORMANCE_CASES = (
    "store-large-file",
    "store-many-small-files",
    "restore-many-files",
    "snapshot-creation",
    "gc-after-churn",
    "stats-inspect",
    "verify-system-deep",
)
WARMUP_ORDER = ("candidate", "reference")
FIVE_PAIR_ORDER = (
    ("reference", "candidate"),
    ("candidate", "reference"),
    ("candidate", "reference"),
    ("reference", "candidate"),
    ("reference", "candidate"),
)
TEN_PAIR_ORDER = FIVE_PAIR_ORDER + tuple(
    tuple(reversed(pair_order)) for pair_order in FIVE_PAIR_ORDER
)
PROFILE_MATRIX = {
    "none-w1": ("none", 1, "ci-paired-w1-v1"),
    "none-w4": ("none", 4, "ci-paired-w4-v1"),
    "zstd-w1": ("zstd", 1, "ci-paired-w1-v1"),
    "zstd-w4": ("zstd", 4, "ci-paired-w4-v1"),
}
FIXTURES = {
    "ci-paired-w1-v1": {
        "id": "ci-paired-w1-v1",
        "seed": 1701,
        "large_file_size_bytes": 96 * 1024 * 1024,
        "many_small_file_count": 600,
        "many_small_file_size_bytes": 1024,
        "mixed_file_count": 400,
        "mixed_min_file_size_bytes": 1024,
        "mixed_max_file_size_bytes": 256 * 1024,
        "remove_every": 4,
        "case_database_isolation": True,
        "workers": 1,
    },
    "ci-paired-w4-v1": {
        "id": "ci-paired-w4-v1",
        "seed": 1701,
        "large_file_size_bytes": 128 * 1024 * 1024,
        "many_small_file_count": 1200,
        "many_small_file_size_bytes": 1024,
        "mixed_file_count": 800,
        "mixed_min_file_size_bytes": 1024,
        "mixed_max_file_size_bytes": 256 * 1024,
        "remove_every": 4,
        "case_database_isolation": True,
        "workers": 4,
    },
}

CLASSIFICATIONS = {
    "CONTRACT_INVALID",
    "PAIR_INVENTORY_INVALID",
    "REFERENCE_GOVERNANCE_INVALID",
    "BINARY_IDENTITY_INVALID",
    "EXECUTION_CONTRACT_MISMATCH",
    "REFERENCE_FUNCTIONAL_FAILURE",
    "CANDIDATE_FUNCTIONAL_FAILURE",
    "CORRECTNESS_REGRESSION",
    "EVIDENCE_INTEGRITY_FAILURE",
    "BENCHMARK_ENVIRONMENT_UNSTABLE",
    "PERFORMANCE_REGRESSION",
    "CANDIDATE_TIMEOUT_INCONCLUSIVE",
    "CI_INFRASTRUCTURE_TIMEOUT",
    "PASS",
}
DECISION_PRECEDENCE = (
    "CONTRACT_INVALID",
    "PAIR_INVENTORY_INVALID",
    "REFERENCE_GOVERNANCE_INVALID",
    "BINARY_IDENTITY_INVALID",
    "EXECUTION_CONTRACT_MISMATCH",
    "REFERENCE_FUNCTIONAL_FAILURE",
    "CANDIDATE_FUNCTIONAL_FAILURE",
    "CANDIDATE_TIMEOUT_INCONCLUSIVE",
    "CI_INFRASTRUCTURE_TIMEOUT",
    "CORRECTNESS_REGRESSION",
    "EVIDENCE_INTEGRITY_FAILURE",
    "BENCHMARK_ENVIRONMENT_UNSTABLE",
    "PERFORMANCE_REGRESSION",
    "PASS",
)


class PairedGateError(raw_gate.GateError):
    """A fail-closed paired-gate error with a stable classification."""

    def __init__(self, classification: str, message: str):
        if classification not in CLASSIFICATIONS - {"PASS"}:
            raise ValueError(f"invalid paired classification {classification!r}")
        super().__init__(message)
        self.classification = classification


def fail(classification: str, message: str) -> None:
    raise PairedGateError(classification, message)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, nested in pairs:
        if key in value:
            raise raw_gate.GateError(f"duplicate JSON key {key!r}")
        value[key] = nested
    return value


def load_json_strict(path: pathlib.Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise raw_gate.GateError(f"JSON input must be a regular non-symlink file: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise raw_gate.GateError(f"read JSON input {path}: {exc}") from exc
    decoder = json.JSONDecoder(
        object_pairs_hook=_reject_duplicate_json_keys,
        parse_constant=lambda value: (_ for _ in ()).throw(
            raw_gate.GateError(f"non-finite JSON value {value!r}")
        ),
    )
    try:
        value, end = decoder.raw_decode(text)
    except (json.JSONDecodeError, raw_gate.GateError) as exc:
        raise raw_gate.GateError(f"malformed JSON in {path}: {exc}") from exc
    if text[end:].strip():
        raise raw_gate.GateError(f"trailing JSON or content in {path}")
    if not isinstance(value, dict):
        raise raw_gate.GateError(f"top-level JSON value in {path} must be an object")
    return value


def require_relative_artifact_path(value: Any, label: str) -> pathlib.PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} must be a normalized relative path")
    relative = pathlib.PurePosixPath(value)
    if (
        relative.is_absolute()
        or value != relative.as_posix()
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} must be a contained relative path")
    return relative


def _contained_regular_file(
    directory: pathlib.Path, relative: pathlib.PurePosixPath, label: str
) -> pathlib.Path:
    if directory.is_symlink() or not directory.is_dir():
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} directory is not a regular directory")
    root = directory.resolve()
    path = directory.joinpath(*relative.parts)
    if path.is_symlink() or not path.is_file():
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} is missing or is a symlink")
    try:
        path.resolve().relative_to(root)
    except ValueError:
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} escapes the artifact directory")
    cursor = path.parent
    while cursor != directory:
        if cursor.is_symlink():
            fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} traverses a symlink directory")
        cursor = cursor.parent
    return path


def _artifact_files(directory: pathlib.Path) -> set[str]:
    if directory.is_symlink() or not directory.is_dir():
        fail("EVIDENCE_INTEGRITY_FAILURE", "artifact root must be a non-symlink directory")
    files: set[str] = set()
    for path in directory.rglob("*"):
        if path.is_symlink():
            fail("EVIDENCE_INTEGRITY_FAILURE", "artifact contains a symlink")
        if path.is_file():
            files.add(path.relative_to(directory).as_posix())
    return files


def _capture_text_is_sensitive(value: str) -> bool:
    try:
        raw_gate.validate_no_sensitive_evidence({"capture": value}, "captured output")
    except raw_gate.GateError:
        return True
    return any(pattern.search(value) for pattern in SENSITIVE_CAPTURE_PATTERNS)


def _sanitize_failure_captures(directory: pathlib.Path) -> None:
    raw_root = directory / "raw"
    if not raw_root.is_dir():
        return
    for path in raw_root.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            value = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            path.write_text("[captured output omitted: invalid text]\n", encoding="utf-8")
            continue
        if "\x00" in value or _capture_text_is_sensitive(value):
            path.write_text("[captured output omitted: sensitive content]\n", encoding="utf-8")


def _create_output_directory(path: pathlib.Path) -> None:
    if path.exists() or path.is_symlink():
        fail("EVIDENCE_INTEGRITY_FAILURE", "output directory already exists")
    try:
        path.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        fail("EVIDENCE_INTEGRITY_FAILURE", f"create output directory: {exc}")
    if path.is_symlink() or not path.is_dir():
        fail("EVIDENCE_INTEGRITY_FAILURE", "output directory is not a regular directory")


def _repository_root() -> pathlib.Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    )
    if completed.returncode != 0:
        fail("REFERENCE_GOVERNANCE_INVALID", "cannot resolve repository root")
    return pathlib.Path(completed.stdout.strip()).resolve()


def _governed_repository_file(
    repository: pathlib.Path, relative: pathlib.PurePosixPath, label: str
) -> pathlib.Path:
    root = repository.resolve()
    path = repository.joinpath(*relative.parts)
    if path.is_symlink() or not path.is_file():
        fail("REFERENCE_GOVERNANCE_INVALID", f"{label} is absent or is a symlink")
    try:
        path.resolve().relative_to(root)
    except ValueError:
        fail("REFERENCE_GOVERNANCE_INVALID", f"{label} escapes the repository")
    cursor = path.parent
    while cursor != repository:
        if cursor.is_symlink():
            fail("REFERENCE_GOVERNANCE_INVALID", f"{label} traverses a symlink directory")
        cursor = cursor.parent
    return path


def require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        fail("REFERENCE_GOVERNANCE_INVALID", f"{label} must be a lowercase 40-character SHA")
    return value


def validate_repository_id(value: Any) -> str:
    if value == "local":
        return value
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value
    ):
        fail("EXECUTION_CONTRACT_MISMATCH", "repository identity is not sanitized")
    return value


def profile_artifact_name(
    *, candidate_sha: str, reference_sha: str, compression: str, workers: int, attempt: int
) -> str:
    require_sha(candidate_sha, "candidate SHA")
    require_sha(reference_sha, "reference SHA")
    if compression not in {"none", "zstd"} or workers not in {1, 4}:
        fail("EXECUTION_CONTRACT_MISMATCH", "artifact profile identity is invalid")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
        fail("CONTRACT_INVALID", "artifact attempt must be a positive integer")
    return (
        f"benchmark-paired-{candidate_sha[:12]}-against-{reference_sha[:12]}-"
        f"{compression}-w{workers}-a{attempt}"
    )


def decision_artifact_name(*, candidate_sha: str, reference_sha: str, attempt: int) -> str:
    require_sha(candidate_sha, "candidate SHA")
    require_sha(reference_sha, "reference SHA")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
        fail("CONTRACT_INVALID", "artifact attempt must be a positive integer")
    return (
        f"benchmark-paired-{candidate_sha[:12]}-against-{reference_sha[:12]}-"
        f"decision-a{attempt}"
    )


def measured_order(pair_count: int) -> tuple[tuple[str, str], ...]:
    if pair_count == 5:
        return FIVE_PAIR_ORDER
    if pair_count == 10:
        return TEN_PAIR_ORDER
    fail("PAIR_INVENTORY_INVALID", "paired sampling requires exactly 5 or 10 pairs")
    raise AssertionError("unreachable")


def fixture_contract(dataset: str, workers: int) -> dict[str, Any]:
    expected = FIXTURES.get(dataset)
    if expected is None:
        fail("EXECUTION_CONTRACT_MISMATCH", f"unsupported paired fixture {dataset!r}")
    if expected["workers"] != workers:
        fail(
            "EXECUTION_CONTRACT_MISMATCH",
            f"fixture {dataset!r} requires workers={expected['workers']}",
        )
    return {key: value for key, value in expected.items() if key != "workers"}


def validate_fixture(value: Any, *, dataset: str, workers: int) -> dict[str, Any]:
    expected = fixture_contract(dataset, workers)
    try:
        fixture = raw_gate.require_exact_fields(
            value, set(expected) | {"ordered_cases"}, "paired fixture"
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    for field, expected_value in expected.items():
        if fixture.get(field) != expected_value:
            fail(
                "EXECUTION_CONTRACT_MISMATCH",
                f"paired fixture field {field!r} does not match {dataset!r}",
            )
    ordered = fixture["ordered_cases"]
    if not isinstance(ordered, list) or len(ordered) != len(ORDERED_CASES):
        fail("CONTRACT_INVALID", "paired fixture ordered case count mismatch")
    for index, expected_name in enumerate(ORDERED_CASES):
        try:
            descriptor = raw_gate.require_exact_fields(
                ordered[index], {"name", "seed"}, f"paired fixture case {index + 1}"
            )
        except raw_gate.GateError as exc:
            fail("CONTRACT_INVALID", str(exc))
        if descriptor["name"] != expected_name or descriptor["seed"] != 1712 + 10 * index:
            fail("EXECUTION_CONTRACT_MISMATCH", f"paired fixture case {index + 1} mismatch")
    return fixture


def validate_raw_report(
    envelope: Any, *, dataset: str, workers: int, compression: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Validate raw schema v2 without changing the legacy fixture authority."""
    try:
        envelope = raw_gate.require_exact_fields(
            envelope, raw_gate.RAW_ENVELOPE_FIELDS, "raw report envelope"
        )
        if envelope["status"] != "ok" or envelope["command"] != "benchmark":
            fail("CONTRACT_INVALID", "raw report must be a successful benchmark envelope")
        data = raw_gate.require_exact_fields(
            envelope["data"], raw_gate.RAW_DATA_FIELDS, "raw report data"
        )
        if data["schema_version"] != RAW_SCHEMA_VERSION:
            fail("CONTRACT_INVALID", f"raw report schema must be {RAW_SCHEMA_VERSION}")
        if data["dataset"] != dataset or data["repeat"] != 1:
            fail("EXECUTION_CONTRACT_MISMATCH", "raw dataset or repeat count mismatch")
        execution = raw_gate.validate_execution(
            data["execution"], workers=workers, label="raw report execution"
        )
        validate_fixture(data["fixture"], dataset=dataset, workers=workers)
        rows = data["rows"]
        if not isinstance(rows, list) or len(rows) != len(ORDERED_CASES):
            fail("CONTRACT_INVALID", "raw report row count mismatch")
        names = [row.get("case") for row in rows if isinstance(row, dict)]
        if names != list(ORDERED_CASES) or len(set(names)) != len(names):
            fail("CONTRACT_INVALID", "raw report case set/order mismatch")
        for row in rows:
            row = raw_gate.require_exact_fields(
                row, raw_gate.RAW_ROW_FIELDS, f"raw row {row.get('case')!r}"
            )
            duration = raw_gate.require_number(
                row["duration_ms"], f"{row['case']} duration", positive=True
            )
            throughput = raw_gate.require_number(
                row["throughput_mbps"], f"{row['case']} throughput", positive=True
            )
            row_execution = raw_gate.validate_execution(
                row["execution"], workers=workers, label=f"{row['case']} execution"
            )
            if row_execution != execution:
                fail("EXECUTION_CONTRACT_MISMATCH", f"{row['case']} execution mismatch")
            raw_gate.validate_operational_counters(row, workers=workers)
            expected_throughput = (
                row["execution_stats"]["total_bytes"]
                / (1024.0 * 1024.0)
                / (duration / 1000.0)
            )
            if not math.isclose(throughput, expected_throughput, rel_tol=1e-12, abs_tol=1e-12):
                fail("CONTRACT_INVALID", f"{row['case']} throughput is inconsistent")
            raw_gate.hard_final_state(row)
        raw_gate.validate_top_execution_stats(data["execution_stats"], rows, workers=workers)
        if compression not in {"none", "zstd"}:
            fail("EXECUTION_CONTRACT_MISMATCH", f"unsupported compression {compression!r}")
        raw_gate.validate_no_sensitive_evidence(envelope, "paired raw report")
        return data, rows
    except PairedGateError:
        raise
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    raise AssertionError("unreachable")


def validate_threshold_policy(value: Any) -> dict[str, float]:
    try:
        value = raw_gate.require_exact_fields(
            value,
            {"schema_version", "report_kind", "contract_version", "policy_id", "cases"},
            "paired threshold policy",
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if (
        value["schema_version"] != 1
        or value["report_kind"] != THRESHOLD_POLICY_KIND
        or value["contract_version"] != CONTRACT_VERSION
        or not isinstance(value["policy_id"], str)
        or not value["policy_id"]
    ):
        fail("CONTRACT_INVALID", "paired threshold policy identity mismatch")
    cases = value["cases"]
    if not isinstance(cases, dict) or set(cases) != set(PERFORMANCE_CASES):
        fail("CONTRACT_INVALID", "paired threshold case set mismatch")
    normalized: dict[str, float] = {}
    for case_name in PERFORMANCE_CASES:
        try:
            threshold = raw_gate.require_number(
                cases[case_name], f"threshold for {case_name}", positive=True
            )
        except raw_gate.GateError as exc:
            fail("CONTRACT_INVALID", str(exc))
        if threshold > 10.0:
            fail("CONTRACT_INVALID", f"threshold for {case_name} exceeds 10%")
        normalized[case_name] = threshold
    return normalized


def validate_reference_manifest(value: Any) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "report_kind",
        "release_train",
        "reference_sha",
        "approval",
        "contract_version",
        "raw_schema_version",
        "diagnostic_schema_version",
        "fixtures",
        "ordered_cases",
        "performance_cases",
        "execution_order",
        "pair_count",
        "threshold_policy_id",
        "threshold_policy_sha256",
    }
    try:
        value = raw_gate.require_exact_fields(value, expected_fields, "reference manifest")
        approval = raw_gate.require_exact_fields(
            value["approval"], {"kind", "value"}, "reference manifest approval"
        )
    except raw_gate.GateError as exc:
        fail("REFERENCE_GOVERNANCE_INVALID", str(exc))
    if value["schema_version"] != 1 or value["report_kind"] != REFERENCE_MANIFEST_KIND:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference manifest identity mismatch")
    if value["release_train"] != "v1.13" or value["contract_version"] != CONTRACT_VERSION:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference manifest contract mismatch")
    require_sha(value["reference_sha"], "reference manifest SHA")
    if approval["kind"] not in {"trusted_tag", "reviewed_record"} or not approval["value"]:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference approval record is invalid")
    if (
        value["raw_schema_version"] != RAW_SCHEMA_VERSION
        or value["diagnostic_schema_version"] != DIAGNOSTIC_SCHEMA_VERSION
    ):
        fail("REFERENCE_GOVERNANCE_INVALID", "reference schema compatibility mismatch")
    if value["fixtures"] != sorted(FIXTURES):
        fail("REFERENCE_GOVERNANCE_INVALID", "reference fixture inventory mismatch")
    if value["ordered_cases"] != list(ORDERED_CASES):
        fail("REFERENCE_GOVERNANCE_INVALID", "reference ordered cases mismatch")
    if value["performance_cases"] != list(PERFORMANCE_CASES):
        fail("REFERENCE_GOVERNANCE_INVALID", "reference performance cases mismatch")
    if value["execution_order"] != {
        "warmups": list(WARMUP_ORDER),
        "measured_pairs": [list(pair) for pair in FIVE_PAIR_ORDER],
    }:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference execution order mismatch")
    if value["pair_count"] != 5:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference pair count must be 5")
    if not isinstance(value["threshold_policy_id"], str) or not value["threshold_policy_id"]:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference threshold policy ID is missing")
    try:
        raw_gate.require_sha256(value["threshold_policy_sha256"], "threshold policy digest")
    except raw_gate.GateError as exc:
        fail("REFERENCE_GOVERNANCE_INVALID", str(exc))
    return value


def verify_reference_governance(
    manifest: dict[str, Any], *, reference_sha: str, candidate_sha: str, repository: pathlib.Path
) -> None:
    validate_reference_manifest(manifest)
    require_sha(reference_sha, "reference SHA")
    require_sha(candidate_sha, "candidate SHA")
    if manifest["reference_sha"] != reference_sha:
        fail("REFERENCE_GOVERNANCE_INVALID", "effective reference differs from manifest")
    for sha in (reference_sha, candidate_sha):
        completed = subprocess.run(
            ["git", "-C", str(repository), "cat-file", "-e", f"{sha}^{{commit}}"],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            fail("REFERENCE_GOVERNANCE_INVALID", f"commit {sha} is not reachable")
    approval = manifest["approval"]
    if approval["kind"] == "trusted_tag":
        if not isinstance(approval["value"], str) or not re.fullmatch(r"v[0-9A-Za-z._-]+", approval["value"]):
            fail("REFERENCE_GOVERNANCE_INVALID", "trusted tag name is invalid")
        tagged = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", f"refs/tags/{approval['value']}^{{commit}}"],
            capture_output=True,
            text=True,
        )
        if tagged.returncode != 0 or tagged.stdout.strip() != reference_sha:
            fail("REFERENCE_GOVERNANCE_INVALID", "trusted tag does not resolve to reference")
    ancestor = subprocess.run(
        ["git", "-C", str(repository), "merge-base", "--is-ancestor", reference_sha, candidate_sha],
        capture_output=True,
        text=True,
    )
    if ancestor.returncode != 0:
        fail("REFERENCE_GOVERNANCE_INVALID", "reference is not an ancestor of candidate")


def reject_candidate_governance_changes(changed_paths: list[str]) -> None:
    governed = {
        "benchmarks/paired/reference-v1.13.json",
        "benchmarks/paired/threshold-policy-v1.13.json",
    }
    if governed.intersection(changed_paths):
        fail("REFERENCE_GOVERNANCE_INVALID", "ordinary candidate changes governed benchmark policy")


def _semantic_contract(row: dict[str, Any]) -> dict[str, Any]:
    stats = row["execution_stats"]
    return {
        "case": row["case"],
        "logical_files": stats["total_files"],
        "logical_bytes": stats["total_bytes"],
        "diagnostic_final_state": raw_gate.hard_final_state(row),
    }


def validate_pair_inventory(records: list[dict[str, Any]], pair_count: int) -> None:
    expected_order = measured_order(pair_count)
    if len(records) != pair_count * 2:
        fail("PAIR_INVENTORY_INVALID", "measured invocation count mismatch")
    seen: set[tuple[int, str]] = set()
    index = 0
    for ordinal, pair_order in enumerate(expected_order, start=1):
        for position, side in enumerate(pair_order, start=1):
            record = records[index]
            index += 1
            key = (record.get("pair_ordinal"), record.get("side"))
            if key in seen:
                fail("PAIR_INVENTORY_INVALID", "duplicate paired invocation")
            seen.add(key)
            if (
                record.get("pair_ordinal") != ordinal
                or record.get("position") != position
                or record.get("side") != side
            ):
                fail("PAIR_INVENTORY_INVALID", f"altered invocation order at pair {ordinal}")


def validate_warmups(
    warmups: list[dict[str, Any]],
    measured: list[dict[str, Any]],
    *,
    dataset: str,
    workers: int,
    compression: str,
) -> None:
    if len(warmups) != 2:
        fail("PAIR_INVENTORY_INVALID", "warmup invocation count mismatch")
    for position, side in enumerate(WARMUP_ORDER, start=1):
        warmup = warmups[position - 1]
        if (
            warmup.get("kind") != "warmup"
            or warmup.get("pair_ordinal") is not None
            or warmup.get("position") != position
            or warmup.get("side") != side
        ):
            fail("PAIR_INVENTORY_INVALID", "warmup invocation order mismatch")
    baseline_records = {
        side: next(record for record in measured if record.get("side") == side)
        for side in ("reference", "candidate")
    }
    baseline_rows = {}
    baseline_data = {}
    for side, record in baseline_records.items():
        data, rows = validate_raw_report(
            record.get("envelope"), dataset=dataset, workers=workers, compression=compression
        )
        baseline_data[side], baseline_rows[side] = data, rows
    warmup_rows = {}
    for warmup in warmups:
        side = warmup["side"]
        data, rows = validate_raw_report(
            warmup.get("envelope"), dataset=dataset, workers=workers, compression=compression
        )
        if (
            data["fixture"] != baseline_data[side]["fixture"]
            or data["execution"] != baseline_data[side]["execution"]
        ):
            fail("EXECUTION_CONTRACT_MISMATCH", f"{side} warmup execution contract mismatch")
        warmup_rows[side] = rows
    for case_index, case_name in enumerate(ORDERED_CASES):
        for side in ("reference", "candidate"):
            if _semantic_contract(warmup_rows[side][case_index]) != _semantic_contract(
                baseline_rows[side][case_index]
            ):
                fail("EVIDENCE_INTEGRITY_FAILURE", f"{side} warmup hard state differs for {case_name}")
        if _semantic_contract(warmup_rows["reference"][case_index]) != _semantic_contract(
            warmup_rows["candidate"][case_index]
        ):
            fail("CORRECTNESS_REGRESSION", f"warmup hard state differs for {case_name}")


def _stability_boundary(mode: str, threshold: float | None) -> float:
    if mode == "diagnostic":
        return 2.5
    if threshold is None:
        fail("CONTRACT_INVALID", "production comparison requires governed thresholds")
    return min(3.0, threshold / 2.0)


def _decimal_median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def compare_records(
    records: list[dict[str, Any]],
    *,
    pair_count: int,
    dataset: str,
    workers: int,
    compression: str,
    mode: str,
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    if mode not in {"diagnostic", "production"}:
        fail("CONTRACT_INVALID", f"unknown comparison mode {mode!r}")
    if mode == "production" and pair_count != 5:
        fail("PAIR_INVENTORY_INVALID", "production comparison requires exactly five pairs")
    if mode == "diagnostic" and pair_count != 10:
        fail("PAIR_INVENTORY_INVALID", "diagnostic comparison requires exactly ten pairs")
    validate_pair_inventory(records, pair_count)
    if mode == "production":
        if thresholds is None or set(thresholds) != set(PERFORMANCE_CASES):
            fail("CONTRACT_INVALID", "production threshold case set mismatch")

    validated: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    first_fixture: dict[str, Any] | None = None
    first_execution: dict[str, Any] | None = None
    for index, record in enumerate(records, start=1):
        data, rows = validate_raw_report(
            record.get("envelope"), dataset=dataset, workers=workers, compression=compression
        )
        if first_fixture is None:
            first_fixture, first_execution = data["fixture"], data["execution"]
        elif data["fixture"] != first_fixture or data["execution"] != first_execution:
            fail("EXECUTION_CONTRACT_MISMATCH", f"fixture/execution changed in invocation {index}")
        validated.append((data, rows))

    case_results: list[dict[str, Any]] = []
    distributions: dict[str, dict[str, Any]] = {"reference": {}, "candidate": {}}
    any_unstable = False
    any_regression = False
    for case_index, case_name in enumerate(ORDERED_CASES):
        by_side: dict[str, list[dict[str, Any]]] = {"reference": [], "candidate": []}
        for record, (_, rows) in zip(records, validated):
            by_side[record["side"]].append(rows[case_index])
        for side, rows in by_side.items():
            expected = _semantic_contract(rows[0])
            if any(_semantic_contract(row) != expected for row in rows[1:]):
                fail("EVIDENCE_INTEGRITY_FAILURE", f"{side} hard state changed for {case_name}")
            counters = [raw_gate.validate_operational_counters(row, workers=workers) for row in rows]
            distributions[side][case_name] = raw_gate.summarize_operational_counters(counters)

        if _semantic_contract(by_side["reference"][0]) != _semantic_contract(by_side["candidate"][0]):
            fail("CORRECTNESS_REGRESSION", f"reference/candidate hard state differs for {case_name}")

        if case_name not in PERFORMANCE_CASES:
            case_results.append({"case": case_name, "performance_gated": False})
            continue

        ratio_values: list[Decimal] = []
        for ordinal in range(1, pair_count + 1):
            pair_rows = {
                record["side"]: rows[case_index]
                for record, (_, rows) in zip(records, validated)
                if record["pair_ordinal"] == ordinal
            }
            ratio_values.append(
                Decimal(str(pair_rows["candidate"]["duration_ms"]))
                / Decimal(str(pair_rows["reference"]["duration_ms"]))
            )
        median_ratio_value = _decimal_median(ratio_values)
        regression_value = (median_ratio_value - Decimal(1)) * Decimal(100)
        mad_value = _decimal_median(
            [abs(value - median_ratio_value) for value in ratio_values]
        )
        mad_ratio_value = mad_value / median_ratio_value * Decimal(100)
        ratios = [float(value) for value in ratio_values]
        median_ratio = float(median_ratio_value)
        regression_pct = float(regression_value)
        paired_mad_ratio_pct = float(mad_ratio_value)
        threshold = thresholds.get(case_name) if thresholds is not None else None
        boundary = _stability_boundary(mode, threshold)
        unstable = mad_ratio_value > Decimal(str(boundary))
        if mode == "diagnostic" and not Decimal("0.95") <= median_ratio_value <= Decimal("1.05"):
            unstable = True
        regression = bool(
            mode == "production"
            and not unstable
            and threshold is not None
            and regression_value > Decimal(str(threshold))
        )
        any_unstable = any_unstable or unstable
        any_regression = any_regression or regression
        logical_bytes = by_side["candidate"][0]["execution_stats"]["total_bytes"]
        candidate_median_ms = float(
            statistics.median(float(row["duration_ms"]) for row in by_side["candidate"])
        )
        case_results.append(
            {
                "case": case_name,
                "performance_gated": True,
                "paired_ratios": ratios,
                "median_ratio": median_ratio,
                "regression_pct": regression_pct,
                "paired_mad_ratio_pct": paired_mad_ratio_pct,
                "stability_boundary_pct": boundary,
                "threshold_pct": threshold,
                "candidate_throughput_mbps": (
                    logical_bytes / (1024.0 * 1024.0) / (candidate_median_ms / 1000.0)
                ),
                "status": "unstable" if unstable else "regression" if regression else "pass",
            }
        )

    classification = (
        "BENCHMARK_ENVIRONMENT_UNSTABLE"
        if any_unstable
        else "PERFORMANCE_REGRESSION"
        if any_regression
        else "PASS"
    )
    return {
        "classification": classification,
        "fixture": first_fixture,
        "execution": first_execution,
        "cases": case_results,
        "operational_counter_distributions": distributions,
        "hard_state_comparison": {"status": "equal", "case_count": len(ORDERED_CASES)},
    }


def _binary_hash(path: pathlib.Path) -> str:
    return raw_gate.sha256_file(path)


def _host_observation() -> dict[str, Any]:
    load = os.getloadavg() if hasattr(os, "getloadavg") else (0.0, 0.0, 0.0)
    return {
        "load_1m": load[0],
        "load_5m": load[1],
        "load_15m": load[2],
        "cpu_count": os.cpu_count() or 0,
    }


def _capture(
    *,
    binary: pathlib.Path,
    expected_hash: str,
    side: str,
    output_dir: pathlib.Path,
    relative_raw_path: pathlib.Path,
    dataset: str,
    workers: int,
    compression: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    if _binary_hash(binary) != expected_hash:
        fail("BINARY_IDENTITY_INVALID", f"{side} binary changed during sampling")
    raw_path = output_dir / relative_raw_path
    stderr_path = raw_path.with_suffix(".stderr")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    before = _host_observation()
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [
                str(binary),
                "benchmark",
                "run",
                "--dataset",
                dataset,
                "--workers",
                str(workers),
                "--repeat",
                "1",
                "--output",
                "json",
            ],
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            env={**os.environ, "COLDKEEP_COMPRESSION": compression},
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode("utf-8", errors="replace")
            if isinstance(exc.stdout, bytes)
            else exc.stdout or ""
        )
        stderr = (
            exc.stderr.decode("utf-8", errors="replace")
            if isinstance(exc.stderr, bytes)
            else exc.stderr or ""
        )
        raw_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        if side == "candidate":
            fail("CANDIDATE_TIMEOUT_INCONCLUSIVE", "candidate command exceeded safety timeout")
        fail("CI_INFRASTRUCTURE_TIMEOUT", "reference command exceeded safety timeout")
    elapsed_ms = (time.monotonic() - started) * 1000.0
    raw_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        classification = (
            "REFERENCE_FUNCTIONAL_FAILURE" if side == "reference" else "CANDIDATE_FUNCTIONAL_FAILURE"
        )
        fail(classification, f"{side} command failed with exit {completed.returncode}")
    try:
        envelope = load_json_strict(raw_path)
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    validate_raw_report(envelope, dataset=dataset, workers=workers, compression=compression)
    return {
        "envelope": envelope,
        "raw_file": relative_raw_path.as_posix(),
        "stderr_file": relative_raw_path.with_suffix(".stderr").as_posix(),
        "command_duration_ms": elapsed_ms,
        "binary_sha256": expected_hash,
        "host_observation": {"before": before, "after": _host_observation()},
    }


def _write_checksums(directory: pathlib.Path) -> None:
    lines = []
    for relative in sorted(_artifact_files(directory)):
        if relative == "checksums.sha256":
            continue
        path = _contained_regular_file(
            directory, pathlib.PurePosixPath(relative), f"artifact file {relative}"
        )
        lines.append(f"{_binary_hash(path)}  {relative}")
    (directory / "checksums.sha256").write_text("\n".join(lines) + "\n", encoding="utf-8")


def validate_checksums(
    directory: pathlib.Path, *, expected_files: set[str] | None = None
) -> None:
    checksum_path = directory / "checksums.sha256"
    if checksum_path.is_symlink() or not checksum_path.is_file():
        fail("EVIDENCE_INTEGRITY_FAILURE", "artifact checksum inventory is missing")
    actual_files = _artifact_files(directory) - {"checksums.sha256"}
    seen: set[str] = set()
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([^\r\n]+)", line)
        if match is None:
            fail("EVIDENCE_INTEGRITY_FAILURE", "malformed checksum inventory")
        digest, relative = match.groups()
        normalized = require_relative_artifact_path(relative, "checksum path")
        if relative in seen:
            fail("EVIDENCE_INTEGRITY_FAILURE", "unsafe or duplicate checksum path")
        seen.add(relative)
        path = _contained_regular_file(directory, normalized, f"checksummed file {relative}")
        if _binary_hash(path) != digest:
            fail("EVIDENCE_INTEGRITY_FAILURE", f"checksum mismatch for {relative}")
    if seen != actual_files:
        fail("EVIDENCE_INTEGRITY_FAILURE", "checksum inventory coverage mismatch")
    if expected_files is not None and actual_files != expected_files:
        fail("EVIDENCE_INTEGRITY_FAILURE", "artifact file inventory mismatch")


def _provenance(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "event_name": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "repository_id": os.environ.get("GITHUB_REPOSITORY", "local"),
        "runner_os": os.environ.get("RUNNER_OS", sys.platform),
        "runner_image": os.environ.get("ImageVersion", "local"),
        "runner_arch": os.environ.get("RUNNER_ARCH", platform.machine()),
        "cpu_count": os.cpu_count() or 0,
        "go_version": args.go_version,
        "postgres_version": args.postgres_version,
        "database_image_digest": args.database_image_digest,
    }


def sample_command(args: argparse.Namespace) -> int:
    fixture_contract(args.dataset, args.workers)
    if (args.mode, args.pairs) not in {("diagnostic", 10), ("production", 5)}:
        fail("PAIR_INVENTORY_INVALID", "mode and fixed pair count are inconsistent")
    if args.command_timeout_seconds != 600:
        fail("EXECUTION_CONTRACT_MISMATCH", "per-command safety timeout must be 600 seconds")
    if os.environ.get("COLDKEEP_CODEC") != "aes-gcm":
        fail("EXECUTION_CONTRACT_MISMATCH", "paired sampling requires COLDKEEP_CODEC=aes-gcm")
    if not args.reference_binary.is_file() or not args.candidate_binary.is_file():
        fail("BINARY_IDENTITY_INVALID", "reference and candidate binaries must exist")
    candidate_sha = require_sha(args.candidate_sha, "candidate SHA")
    thresholds: dict[str, float] | None = None
    governance: dict[str, Any]
    if args.mode == "production":
        if args.reference_sha is not None:
            fail(
                "REFERENCE_GOVERNANCE_INVALID",
                "production reference SHA cannot come from command input",
            )
        if not PRODUCTION_SAMPLING_AUTHORIZED:
            fail(
                "REFERENCE_GOVERNANCE_INVALID",
                "production paired sampling is not authorized in this repository state",
            )
        repository = _repository_root()
        manifest_path = _governed_repository_file(
            repository, GOVERNED_MANIFEST_RELATIVE, "governed reference manifest"
        )
        threshold_path = _governed_repository_file(
            repository, GOVERNED_THRESHOLD_RELATIVE, "governed threshold policy"
        )
        manifest = load_json_strict(manifest_path)
        reference_sha = manifest.get("reference_sha")
        verify_reference_governance(
            manifest,
            reference_sha=reference_sha,
            candidate_sha=candidate_sha,
            repository=repository,
        )
        policy_value = load_json_strict(threshold_path)
        thresholds = validate_threshold_policy(policy_value)
        policy_hash = _binary_hash(threshold_path)
        if manifest["threshold_policy_sha256"] != policy_hash:
            fail("REFERENCE_GOVERNANCE_INVALID", "threshold policy digest differs from manifest")
        args.reference_sha = reference_sha
        governance_dir = args.output_dir / "governance"
        governance_dir.mkdir(parents=False, exist_ok=False)
        shutil.copyfile(manifest_path, governance_dir / "reference-manifest.json")
        shutil.copyfile(threshold_path, governance_dir / "threshold-policy.json")
        governance = {
            "status": "governed",
            "manifest_sha256": _binary_hash(manifest_path),
            "threshold_policy_id": manifest["threshold_policy_id"],
            "threshold_policy_sha256": policy_hash,
        }
    else:
        if args.reference_sha is None:
            fail("REFERENCE_GOVERNANCE_INVALID", "diagnostic sampling requires an explicit reference SHA")
        reference_sha = require_sha(args.reference_sha, "reference SHA")
        governance = {
            "status": "provisional-diagnostic",
            "manifest_sha256": None,
            "threshold_policy_id": None,
            "threshold_policy_sha256": None,
        }

    binary_hashes = {
        "reference": _binary_hash(args.reference_binary),
        "candidate": _binary_hash(args.candidate_binary),
    }
    warmup_records: list[dict[str, Any]] = []
    for position, side in enumerate(WARMUP_ORDER, start=1):
        record = _capture(
            binary=getattr(args, f"{side}_binary"),
            expected_hash=binary_hashes[side],
            side=side,
            output_dir=args.output_dir,
            relative_raw_path=pathlib.Path("raw") / f"warmup-{position:02d}-{side}.json",
            dataset=args.dataset,
            workers=args.workers,
            compression=args.compression,
            timeout_seconds=args.command_timeout_seconds,
        )
        record.update({"kind": "warmup", "pair_ordinal": None, "position": position, "side": side})
        warmup_records.append(record)

    records: list[dict[str, Any]] = []
    for ordinal, pair_order in enumerate(measured_order(args.pairs), start=1):
        for position, side in enumerate(pair_order, start=1):
            record = _capture(
                binary=getattr(args, f"{side}_binary"),
                expected_hash=binary_hashes[side],
                side=side,
                output_dir=args.output_dir,
                relative_raw_path=(
                    pathlib.Path("raw") / f"pair-{ordinal:02d}" / f"{position:02d}-{side}.json"
                ),
                dataset=args.dataset,
                workers=args.workers,
                compression=args.compression,
                timeout_seconds=args.command_timeout_seconds,
            )
            record.update(
                {"kind": "measured", "pair_ordinal": ordinal, "position": position, "side": side}
            )
            records.append(record)

    comparison = compare_records(
        records,
        pair_count=args.pairs,
        dataset=args.dataset,
        workers=args.workers,
        compression=args.compression,
        mode=args.mode,
        thresholds=thresholds,
    )
    validate_warmups(
        warmup_records,
        records,
        dataset=args.dataset,
        workers=args.workers,
        compression=args.compression,
    )
    inventory = []
    for record in warmup_records + records:
        inventory.append(
            {
                key: record[key]
                for key in (
                    "kind",
                    "pair_ordinal",
                    "position",
                    "side",
                    "raw_file",
                    "stderr_file",
                    "command_duration_ms",
                    "binary_sha256",
                    "host_observation",
                )
            }
        )
    report = {
        "schema_version": SCHEMA_VERSION,
        "evidence_policy_version": EVIDENCE_POLICY_VERSION,
        "report_kind": REPORT_KIND,
        "status": "complete",
        "mode": args.mode,
        "classification": comparison["classification"],
        "contract_version": CONTRACT_VERSION,
        "identity": {
            "reference_sha": reference_sha,
            "candidate_sha": candidate_sha,
            "reference_binary_sha256": binary_hashes["reference"],
            "candidate_binary_sha256": binary_hashes["candidate"],
        },
        "governance": governance,
        "profile": {
            "codec": "aes-gcm",
            "compression": args.compression,
            "dataset": args.dataset,
            "workers": args.workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "fixture": comparison["fixture"],
        "warmup_order": list(WARMUP_ORDER),
        "measured_order": [list(pair) for pair in measured_order(args.pairs)],
        "pair_count": args.pairs,
        "invocation_inventory": inventory,
        "cases": comparison["cases"],
        "operational_counter_distributions": comparison["operational_counter_distributions"],
        "hard_state_comparison": comparison["hard_state_comparison"],
        "cleanup": {
            "status": "complete",
            "attempted": (2 + args.pairs * 2) * len(ORDERED_CASES),
            "succeeded": (2 + args.pairs * 2) * len(ORDERED_CASES),
            "failed": 0,
        },
        "provenance": _provenance(args),
    }
    raw_gate.validate_no_sensitive_evidence(report, "paired comparison report")
    raw_gate.write_json(args.output_dir / "paired-comparison.json", report)
    _write_checksums(args.output_dir)
    print(json.dumps({"classification": report["classification"], "report": "paired-comparison.json"}))
    return 0 if report["classification"] == "PASS" else 1


REPORT_FIELDS = {
    "schema_version",
    "evidence_policy_version",
    "report_kind",
    "status",
    "mode",
    "classification",
    "contract_version",
    "identity",
    "governance",
    "profile",
    "fixture",
    "warmup_order",
    "measured_order",
    "pair_count",
    "invocation_inventory",
    "cases",
    "operational_counter_distributions",
    "hard_state_comparison",
    "cleanup",
    "provenance",
}

FAILURE_REPORT_FIELDS = {
    "schema_version",
    "evidence_policy_version",
    "report_kind",
    "status",
    "mode",
    "classification",
    "contract_version",
    "identity",
    "governance_status",
    "profile",
    "requested_pair_count",
    "warmup_order",
    "measured_order",
    "attempted_invocations",
    "cleanup",
    "provenance",
}


def _validate_failure_report(report: Any, *, expected_profile: str | None) -> dict[str, Any]:
    try:
        report = raw_gate.require_exact_fields(report, FAILURE_REPORT_FIELDS, "paired failure report")
        identity = raw_gate.require_exact_fields(
            report["identity"],
            {
                "reference_sha",
                "candidate_sha",
                "reference_binary_sha256",
                "candidate_binary_sha256",
            },
            "paired failure identity",
        )
        profile = raw_gate.require_exact_fields(
            report["profile"],
            {"codec", "compression", "dataset", "workers", "pipeline_depth", "deterministic"},
            "paired failure profile",
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if (
        report["schema_version"] != 1
        or report["evidence_policy_version"] != 2
        or report["report_kind"] != REPORT_KIND
        or report["status"] != "failed"
        or report["contract_version"] != CONTRACT_VERSION
        or report["classification"] not in CLASSIFICATIONS - {"PASS", "PERFORMANCE_REGRESSION", "BENCHMARK_ENVIRONMENT_UNSTABLE"}
    ):
        fail("CONTRACT_INVALID", "paired failure report identity mismatch")
    if expected_profile is not None:
        expected = PROFILE_MATRIX.get(expected_profile)
        if expected is None or (profile["compression"], profile["workers"], profile["dataset"]) != expected:
            fail("EXECUTION_CONTRACT_MISMATCH", f"failure profile {expected_profile} mismatch")
    if profile["codec"] != "aes-gcm" or profile["pipeline_depth"] != 1 or profile["deterministic"] is not True:
        fail("EXECUTION_CONTRACT_MISMATCH", "paired failure profile policy mismatch")
    if report["requested_pair_count"] not in {5, 10}:
        fail("PAIR_INVENTORY_INVALID", "failure report pair count is invalid")
    if report["warmup_order"] != list(WARMUP_ORDER) or report["measured_order"] != [
        list(pair) for pair in measured_order(report["requested_pair_count"])
    ]:
        fail("PAIR_INVENTORY_INVALID", "failure report execution order mismatch")
    expected_governance = (
        "provisional-diagnostic" if report["mode"] == "diagnostic" else "not-established"
    )
    if report["governance_status"] != expected_governance:
        fail("REFERENCE_GOVERNANCE_INVALID", "failure governance status mismatch")
    attempted = report["attempted_invocations"]
    if not isinstance(attempted, list):
        fail("PAIR_INVENTORY_INVALID", "failure attempted inventory must be an array")
    expected_attempts = [
        {"kind": "warmup", "pair_ordinal": None, "position": position, "side": side}
        for position, side in enumerate(WARMUP_ORDER, start=1)
    ]
    expected_attempts.extend(
        {"kind": "measured", "pair_ordinal": ordinal, "position": position, "side": side}
        for ordinal, pair_order in enumerate(measured_order(report["requested_pair_count"]), start=1)
        for position, side in enumerate(pair_order, start=1)
    )
    normalized_attempts = []
    try:
        for index, invocation in enumerate(attempted):
            invocation = raw_gate.require_exact_fields(
                invocation,
                {"kind", "pair_ordinal", "position", "side", "raw_file", "stderr_file"},
                f"failure invocation {index + 1}",
            )
            normalized_attempts.append(
                {key: invocation[key] for key in ("kind", "pair_ordinal", "position", "side")}
            )
    except raw_gate.GateError as exc:
        fail("PAIR_INVENTORY_INVALID", str(exc))
    if normalized_attempts != expected_attempts[: len(normalized_attempts)]:
        fail("PAIR_INVENTORY_INVALID", "failure invocation inventory is not a fixed-order prefix")
    for index, (invocation, expected) in enumerate(
        zip(attempted, expected_attempts), start=1
    ):
        if expected["kind"] == "warmup":
            expected_raw = f"raw/warmup-{expected['position']:02d}-{expected['side']}.json"
        else:
            expected_raw = (
                f"raw/pair-{expected['pair_ordinal']:02d}/"
                f"{expected['position']:02d}-{expected['side']}.json"
            )
        if (
            require_relative_artifact_path(
                invocation["raw_file"], f"failure invocation {index} raw file"
            ).as_posix()
            != expected_raw
            or require_relative_artifact_path(
                invocation["stderr_file"], f"failure invocation {index} stderr file"
            ).as_posix()
            != pathlib.PurePosixPath(expected_raw).with_suffix(".stderr").as_posix()
        ):
            fail("PAIR_INVENTORY_INVALID", "failure invocation path/order mismatch")
    try:
        cleanup = raw_gate.require_exact_fields(
            report["cleanup"],
            {"status", "observed_invocations", "required_invocations"},
            "failure cleanup",
        )
        observed_invocations = raw_gate.require_nonnegative_integer(
            cleanup["observed_invocations"], "failure observed invocations"
        )
        required_invocations = raw_gate.require_nonnegative_integer(
            cleanup["required_invocations"], "failure required invocations"
        )
    except raw_gate.GateError as exc:
        fail("EVIDENCE_INTEGRITY_FAILURE", str(exc))
    if (
        cleanup["status"] != "incomplete"
        or observed_invocations != len(attempted)
        or required_invocations != len(expected_attempts)
        or observed_invocations >= required_invocations
    ):
        fail("EVIDENCE_INTEGRITY_FAILURE", "failure cleanup/inventory evidence mismatch")
    for field in ("reference_sha", "candidate_sha"):
        if not isinstance(identity[field], str) or not re.fullmatch(r"[0-9a-f]{40}", identity[field]):
            if report["classification"] != "REFERENCE_GOVERNANCE_INVALID":
                fail("CONTRACT_INVALID", f"paired failure {field} is invalid")
    for field in ("reference_binary_sha256", "candidate_binary_sha256"):
        if identity[field] is not None:
            try:
                raw_gate.require_sha256(identity[field], f"paired failure {field}")
            except raw_gate.GateError as exc:
                fail("CONTRACT_INVALID", str(exc))
    try:
        provenance = raw_gate.require_exact_fields(
            report["provenance"],
            {
                "event_name",
                "repository_id",
                "runner_os",
                "runner_image",
                "runner_arch",
                "cpu_count",
                "go_version",
                "postgres_version",
                "database_image_digest",
            },
            "paired failure provenance",
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    validate_repository_id(provenance["repository_id"])
    raw_gate.validate_no_sensitive_evidence(report, "paired failure report")
    return report


def validate_report_summary(report: Any, *, expected_profile: str | None = None) -> dict[str, Any]:
    if isinstance(report, dict) and report.get("status") == "failed":
        return _validate_failure_report(report, expected_profile=expected_profile)
    try:
        report = raw_gate.require_exact_fields(report, REPORT_FIELDS, "paired comparison report")
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if (
        report["schema_version"] != 1
        or report["evidence_policy_version"] != 2
        or report["report_kind"] != REPORT_KIND
        or report["status"] != "complete"
        or report["contract_version"] != CONTRACT_VERSION
        or report["classification"] not in CLASSIFICATIONS
    ):
        fail("CONTRACT_INVALID", "paired comparison report identity mismatch")
    profile = report["profile"]
    try:
        profile = raw_gate.require_exact_fields(
            profile,
            {"codec", "compression", "dataset", "workers", "pipeline_depth", "deterministic"},
            "paired profile",
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if expected_profile is not None:
        expected = PROFILE_MATRIX.get(expected_profile)
        if expected is None or (profile["compression"], profile["workers"], profile["dataset"]) != expected:
            fail("EXECUTION_CONTRACT_MISMATCH", f"profile {expected_profile} identity mismatch")
    if (
        profile["codec"] != "aes-gcm"
        or profile["compression"] not in {"none", "zstd"}
        or profile["pipeline_depth"] != 1
        or profile["deterministic"] is not True
    ):
        fail("EXECUTION_CONTRACT_MISMATCH", "paired profile policy mismatch")
    validate_fixture(report["fixture"], dataset=profile["dataset"], workers=profile["workers"])

    try:
        identity = raw_gate.require_exact_fields(
            report["identity"],
            {
                "reference_sha",
                "candidate_sha",
                "reference_binary_sha256",
                "candidate_binary_sha256",
            },
            "paired identity",
        )
        raw_gate.require_sha256(identity["reference_binary_sha256"], "reference binary")
        raw_gate.require_sha256(identity["candidate_binary_sha256"], "candidate binary")
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    for field in ("reference_sha", "candidate_sha"):
        if not isinstance(identity[field], str) or not re.fullmatch(r"[0-9a-f]{40}", identity[field]):
            fail("CONTRACT_INVALID", f"paired identity {field} is invalid")
    try:
        governance = raw_gate.require_exact_fields(
            report["governance"],
            {
                "status",
                "manifest_sha256",
                "threshold_policy_id",
                "threshold_policy_sha256",
            },
            "paired governance",
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if report["mode"] == "production":
        if governance["status"] != "governed":
            fail("REFERENCE_GOVERNANCE_INVALID", "production report is not governed")
        try:
            raw_gate.require_sha256(governance["manifest_sha256"], "manifest digest")
            raw_gate.require_sha256(governance["threshold_policy_sha256"], "threshold digest")
        except raw_gate.GateError as exc:
            fail("REFERENCE_GOVERNANCE_INVALID", str(exc))
        if not isinstance(governance["threshold_policy_id"], str) or not governance["threshold_policy_id"]:
            fail("REFERENCE_GOVERNANCE_INVALID", "threshold policy identity is missing")
    elif report["mode"] == "diagnostic":
        if governance != {
            "status": "provisional-diagnostic",
            "manifest_sha256": None,
            "threshold_policy_id": None,
            "threshold_policy_sha256": None,
        }:
            fail("REFERENCE_GOVERNANCE_INVALID", "diagnostic governance fields mismatch")
    else:
        fail("CONTRACT_INVALID", "paired report mode is invalid")
    if report["warmup_order"] != list(WARMUP_ORDER):
        fail("PAIR_INVENTORY_INVALID", "warmup order mismatch")
    if (report["mode"], report["pair_count"]) not in {("diagnostic", 10), ("production", 5)}:
        fail("PAIR_INVENTORY_INVALID", "report mode and pair count mismatch")
    if report["measured_order"] != [list(pair) for pair in measured_order(report["pair_count"])]:
        fail("PAIR_INVENTORY_INVALID", "measured order mismatch")
    inventory = report["invocation_inventory"]
    if not isinstance(inventory, list) or len(inventory) != 2 + report["pair_count"] * 2:
        fail("PAIR_INVENTORY_INVALID", "report invocation inventory count mismatch")
    inventory_fields = {
        "kind",
        "pair_ordinal",
        "position",
        "side",
        "raw_file",
        "stderr_file",
        "command_duration_ms",
        "binary_sha256",
        "host_observation",
    }
    try:
        for index, invocation in enumerate(inventory):
            raw_gate.require_exact_fields(invocation, inventory_fields, f"invocation {index + 1}")
            raw_gate.require_number(
                invocation["command_duration_ms"], f"invocation {index + 1} duration", positive=True
            )
            raw_gate.require_sha256(invocation["binary_sha256"], f"invocation {index + 1} binary")
            host = raw_gate.require_exact_fields(
                invocation["host_observation"], {"before", "after"}, f"invocation {index + 1} host"
            )
            for point in ("before", "after"):
                values = raw_gate.require_exact_fields(
                    host[point],
                    {"load_1m", "load_5m", "load_15m", "cpu_count"},
                    f"invocation {index + 1} host {point}",
                )
                for field in ("load_1m", "load_5m", "load_15m", "cpu_count"):
                    raw_gate.require_number(values[field], f"invocation {index + 1} {point} {field}")
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    expected_inventory_paths: list[tuple[str, str]] = []
    for position, side in enumerate(WARMUP_ORDER, start=1):
        raw_file = f"raw/warmup-{position:02d}-{side}.json"
        expected_inventory_paths.append(
            (raw_file, pathlib.PurePosixPath(raw_file).with_suffix(".stderr").as_posix())
        )
    for ordinal, pair_order in enumerate(measured_order(report["pair_count"]), start=1):
        for position, side in enumerate(pair_order, start=1):
            raw_file = f"raw/pair-{ordinal:02d}/{position:02d}-{side}.json"
            expected_inventory_paths.append(
                (raw_file, pathlib.PurePosixPath(raw_file).with_suffix(".stderr").as_posix())
            )
    for index, (invocation, expected_paths) in enumerate(
        zip(inventory, expected_inventory_paths), start=1
    ):
        raw_file = require_relative_artifact_path(
            invocation["raw_file"], f"invocation {index} raw file"
        ).as_posix()
        stderr_file = require_relative_artifact_path(
            invocation["stderr_file"], f"invocation {index} stderr file"
        ).as_posix()
        if (raw_file, stderr_file) != expected_paths:
            fail("PAIR_INVENTORY_INVALID", f"invocation {index} artifact path mismatch")
    for position, side in enumerate(WARMUP_ORDER, start=1):
        invocation = inventory[position - 1]
        if (
            invocation["kind"] != "warmup"
            or invocation["pair_ordinal"] is not None
            or invocation["position"] != position
            or invocation["side"] != side
        ):
            fail("PAIR_INVENTORY_INVALID", "warmup inventory mismatch")
    measured_inventory = [
        {
            "pair_ordinal": invocation["pair_ordinal"],
            "position": invocation["position"],
            "side": invocation["side"],
        }
        for invocation in inventory[2:]
    ]
    validate_pair_inventory(measured_inventory, report["pair_count"])
    for invocation in inventory:
        expected_hash = identity[f"{invocation['side']}_binary_sha256"]
        if invocation["binary_sha256"] != expected_hash:
            fail("BINARY_IDENTITY_INVALID", "invocation binary hash continuity failure")

    cases = report["cases"]
    if not isinstance(cases, list) or [case.get("case") for case in cases if isinstance(case, dict)] != list(ORDERED_CASES):
        fail("CONTRACT_INVALID", "paired report case set/order mismatch")
    any_unstable = False
    any_regression = False
    for case in cases:
        case_name = case["case"]
        if case_name not in PERFORMANCE_CASES:
            try:
                raw_gate.require_exact_fields(case, {"case", "performance_gated"}, f"case {case_name}")
            except raw_gate.GateError as exc:
                fail("CONTRACT_INVALID", str(exc))
            if case["performance_gated"] is not False:
                fail("CONTRACT_INVALID", f"case {case_name} performance policy mismatch")
            continue
        expected_case_fields = {
            "case",
            "performance_gated",
            "paired_ratios",
            "median_ratio",
            "regression_pct",
            "paired_mad_ratio_pct",
            "stability_boundary_pct",
            "threshold_pct",
            "candidate_throughput_mbps",
            "status",
        }
        try:
            raw_gate.require_exact_fields(case, expected_case_fields, f"case {case_name}")
            if not isinstance(case["paired_ratios"], list) or len(case["paired_ratios"]) != report["pair_count"]:
                fail("CONTRACT_INVALID", f"case {case_name} ratio count mismatch")
            ratio_values = [
                Decimal(str(raw_gate.require_number(value, f"case {case_name} ratio", positive=True)))
                for value in case["paired_ratios"]
            ]
            median_value = _decimal_median(ratio_values)
            mad_value = _decimal_median([abs(value - median_value) for value in ratio_values])
            expected_regression = (median_value - Decimal(1)) * Decimal(100)
            expected_mad = mad_value / median_value * Decimal(100)
            for field in ("median_ratio", "stability_boundary_pct", "candidate_throughput_mbps"):
                raw_gate.require_number(
                    case[field], f"case {case_name} {field}", positive=True
                )
            raw_gate.require_number(
                case["paired_mad_ratio_pct"], f"case {case_name} paired_mad_ratio_pct"
            )
            regression_number = case["regression_pct"]
            if (
                isinstance(regression_number, bool)
                or not isinstance(regression_number, (int, float))
                or not math.isfinite(float(regression_number))
            ):
                fail("CONTRACT_INVALID", f"case {case_name} regression_pct must be finite numeric")
        except raw_gate.GateError as exc:
            fail("CONTRACT_INVALID", str(exc))
        if not math.isclose(case["median_ratio"], float(median_value), rel_tol=1e-12, abs_tol=1e-12):
            fail("CONTRACT_INVALID", f"case {case_name} median ratio mismatch")
        if not math.isclose(case["regression_pct"], float(expected_regression), rel_tol=1e-12, abs_tol=1e-12):
            fail("CONTRACT_INVALID", f"case {case_name} regression mismatch")
        if not math.isclose(case["paired_mad_ratio_pct"], float(expected_mad), rel_tol=1e-12, abs_tol=1e-12):
            fail("CONTRACT_INVALID", f"case {case_name} paired MAD mismatch")
        if report["mode"] == "diagnostic":
            if case["threshold_pct"] is not None or case["stability_boundary_pct"] != 2.5:
                fail("CONTRACT_INVALID", f"case {case_name} diagnostic threshold fields mismatch")
            unstable = expected_mad > Decimal("2.5") or not Decimal("0.95") <= median_value <= Decimal("1.05")
            regression = False
        else:
            try:
                threshold = raw_gate.require_number(
                    case["threshold_pct"], f"case {case_name} threshold", positive=True
                )
            except raw_gate.GateError as exc:
                fail("CONTRACT_INVALID", str(exc))
            if threshold > 10:
                fail("CONTRACT_INVALID", f"case {case_name} threshold exceeds 10%")
            expected_boundary = min(3.0, threshold / 2.0)
            if case["stability_boundary_pct"] != expected_boundary:
                fail("CONTRACT_INVALID", f"case {case_name} stability boundary mismatch")
            unstable = expected_mad > Decimal(str(expected_boundary))
            regression = not unstable and expected_regression > Decimal(str(threshold))
        expected_status = "unstable" if unstable else "regression" if regression else "pass"
        if case["status"] != expected_status:
            fail("CONTRACT_INVALID", f"case {case_name} status mismatch")
        any_unstable = any_unstable or unstable
        any_regression = any_regression or regression

    expected_classification = (
        "BENCHMARK_ENVIRONMENT_UNSTABLE"
        if any_unstable
        else "PERFORMANCE_REGRESSION"
        if any_regression
        else "PASS"
    )
    if report["classification"] != expected_classification:
        fail("CONTRACT_INVALID", "paired report classification does not match case evidence")

    try:
        distributions = raw_gate.require_exact_fields(
            report["operational_counter_distributions"],
            {"reference", "candidate"},
            "operational distributions",
        )
        for side in ("reference", "candidate"):
            side_cases = raw_gate.require_exact_fields(
                distributions[side], set(ORDERED_CASES), f"{side} operational distributions"
            )
            for case_name in ORDERED_CASES:
                counters = raw_gate.require_exact_fields(
                    side_cases[case_name],
                    set(raw_gate.OPERATIONAL_COUNTER_FIELDS),
                    f"{side} {case_name} counters",
                )
                for field in raw_gate.OPERATIONAL_COUNTER_FIELDS:
                    summary = raw_gate.require_exact_fields(
                        counters[field], {"min", "max", "values"}, f"{side} {case_name} {field}"
                    )
                    if not isinstance(summary["values"], list) or not summary["values"]:
                        fail("CONTRACT_INVALID", f"{side} {case_name} {field} values are empty")
                    values = [
                        raw_gate.require_nonnegative_integer(value, f"{side} {case_name} {field}")
                        for value in summary["values"]
                    ]
                    if values != sorted(set(values)) or summary["min"] != min(values) or summary["max"] != max(values):
                        fail("CONTRACT_INVALID", f"{side} {case_name} {field} distribution mismatch")
        hard = raw_gate.require_exact_fields(
            report["hard_state_comparison"], {"status", "case_count"}, "hard-state comparison"
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if hard != {"status": "equal", "case_count": len(ORDERED_CASES)}:
        fail("CORRECTNESS_REGRESSION", "hard-state comparison is incomplete")
    try:
        cleanup = raw_gate.require_exact_fields(
            report["cleanup"], {"status", "attempted", "succeeded", "failed"}, "paired cleanup"
        )
        attempted = raw_gate.require_nonnegative_integer(cleanup["attempted"], "cleanup attempted")
        succeeded = raw_gate.require_nonnegative_integer(cleanup["succeeded"], "cleanup succeeded")
        failed_count = raw_gate.require_nonnegative_integer(cleanup["failed"], "cleanup failed")
    except raw_gate.GateError as exc:
        fail("EVIDENCE_INTEGRITY_FAILURE", str(exc))
    if cleanup["status"] != "complete" or failed_count != 0 or attempted != succeeded:
        fail("EVIDENCE_INTEGRITY_FAILURE", "paired cleanup is incomplete")
    expected_cleanup = (2 + report["pair_count"] * 2) * len(ORDERED_CASES)
    if attempted != expected_cleanup:
        fail("EVIDENCE_INTEGRITY_FAILURE", "paired cleanup inventory count mismatch")

    try:
        provenance = raw_gate.require_exact_fields(
            report["provenance"],
            {
                "event_name",
                "repository_id",
                "runner_os",
                "runner_image",
                "runner_arch",
                "cpu_count",
                "go_version",
                "postgres_version",
                "database_image_digest",
            },
            "paired provenance",
        )
    except raw_gate.GateError as exc:
        fail("CONTRACT_INVALID", str(exc))
    if any(provenance[field] in (None, "", "unknown") for field in provenance):
        fail("EXECUTION_CONTRACT_MISMATCH", "paired provenance is incomplete")
    validate_repository_id(provenance["repository_id"])
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(provenance["database_image_digest"])):
        fail("EXECUTION_CONTRACT_MISMATCH", "PostgreSQL image digest is invalid")
    if isinstance(provenance["cpu_count"], bool) or not isinstance(provenance["cpu_count"], int) or provenance["cpu_count"] <= 0:
        fail("EXECUTION_CONTRACT_MISMATCH", "provenance cpu_count is invalid")
    raw_gate.validate_no_sensitive_evidence(report, "paired comparison report")
    return report


def _expected_profile_artifact_files(report: dict[str, Any]) -> set[str]:
    expected = {"paired-comparison.json"}
    inventory_key = (
        "invocation_inventory" if report["status"] == "complete" else "attempted_invocations"
    )
    for index, invocation in enumerate(report[inventory_key], start=1):
        expected.add(
            require_relative_artifact_path(
                invocation["raw_file"], f"artifact invocation {index} raw file"
            ).as_posix()
        )
        expected.add(
            require_relative_artifact_path(
                invocation["stderr_file"], f"artifact invocation {index} stderr file"
            ).as_posix()
        )
    if report["status"] == "complete" and report["mode"] == "production":
        expected.update(
            {
                "governance/reference-manifest.json",
                "governance/threshold-policy.json",
            }
        )
    return expected


def _read_artifact_capture(path: pathlib.Path, label: str) -> None:
    try:
        value = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        fail("EVIDENCE_INTEGRITY_FAILURE", f"read {label}: {exc}")
    if "\x00" in value:
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} contains a NUL byte")
    if _capture_text_is_sensitive(value):
        fail("EVIDENCE_INTEGRITY_FAILURE", f"{label} contains sensitive content")


def validate_profile_artifact(
    directory: pathlib.Path, *, expected_profile: str
) -> dict[str, Any]:
    validate_checksums(directory)
    report_path = _contained_regular_file(
        directory, pathlib.PurePosixPath("paired-comparison.json"), "paired report"
    )
    try:
        report = validate_report_summary(
            load_json_strict(report_path), expected_profile=expected_profile
        )
    except raw_gate.GateError as exc:
        if isinstance(exc, PairedGateError):
            raise
        fail("CONTRACT_INVALID", str(exc))
    validate_checksums(
        directory, expected_files=_expected_profile_artifact_files(report)
    )
    if report["status"] == "failed":
        for index, invocation in enumerate(report["attempted_invocations"], start=1):
            for field in ("raw_file", "stderr_file"):
                relative = require_relative_artifact_path(
                    invocation[field], f"failure invocation {index} {field}"
                )
                path = _contained_regular_file(
                    directory, relative, f"failure invocation {index} {field}"
                )
                _read_artifact_capture(path, f"failure invocation {index} {field}")
        return report
    if report["mode"] != "production":
        fail(
            "REFERENCE_GOVERNANCE_INVALID",
            "decision input must be a governed production comparison",
        )
    if not PRODUCTION_SAMPLING_AUTHORIZED:
        fail(
            "REFERENCE_GOVERNANCE_INVALID",
            "production paired decisions are not authorized in this repository state",
        )

    repository = _repository_root()
    repository_manifest = _governed_repository_file(
        repository, GOVERNED_MANIFEST_RELATIVE, "governed reference manifest"
    )
    repository_thresholds = _governed_repository_file(
        repository, GOVERNED_THRESHOLD_RELATIVE, "governed threshold policy"
    )
    artifact_manifest = _contained_regular_file(
        directory,
        pathlib.PurePosixPath("governance/reference-manifest.json"),
        "artifact reference manifest",
    )
    artifact_thresholds = _contained_regular_file(
        directory,
        pathlib.PurePosixPath("governance/threshold-policy.json"),
        "artifact threshold policy",
    )
    if (
        _binary_hash(artifact_manifest) != _binary_hash(repository_manifest)
        or _binary_hash(artifact_thresholds) != _binary_hash(repository_thresholds)
    ):
        fail("REFERENCE_GOVERNANCE_INVALID", "artifact governance differs from repository")
    manifest = load_json_strict(artifact_manifest)
    policy = load_json_strict(artifact_thresholds)
    validate_reference_manifest(manifest)
    thresholds = validate_threshold_policy(policy)
    identity = report["identity"]
    verify_reference_governance(
        manifest,
        reference_sha=identity["reference_sha"],
        candidate_sha=identity["candidate_sha"],
        repository=repository,
    )
    governance = report["governance"]
    if (
        governance["manifest_sha256"] != _binary_hash(artifact_manifest)
        or governance["threshold_policy_id"] != policy["policy_id"]
        or governance["threshold_policy_sha256"] != _binary_hash(artifact_thresholds)
        or manifest["threshold_policy_id"] != policy["policy_id"]
        or manifest["threshold_policy_sha256"] != _binary_hash(artifact_thresholds)
    ):
        fail("REFERENCE_GOVERNANCE_INVALID", "artifact governance identity mismatch")

    warmups: list[dict[str, Any]] = []
    measured: list[dict[str, Any]] = []
    for invocation in report["invocation_inventory"]:
        raw_relative = require_relative_artifact_path(
            invocation["raw_file"], "artifact raw report"
        )
        stderr_relative = require_relative_artifact_path(
            invocation["stderr_file"], "artifact stderr"
        )
        raw_path = _contained_regular_file(directory, raw_relative, "artifact raw report")
        stderr_path = _contained_regular_file(directory, stderr_relative, "artifact stderr")
        try:
            envelope = load_json_strict(raw_path)
        except raw_gate.GateError as exc:
            fail("CONTRACT_INVALID", str(exc))
        _read_artifact_capture(stderr_path, "artifact stderr")
        record = {
            "kind": invocation["kind"],
            "pair_ordinal": invocation["pair_ordinal"],
            "position": invocation["position"],
            "side": invocation["side"],
            "envelope": envelope,
        }
        (warmups if invocation["kind"] == "warmup" else measured).append(record)

    profile = report["profile"]
    recomputed = compare_records(
        measured,
        pair_count=report["pair_count"],
        dataset=profile["dataset"],
        workers=profile["workers"],
        compression=profile["compression"],
        mode="production",
        thresholds=thresholds,
    )
    validate_warmups(
        warmups,
        measured,
        dataset=profile["dataset"],
        workers=profile["workers"],
        compression=profile["compression"],
    )
    for field in (
        "classification",
        "fixture",
        "cases",
        "operational_counter_distributions",
        "hard_state_comparison",
    ):
        if report[field] != recomputed[field]:
            fail("EVIDENCE_INTEGRITY_FAILURE", f"paired report {field} differs from raw evidence")
    return report


def decision_classification(classifications: list[str]) -> str:
    for classification in DECISION_PRECEDENCE:
        if classification in classifications:
            return classification
    fail("CONTRACT_INVALID", "decision contains no recognized classification")
    raise AssertionError("unreachable")


def decision_command(args: argparse.Namespace) -> int:
    profile_args: dict[str, pathlib.Path] = {}
    for value in args.profile:
        if "=" not in value:
            fail("CONTRACT_INVALID", "decision profile must be NAME=ARTIFACT_DIRECTORY")
        name, path = value.split("=", 1)
        if name in profile_args:
            fail("CONTRACT_INVALID", f"duplicate decision profile {name!r}")
        profile_args[name] = pathlib.Path(path)
    if set(profile_args) != set(PROFILE_MATRIX):
        fail("CONTRACT_INVALID", "decision requires exactly the four paired profiles")

    summaries = []
    common_identity: tuple[Any, ...] | None = None
    common_provenance: tuple[Any, ...] | None = None
    common_governance: tuple[Any, ...] | None = None
    for name in sorted(profile_args):
        directory = profile_args[name]
        report = validate_profile_artifact(directory, expected_profile=name)
        identity = report["identity"]
        identity_key = (
            identity["reference_sha"],
            identity["candidate_sha"],
            identity["reference_binary_sha256"],
            identity["candidate_binary_sha256"],
            report["contract_version"],
        )
        provenance = report["provenance"]
        provenance_key = (
            provenance["repository_id"],
            provenance["go_version"],
            provenance["postgres_version"],
            provenance["database_image_digest"],
        )
        governance = report.get("governance")
        governance_key = (
            governance["manifest_sha256"],
            governance["threshold_policy_id"],
            governance["threshold_policy_sha256"],
        ) if governance is not None else None
        if common_identity is None:
            common_identity = identity_key
            common_provenance = provenance_key
            common_governance = governance_key
        elif (
            identity_key != common_identity
            or provenance_key != common_provenance
            or governance_key != common_governance
        ):
            fail("EXECUTION_CONTRACT_MISMATCH", "profile source or environment pins differ")
        summaries.append({"profile": name, "classification": report["classification"]})

    classification = decision_classification([item["classification"] for item in summaries])
    decision = {
        "schema_version": 1,
        "evidence_policy_version": 2,
        "report_kind": DECISION_KIND,
        "status": "complete",
        "classification": classification,
        "contract_version": CONTRACT_VERSION,
        "identity": {
            "reference_sha": common_identity[0],
            "candidate_sha": common_identity[1],
        },
        "profiles": summaries,
        "matrix_coverage": sorted(PROFILE_MATRIX),
    }
    _create_output_directory(args.output_dir)
    raw_gate.write_json(args.output_dir / "paired-decision.json", decision)
    _write_checksums(args.output_dir)
    print(json.dumps({"classification": classification, "report": "paired-decision.json"}))
    return 0 if classification == "PASS" else 1


def _write_failure_artifact(args: argparse.Namespace, exc: PairedGateError) -> None:
    """Write a sanitized immutable summary without claiming partial evidence."""
    output_dir = args.output_dir
    if output_dir.is_symlink() or not output_dir.is_dir():
        raise PairedGateError(
            "EVIDENCE_INTEGRITY_FAILURE", "owned failure artifact directory is unavailable"
        )

    def available_hash(path: pathlib.Path) -> str | None:
        try:
            return _binary_hash(path) if path.is_file() else None
        except OSError:
            return None

    _sanitize_failure_captures(output_dir)
    attempted_invocations = []
    raw_root = output_dir / "raw"
    if raw_root.is_dir():
        for path in sorted(raw_root.rglob("*.json")):
            relative = path.relative_to(output_dir).as_posix()
            warmup_match = re.fullmatch(r"raw/warmup-(\d{2})-(reference|candidate)\.json", relative)
            pair_match = re.fullmatch(
                r"raw/pair-(\d{2})/(\d{2})-(reference|candidate)\.json", relative
            )
            if warmup_match:
                position, side = warmup_match.groups()
                attempted_invocations.append(
                    {
                        "kind": "warmup",
                        "pair_ordinal": None,
                        "position": int(position),
                        "side": side,
                        "raw_file": relative,
                        "stderr_file": path.with_suffix(".stderr").relative_to(output_dir).as_posix(),
                    }
                )
            elif pair_match:
                ordinal, position, side = pair_match.groups()
                attempted_invocations.append(
                    {
                        "kind": "measured",
                        "pair_ordinal": int(ordinal),
                        "position": int(position),
                        "side": side,
                        "raw_file": relative,
                        "stderr_file": path.with_suffix(".stderr").relative_to(output_dir).as_posix(),
                    }
                )
    attempted_invocations.sort(
        key=lambda item: (
            0 if item["kind"] == "warmup" else 1,
            item["pair_ordinal"] or 0,
            item["position"],
        )
    )

    report = {
        "schema_version": SCHEMA_VERSION,
        "evidence_policy_version": EVIDENCE_POLICY_VERSION,
        "report_kind": REPORT_KIND,
        "status": "failed",
        "mode": args.mode,
        "classification": exc.classification,
        "contract_version": CONTRACT_VERSION,
        "identity": {
            "reference_sha": args.reference_sha,
            "candidate_sha": args.candidate_sha,
            "reference_binary_sha256": available_hash(args.reference_binary),
            "candidate_binary_sha256": available_hash(args.candidate_binary),
        },
        "governance_status": (
            "provisional-diagnostic" if args.mode == "diagnostic" else "not-established"
        ),
        "profile": {
            "codec": "aes-gcm",
            "compression": args.compression,
            "dataset": args.dataset,
            "workers": args.workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "requested_pair_count": args.pairs,
        "warmup_order": list(WARMUP_ORDER),
        "measured_order": [list(pair) for pair in measured_order(args.pairs)],
        "attempted_invocations": attempted_invocations,
        "cleanup": {
            "status": "incomplete",
            "observed_invocations": len(attempted_invocations),
            "required_invocations": 2 + args.pairs * 2,
        },
        "provenance": _provenance(args),
    }
    raw_gate.write_json(output_dir / "paired-comparison.json", report)
    _write_checksums(output_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    sample = subparsers.add_parser("sample", help="run one paired benchmark profile")
    sample.add_argument("--reference-binary", type=pathlib.Path, required=True)
    sample.add_argument("--candidate-binary", type=pathlib.Path, required=True)
    sample.add_argument("--reference-sha")
    sample.add_argument("--candidate-sha", required=True)
    sample.add_argument("--output-dir", type=pathlib.Path, required=True)
    sample.add_argument("--dataset", choices=sorted(FIXTURES), required=True)
    sample.add_argument("--compression", choices=("none", "zstd"), required=True)
    sample.add_argument("--workers", type=int, choices=(1, 4), required=True)
    sample.add_argument("--mode", choices=("diagnostic", "production"), required=True)
    sample.add_argument("--pairs", type=int, choices=(5, 10), required=True)
    sample.add_argument("--command-timeout-seconds", type=int, default=600)
    sample.add_argument("--go-version", required=True)
    sample.add_argument("--postgres-version", required=True)
    sample.add_argument("--database-image-digest", required=True)
    sample.set_defaults(handler=sample_command)

    decision = subparsers.add_parser("decision", help="combine four immutable profile artifacts")
    decision.add_argument("--profile", action="append", required=True)
    decision.add_argument("--output-dir", type=pathlib.Path, required=True)
    decision.set_defaults(handler=decision_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_owned = False
    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def handle_sigterm(_signum: int, _frame: Any) -> None:
        raise PairedGateError(
            "CI_INFRASTRUCTURE_TIMEOUT", "sampling terminated before fixed inventory completed"
        )

    try:
        if args.command == "sample":
            _create_output_directory(args.output_dir)
            output_owned = True
            signal.signal(signal.SIGTERM, handle_sigterm)
        return args.handler(args)
    except PairedGateError as exc:
        if args.command == "sample" and output_owned:
            _write_failure_artifact(args, exc)
        print(json.dumps({"classification": exc.classification, "error": str(exc)}), file=sys.stderr)
        return 2
    except raw_gate.GateError as exc:
        paired_exc = PairedGateError("CONTRACT_INVALID", str(exc))
        if args.command == "sample" and output_owned:
            _write_failure_artifact(args, paired_exc)
        print(json.dumps({"classification": "CONTRACT_INVALID", "error": str(exc)}), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        paired_exc = PairedGateError(
            "PAIR_INVENTORY_INVALID", "sampling interrupted before fixed inventory completed"
        )
        if args.command == "sample" and output_owned:
            _write_failure_artifact(args, paired_exc)
        print(
            json.dumps({"classification": paired_exc.classification, "error": str(paired_exc)}),
            file=sys.stderr,
        )
        return 2
    finally:
        if args.command == "sample":
            signal.signal(signal.SIGTERM, previous_sigterm)


if __name__ == "__main__":
    raise SystemExit(main())
