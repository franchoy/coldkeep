#!/usr/bin/env python3
"""Contract tests for scripts/paired_benchmark_gate.py."""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from unittest import mock

SCRIPT_DIR = pathlib.Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
MODULE_PATH = SCRIPT_DIR / "paired_benchmark_gate.py"
SPEC = importlib.util.spec_from_file_location("paired_benchmark_gate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def fixture(dataset: str = "ci-paired-w1-v1") -> dict:
    expected = {key: value for key, value in gate.FIXTURES[dataset].items() if key != "workers"}
    return {
        **expected,
        "ordered_cases": [
            {"name": name, "seed": 1712 + index * 10}
            for index, name in enumerate(gate.ORDERED_CASES)
        ],
    }


def diagnostic_state(*, restored: bool = False) -> dict:
    digest = "d" * 64
    return {
        "schema_version": 2,
        "active_logical_namespace": {"count": 1, "total_bytes": 1024, "sha256": digest},
        "logical_catalog": {"count": 1, "total_bytes": 1024, "sha256": digest},
        "logical_statuses": {"completed": 1, "processing": 0, "aborted": 0},
        "chunk_graph": {"count": 1, "total_bytes": 1024, "sha256": digest},
        "restored_tree": {
            "count": 1 if restored else 0,
            "total_bytes": 1024 if restored else 0,
            "sha256": digest,
        },
        "snapshots": {"count": 0, "total_bytes": 0, "sha256": digest},
        "snapshot_count": 0,
        "gc": {
            "total_chunks": 1,
            "reachable_chunks": 1,
            "unreachable_chunks": 0,
            "logically_reclaimable_bytes": 0,
            "physically_reclaimable_bytes": 0,
            "packed_blocks_live": 1,
            "packed_blocks_dead": 0,
            "packed_bytes_live": 1024,
            "packed_bytes_reclaimable": 0,
            "retained_dead_bytes": 0,
        },
        "verification": {
            "blocks_checked": 1,
            "physical_hashes_checked": 1,
            "compressed_hashes_checked": 0,
            "logical_hashes_checked": 1,
            "compressed_blocks_checked": 0,
            "physical_file_issues": 0,
            "snapshot_membership_rows": 0,
            "snapshot_reachability_issues": 0,
        },
        "physical": {
            "container_count": 1,
            "storage_block_count": 1,
            "legacy_block_count": 0,
            "chunk_reference_count": 1,
            "payload_bytes": 1024,
            "container_bytes": 1088,
            "canonical_sha256": digest,
        },
        "physical_layout_sha256": "e" * 64,
    }


def raw_row(case_name: str, *, workers: int, duration_ms: float) -> dict:
    return {
        "case": case_name,
        "duration_ms": duration_ms,
        "throughput_mbps": 1024 / (1024 * 1024) / (duration_ms / 1000),
        "execution": {
            "store_folder_workers": workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "execution_stats": {
            "total_files": 1,
            "total_bytes": 1024,
            "workers_used": workers,
            "container_append_count": 1,
            "fsync_count": 1,
            "container_open_count": 1,
            "container_close_count": 1,
            "io": {
                "container_opens": 1,
                "container_appends": 1,
                "fsyncs": 1,
                "bytes_written": 1024,
                "bytes_read": 0,
            },
        },
        "diagnostic_final_state": diagnostic_state(
            restored=case_name in {"restore-large-file", "restore-many-files"}
        ),
    }


def recompute_top(report: dict) -> None:
    rows = report["data"]["rows"]
    report["data"]["execution_stats"] = {
        "total_files": sum(row["execution_stats"]["total_files"] for row in rows),
        "total_bytes": sum(row["execution_stats"]["total_bytes"] for row in rows),
        "workers_used": report["data"]["execution"]["store_folder_workers"],
        "container_append_count": sum(
            row["execution_stats"]["container_append_count"] for row in rows
        ),
        "fsync_count": sum(row["execution_stats"]["fsync_count"] for row in rows),
        "container_open_count": sum(
            row["execution_stats"]["container_open_count"] for row in rows
        ),
        "container_close_count": sum(
            row["execution_stats"]["container_close_count"] for row in rows
        ),
        "snapshot_metadata_write_count": 0,
        "io": {
            field: sum(row["execution_stats"]["io"][field] for row in rows)
            for field in gate.raw_gate.IO_COUNTER_FIELDS
        },
    }


def raw_report(
    *, dataset: str = "ci-paired-w1-v1", workers: int = 1, duration_ms: float = 1000
) -> dict:
    rows = [raw_row(case_name, workers=workers, duration_ms=duration_ms) for case_name in gate.ORDERED_CASES]
    report = {
        "status": "ok",
        "command": "benchmark",
        "data": {
            "schema_version": 2,
            "generated_at_utc": "2026-07-27T00:00:00Z",
            "dataset": dataset,
            "repeat": 1,
            "fixture": fixture(dataset),
            "execution": {
                "store_folder_workers": workers,
                "pipeline_depth": 1,
                "deterministic": True,
            },
            "execution_stats": {},
            "rows": rows,
        },
    }
    recompute_top(report)
    gate.validate_raw_report(report, dataset=dataset, workers=workers, compression="none")
    return report


def records_for_ratios(
    ratios: list[float], *, dataset: str = "ci-paired-w1-v1", workers: int = 1
) -> list[dict]:
    records = []
    for ordinal, pair_order in enumerate(gate.measured_order(len(ratios)), start=1):
        for position, side in enumerate(pair_order, start=1):
            duration = 1000 if side == "reference" else 1000 * ratios[ordinal - 1]
            records.append(
                {
                    "pair_ordinal": ordinal,
                    "position": position,
                    "side": side,
                    "envelope": raw_report(
                        dataset=dataset, workers=workers, duration_ms=duration
                    ),
                }
            )
    return records


def thresholds(value: float = 5.0) -> dict[str, float]:
    return {case_name: value for case_name in gate.PERFORMANCE_CASES}


def manifest(reference_sha: str = "a" * 40) -> dict:
    return {
        "schema_version": 1,
        "report_kind": gate.REFERENCE_MANIFEST_KIND,
        "release_train": "v1.13",
        "reference_sha": reference_sha,
        "approval": {"kind": "trusted_tag", "value": "v1.13.11"},
        "contract_version": gate.CONTRACT_VERSION,
        "raw_schema_version": 2,
        "diagnostic_schema_version": 2,
        "fixtures": sorted(gate.FIXTURES),
        "ordered_cases": list(gate.ORDERED_CASES),
        "performance_cases": list(gate.PERFORMANCE_CASES),
        "execution_order": {
            "warmups": list(gate.WARMUP_ORDER),
            "measured_pairs": [list(pair) for pair in gate.FIVE_PAIR_ORDER],
        },
        "pair_count": 5,
        "threshold_policy_id": "paired-v1-test",
        "threshold_policy_sha256": "b" * 64,
    }


def invocation_inventory(pair_count: int) -> list[dict]:
    items = []
    for position, side in enumerate(gate.WARMUP_ORDER, start=1):
        items.append(invocation("warmup", None, position, side))
    for ordinal, pair_order in enumerate(gate.measured_order(pair_count), start=1):
        for position, side in enumerate(pair_order, start=1):
            items.append(invocation("measured", ordinal, position, side))
    return items


def invocation(kind: str, ordinal: int | None, position: int, side: str) -> dict:
    if kind == "warmup":
        raw_file = f"raw/warmup-{position:02d}-{side}.json"
    else:
        raw_file = f"raw/pair-{ordinal:02d}/{position:02d}-{side}.json"
    return {
        "kind": kind,
        "pair_ordinal": ordinal,
        "position": position,
        "side": side,
        "raw_file": raw_file,
        "stderr_file": pathlib.PurePosixPath(raw_file).with_suffix(".stderr").as_posix(),
        "command_duration_ms": 1000,
        "binary_sha256": ("c" if side == "reference" else "d") * 64,
        "host_observation": {
            "before": {"load_1m": 0, "load_5m": 0, "load_15m": 0, "cpu_count": 4},
            "after": {"load_1m": 0, "load_5m": 0, "load_15m": 0, "cpu_count": 4},
        },
    }


def report_summary(
    *, profile: str = "none-w1", classification: str = "PASS", mode: str = "production"
) -> dict:
    compression, workers, dataset = gate.PROFILE_MATRIX[profile]
    pair_count = 5 if mode == "production" else 10
    cases = []
    for case_name in gate.ORDERED_CASES:
        if case_name not in gate.PERFORMANCE_CASES:
            cases.append({"case": case_name, "performance_gated": False})
        else:
            cases.append(
                {
                    "case": case_name,
                    "performance_gated": True,
                    "paired_ratios": [1.0] * pair_count,
                    "median_ratio": 1.0,
                    "regression_pct": 0.0,
                    "paired_mad_ratio_pct": 0.0,
                    "stability_boundary_pct": 2.5,
                    "threshold_pct": 5.0 if mode == "production" else None,
                    "candidate_throughput_mbps": 1.0,
                    "status": "pass",
                }
            )
    distributions = {
        side: {
            case_name: {
                field: {"min": 0, "max": 0, "values": [0]}
                for field in gate.raw_gate.OPERATIONAL_COUNTER_FIELDS
            }
            for case_name in gate.ORDERED_CASES
        }
        for side in ("reference", "candidate")
    }
    return {
        "schema_version": 1,
        "evidence_policy_version": 2,
        "report_kind": gate.REPORT_KIND,
        "status": "complete",
        "mode": mode,
        "classification": classification,
        "contract_version": gate.CONTRACT_VERSION,
        "identity": {
            "reference_sha": "a" * 40,
            "candidate_sha": "b" * 40,
            "reference_binary_sha256": "c" * 64,
            "candidate_binary_sha256": "d" * 64,
        },
        "governance": (
            {
                "status": "governed",
                "manifest_sha256": "e" * 64,
                "threshold_policy_id": "paired-v1-test",
                "threshold_policy_sha256": "f" * 64,
            }
            if mode == "production"
            else {
                "status": "provisional-diagnostic",
                "manifest_sha256": None,
                "threshold_policy_id": None,
                "threshold_policy_sha256": None,
            }
        ),
        "profile": {
            "codec": "aes-gcm",
            "compression": compression,
            "dataset": dataset,
            "workers": workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "fixture": fixture(dataset),
        "warmup_order": list(gate.WARMUP_ORDER),
        "measured_order": [list(pair) for pair in gate.measured_order(pair_count)],
        "pair_count": pair_count,
        "invocation_inventory": invocation_inventory(pair_count),
        "cases": cases,
        "operational_counter_distributions": distributions,
        "hard_state_comparison": {"status": "equal", "case_count": 9},
        "cleanup": {
            "status": "complete",
            "attempted": (2 + pair_count * 2) * len(gate.ORDERED_CASES),
            "succeeded": (2 + pair_count * 2) * len(gate.ORDERED_CASES),
            "failed": 0,
        },
        "provenance": {
            "event_name": "pull_request",
            "repository_id": "owner/coldkeep",
            "runner_os": "Linux",
            "runner_image": "image",
            "runner_arch": "X64",
            "cpu_count": 4,
            "go_version": "go1.25.1",
            "postgres_version": "16.14",
            "database_image_digest": "sha256:" + "1" * 64,
        },
    }


def write_summary_artifact(directory: pathlib.Path, report: dict) -> None:
    directory.mkdir()
    for invocation_record in report["invocation_inventory"]:
        raw_path = directory / invocation_record["raw_file"]
        stderr_path = directory / invocation_record["stderr_file"]
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text("{}\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
    gate.raw_gate.write_json(directory / "paired-comparison.json", report)
    gate._write_checksums(directory)


def write_governed_repository(repository: pathlib.Path) -> None:
    governance = repository / "benchmarks" / "paired"
    governance.mkdir(parents=True)
    policy = {
        "schema_version": 1,
        "report_kind": gate.THRESHOLD_POLICY_KIND,
        "contract_version": gate.CONTRACT_VERSION,
        "policy_id": "paired-v1-test",
        "cases": thresholds(5),
    }
    policy_path = governance / "threshold-policy-v1.13.json"
    gate.raw_gate.write_json(policy_path, policy)
    governed_manifest = manifest("a" * 40)
    governed_manifest["approval"] = {"kind": "reviewed_record", "value": "test-approval"}
    governed_manifest["threshold_policy_sha256"] = gate._binary_hash(policy_path)
    gate.raw_gate.write_json(governance / "reference-v1.13.json", governed_manifest)


def write_complete_profile_artifact(
    directory: pathlib.Path, repository: pathlib.Path, *, profile: str = "none-w1"
) -> None:
    compression, workers, dataset = gate.PROFILE_MATRIX[profile]
    measured = records_for_ratios([1.0] * 5, dataset=dataset, workers=workers)
    warmups = [
        {
            "kind": "warmup",
            "pair_ordinal": None,
            "position": position,
            "side": side,
            "envelope": raw_report(dataset=dataset, workers=workers),
        }
        for position, side in enumerate(gate.WARMUP_ORDER, start=1)
    ]
    comparison = gate.compare_records(
        measured,
        pair_count=5,
        dataset=dataset,
        workers=workers,
        compression=compression,
        mode="production",
        thresholds=thresholds(5),
    )
    report = report_summary(profile=profile)
    report["fixture"] = comparison["fixture"]
    report["cases"] = comparison["cases"]
    report["operational_counter_distributions"] = comparison[
        "operational_counter_distributions"
    ]
    report["hard_state_comparison"] = comparison["hard_state_comparison"]

    directory.mkdir()
    artifact_governance = directory / "governance"
    artifact_governance.mkdir()
    repository_governance = repository / "benchmarks" / "paired"
    shutil.copyfile(
        repository_governance / "reference-v1.13.json",
        artifact_governance / "reference-manifest.json",
    )
    shutil.copyfile(
        repository_governance / "threshold-policy-v1.13.json",
        artifact_governance / "threshold-policy.json",
    )
    report["governance"] = {
        "status": "governed",
        "manifest_sha256": gate._binary_hash(artifact_governance / "reference-manifest.json"),
        "threshold_policy_id": "paired-v1-test",
        "threshold_policy_sha256": gate._binary_hash(
            artifact_governance / "threshold-policy.json"
        ),
    }

    observations = warmups + measured
    for invocation_record, observation in zip(report["invocation_inventory"], observations):
        raw_path = directory / invocation_record["raw_file"]
        stderr_path = directory / invocation_record["stderr_file"]
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        gate.raw_gate.write_json(raw_path, observation["envelope"])
        stderr_path.write_text("", encoding="utf-8")
    gate.raw_gate.write_json(directory / "paired-comparison.json", report)
    gate._write_checksums(directory)


class PairedContractTests(unittest.TestCase):
    def test_fixture_registry_and_performance_case_list_are_exact(self) -> None:
        self.assertEqual(
            gate.FIXTURES,
            {
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
            },
        )
        self.assertEqual(
            gate.PERFORMANCE_CASES,
            (
                "store-large-file",
                "store-many-small-files",
                "restore-many-files",
                "snapshot-creation",
                "gc-after-churn",
                "stats-inspect",
                "verify-system-deep",
            ),
        )
        self.assertEqual(
            set(gate.ORDERED_CASES) - set(gate.PERFORMANCE_CASES),
            {"store-mixed-dataset", "restore-large-file"},
        )

    def test_governed_artifact_names(self) -> None:
        self.assertEqual(
            gate.profile_artifact_name(
                candidate_sha="b" * 40,
                reference_sha="a" * 40,
                compression="zstd",
                workers=4,
                attempt=2,
            ),
            "benchmark-paired-bbbbbbbbbbbb-against-aaaaaaaaaaaa-zstd-w4-a2",
        )
        self.assertEqual(
            gate.decision_artifact_name(
                candidate_sha="b" * 40, reference_sha="a" * 40, attempt=2
            ),
            "benchmark-paired-bbbbbbbbbbbb-against-aaaaaaaaaaaa-decision-a2",
        )

    def test_fixed_warmup_and_pair_orders(self) -> None:
        self.assertEqual(gate.WARMUP_ORDER, ("candidate", "reference"))
        self.assertEqual(
            gate.measured_order(5),
            (
                ("reference", "candidate"),
                ("candidate", "reference"),
                ("candidate", "reference"),
                ("reference", "candidate"),
                ("reference", "candidate"),
            ),
        )
        self.assertEqual(
            gate.measured_order(10),
            (
                ("reference", "candidate"),
                ("candidate", "reference"),
                ("candidate", "reference"),
                ("reference", "candidate"),
                ("reference", "candidate"),
                ("candidate", "reference"),
                ("reference", "candidate"),
                ("reference", "candidate"),
                ("candidate", "reference"),
                ("candidate", "reference"),
            ),
        )
        self.assertEqual(
            [pair[0] for pair in gate.measured_order(10)].count("reference"), 5
        )
        self.assertEqual(
            [pair[0] for pair in gate.measured_order(10)].count("candidate"), 5
        )
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.measured_order(6)
        self.assertEqual(caught.exception.classification, "PAIR_INVENTORY_INVALID")

    def test_warmups_are_ordered_and_participate_in_hard_state_validation(self) -> None:
        measured = records_for_ratios([1.0] * 5)
        warmups = [
            {
                "kind": "warmup",
                "pair_ordinal": None,
                "position": position,
                "side": side,
                "envelope": raw_report(),
            }
            for position, side in enumerate(gate.WARMUP_ORDER, start=1)
        ]
        gate.validate_warmups(
            warmups,
            measured,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
        )
        warmups[0]["envelope"]["data"]["rows"][0]["diagnostic_final_state"][
            "active_logical_namespace"
        ]["sha256"] = "a" * 64
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.validate_warmups(
                warmups,
                measured,
                dataset="ci-paired-w1-v1",
                workers=1,
                compression="none",
            )
        self.assertEqual(caught.exception.classification, "EVIDENCE_INTEGRITY_FAILURE")

    def test_paired_ratio_uses_pairwise_values(self) -> None:
        result = gate.compare_records(
            records_for_ratios([1.00, 1.01, 1.02, 1.03, 2.00]),
            pair_count=5,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="production",
            thresholds=thresholds(5),
        )
        case = result["cases"][0]
        self.assertEqual(case["paired_ratios"], [1.0, 1.01, 1.02, 1.03, 2.0])
        self.assertEqual(case["median_ratio"], 1.02)
        self.assertAlmostEqual(case["regression_pct"], 2.0)
        self.assertAlmostEqual(case["paired_mad_ratio_pct"], 0.01 / 1.02 * 100)

    def test_exact_threshold_passes_and_above_fails(self) -> None:
        exact = gate.compare_records(
            records_for_ratios([1.05] * 5),
            pair_count=5,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="production",
            thresholds=thresholds(5),
        )
        self.assertEqual(exact["classification"], "PASS")
        above = gate.compare_records(
            records_for_ratios([1.05001] * 5),
            pair_count=5,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="production",
            thresholds=thresholds(5),
        )
        self.assertEqual(above["classification"], "PERFORMANCE_REGRESSION")

    def test_instability_precedes_regression(self) -> None:
        result = gate.compare_records(
            records_for_ratios([1.0, 1.05, 1.1, 1.15, 1.2]),
            pair_count=5,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="production",
            thresholds=thresholds(5),
        )
        self.assertEqual(result["classification"], "BENCHMARK_ENVIRONMENT_UNSTABLE")

    def test_diagnostic_mad_exact_boundary_passes_and_above_fails(self) -> None:
        exact_ratios = [0.95, 0.95, 0.975, 0.975, 1.0, 1.0, 1.025, 1.025, 1.05, 1.05]
        exact = gate.compare_records(
            records_for_ratios(exact_ratios),
            pair_count=10,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="diagnostic",
        )
        self.assertEqual(exact["classification"], "PASS")
        self.assertEqual(exact["cases"][0]["paired_mad_ratio_pct"], 2.5)

        above_ratios = [0.948, 0.948, 0.974, 0.974, 1.0, 1.0, 1.026, 1.026, 1.052, 1.052]
        above = gate.compare_records(
            records_for_ratios(above_ratios),
            pair_count=10,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="diagnostic",
        )
        self.assertEqual(above["classification"], "BENCHMARK_ENVIRONMENT_UNSTABLE")
        self.assertGreater(above["cases"][0]["paired_mad_ratio_pct"], 2.5)

    def test_diagnostic_requires_ten_pairs_and_five_percent_signal(self) -> None:
        with self.assertRaises(gate.PairedGateError):
            gate.compare_records(
                records_for_ratios([1.0] * 5),
                pair_count=5,
                dataset="ci-paired-w1-v1",
                workers=1,
                compression="none",
                mode="diagnostic",
            )
        result = gate.compare_records(
            records_for_ratios([1.051] * 10),
            pair_count=10,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="diagnostic",
        )
        self.assertEqual(result["classification"], "BENCHMARK_ENVIRONMENT_UNSTABLE")

    def test_missing_duplicate_and_reordered_pairs_fail_closed(self) -> None:
        base = records_for_ratios([1.0] * 5)
        for mutated in (base[:-1], base[:1] + base[:1] + base[2:], list(reversed(base))):
            with self.assertRaises(gate.PairedGateError) as caught:
                gate.compare_records(
                    mutated,
                    pair_count=5,
                    dataset="ci-paired-w1-v1",
                    workers=1,
                    compression="none",
                    mode="production",
                    thresholds=thresholds(),
                )
            self.assertEqual(caught.exception.classification, "PAIR_INVENTORY_INVALID")

    def test_layout_and_counter_variation_are_permitted(self) -> None:
        records = records_for_ratios([1.0] * 5)
        for record in records:
            if record["side"] != "candidate":
                continue
            for row in record["envelope"]["data"]["rows"]:
                state = row["diagnostic_final_state"]
                state["physical"]["container_count"] = 2
                state["physical"]["container_bytes"] = 2048
                state["physical_layout_sha256"] = "a" * 64
                stats = row["execution_stats"]
                stats["container_append_count"] = 2
                stats["container_open_count"] = 2
                stats["container_close_count"] = 2
                stats["fsync_count"] = 2
                stats["io"].update(
                    {"container_opens": 2, "container_appends": 2, "fsyncs": 2, "bytes_written": 2048}
                )
            recompute_top(record["envelope"])
        result = gate.compare_records(
            records,
            pair_count=5,
            dataset="ci-paired-w1-v1",
            workers=1,
            compression="none",
            mode="production",
            thresholds=thresholds(),
        )
        self.assertEqual(result["classification"], "PASS")

    def test_canonical_content_mismatch_is_correctness_regression(self) -> None:
        records = records_for_ratios([1.0] * 5)
        for record in records:
            if record["side"] == "candidate":
                record["envelope"]["data"]["rows"][0]["diagnostic_final_state"]["physical"][
                    "canonical_sha256"
                ] = "a" * 64
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.compare_records(
                records,
                pair_count=5,
                dataset="ci-paired-w1-v1",
                workers=1,
                compression="none",
                mode="production",
                thresholds=thresholds(),
            )
        self.assertEqual(caught.exception.classification, "CORRECTNESS_REGRESSION")

    def test_internal_hard_state_drift_is_evidence_failure(self) -> None:
        records = records_for_ratios([1.0] * 5)
        candidate = next(record for record in records[2:] if record["side"] == "candidate")
        candidate["envelope"]["data"]["rows"][0]["diagnostic_final_state"][
            "active_logical_namespace"
        ]["sha256"] = "a" * 64
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.compare_records(
                records,
                pair_count=5,
                dataset="ci-paired-w1-v1",
                workers=1,
                compression="none",
                mode="production",
                thresholds=thresholds(),
            )
        self.assertEqual(caught.exception.classification, "EVIDENCE_INTEGRITY_FAILURE")

    def test_schema_unknown_field_and_fixture_mismatch_rejected(self) -> None:
        report = raw_report()
        report["data"]["new_field"] = 1
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.validate_raw_report(report, dataset="ci-paired-w1-v1", workers=1, compression="none")
        self.assertEqual(caught.exception.classification, "CONTRACT_INVALID")

        report = raw_report()
        report["data"]["fixture"]["mixed_file_count"] += 1
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.validate_raw_report(report, dataset="ci-paired-w1-v1", workers=1, compression="none")
        self.assertEqual(caught.exception.classification, "EXECUTION_CONTRACT_MISMATCH")

        for invalid_duration in (0, -1, float("inf"), float("nan")):
            with self.subTest(duration=invalid_duration):
                report = raw_report()
                report["data"]["rows"][0]["duration_ms"] = invalid_duration
                with self.assertRaises(gate.PairedGateError) as caught:
                    gate.validate_raw_report(
                        report,
                        dataset="ci-paired-w1-v1",
                        workers=1,
                        compression="none",
                    )
                self.assertEqual(caught.exception.classification, "CONTRACT_INVALID")

    def test_threshold_policy_is_complete_and_no_broader_than_ten_percent(self) -> None:
        policy = {
            "schema_version": 1,
            "report_kind": gate.THRESHOLD_POLICY_KIND,
            "contract_version": gate.CONTRACT_VERSION,
            "policy_id": "test",
            "cases": thresholds(10),
        }
        self.assertEqual(gate.validate_threshold_policy(policy), thresholds(10))
        policy["cases"][gate.PERFORMANCE_CASES[0]] = 10.001
        with self.assertRaises(gate.PairedGateError):
            gate.validate_threshold_policy(policy)

    def test_reference_manifest_and_candidate_governance(self) -> None:
        gate.validate_reference_manifest(manifest())
        changed = ["cmd/coldkeep/main.go", "benchmarks/paired/reference-v1.13.json"]
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.reject_candidate_governance_changes(changed)
        self.assertEqual(caught.exception.classification, "REFERENCE_GOVERNANCE_INVALID")
        invalid = manifest()
        invalid["reference_sha"] = "v1.13.11"
        with self.assertRaises(gate.PairedGateError):
            gate.validate_reference_manifest(invalid)

    def test_production_sampling_is_disabled_and_rejects_cli_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            reference = root / "reference"
            candidate = root / "candidate"
            reference.write_bytes(b"same")
            candidate.write_bytes(b"same")
            base = [
                "sample",
                "--reference-binary",
                str(reference),
                "--candidate-binary",
                str(candidate),
                "--candidate-sha",
                "b" * 40,
                "--dataset",
                "ci-paired-w1-v1",
                "--compression",
                "none",
                "--workers",
                "1",
                "--mode",
                "production",
                "--pairs",
                "5",
                "--go-version",
                "go1.25.12",
                "--postgres-version",
                "16.14",
                "--database-image-digest",
                "sha256:" + "1" * 64,
            ]
            with mock.patch.dict(gate.os.environ, {"COLDKEEP_CODEC": "aes-gcm"}, clear=False):
                output = root / "disabled"
                exit_code = gate.main([*base, "--output-dir", str(output)])
                self.assertEqual(exit_code, 2)
                report = gate.load_json_strict(output / "paired-comparison.json")
                self.assertEqual(report["classification"], "REFERENCE_GOVERNANCE_INVALID")
                self.assertEqual(report["governance_status"], "not-established")

                supplied = root / "supplied"
                exit_code = gate.main(
                    [
                        *base,
                        "--reference-sha",
                        "a" * 40,
                        "--output-dir",
                        str(supplied),
                    ]
                )
                self.assertEqual(exit_code, 2)
                report = gate.load_json_strict(supplied / "paired-comparison.json")
                self.assertEqual(report["classification"], "REFERENCE_GOVERNANCE_INVALID")

    def test_reference_reachability_and_ancestry(self) -> None:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, text=True, capture_output=True
        ).stdout.strip()
        governed = manifest(head)
        governed["approval"] = {"kind": "reviewed_record", "value": "phase11-test"}
        gate.verify_reference_governance(
            governed,
            reference_sha=head,
            candidate_sha=head,
            repository=SCRIPT_DIR.parent,
        )

    def test_binary_identity_and_timeout_classifications(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            binary = root / "coldkeep"
            binary.write_bytes(b"binary")
            with self.assertRaises(gate.PairedGateError) as caught:
                gate._capture(
                    binary=binary,
                    expected_hash="0" * 64,
                    side="candidate",
                    output_dir=root,
                    relative_raw_path=pathlib.Path("raw/candidate.json"),
                    dataset="ci-paired-w1-v1",
                    workers=1,
                    compression="none",
                    timeout_seconds=1,
                )
            self.assertEqual(caught.exception.classification, "BINARY_IDENTITY_INVALID")

            digest = gate._binary_hash(binary)
            for side, expected in (
                ("candidate", "CANDIDATE_TIMEOUT_INCONCLUSIVE"),
                ("reference", "CI_INFRASTRUCTURE_TIMEOUT"),
            ):
                with mock.patch.object(
                    gate.subprocess, "run", side_effect=subprocess.TimeoutExpired(["coldkeep"], 1)
                ):
                    with self.assertRaises(gate.PairedGateError) as caught:
                        gate._capture(
                            binary=binary,
                            expected_hash=digest,
                            side=side,
                            output_dir=root,
                            relative_raw_path=pathlib.Path(f"raw/{side}.json"),
                            dataset="ci-paired-w1-v1",
                            workers=1,
                            compression="none",
                            timeout_seconds=1,
                        )
                self.assertEqual(caught.exception.classification, expected)

    def test_functional_failure_classification_is_side_specific(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            binary = root / "coldkeep"
            binary.write_bytes(b"binary")
            digest = gate._binary_hash(binary)
            completed = subprocess.CompletedProcess(["coldkeep"], 2, stdout="", stderr="failed")
            for side, expected in (
                ("candidate", "CANDIDATE_FUNCTIONAL_FAILURE"),
                ("reference", "REFERENCE_FUNCTIONAL_FAILURE"),
            ):
                with mock.patch.object(gate.subprocess, "run", return_value=completed):
                    with self.assertRaises(gate.PairedGateError) as caught:
                        gate._capture(
                            binary=binary,
                            expected_hash=digest,
                            side=side,
                            output_dir=root,
                            relative_raw_path=pathlib.Path(f"raw/{side}.json"),
                            dataset="ci-paired-w1-v1",
                            workers=1,
                            compression="none",
                            timeout_seconds=1,
                        )
                self.assertEqual(caught.exception.classification, expected)

    def test_cli_failure_still_writes_immutable_profile_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = pathlib.Path(temp) / "artifact"
            exit_code = gate.main(
                [
                    "sample",
                    "--reference-binary",
                    str(pathlib.Path(temp) / "missing-reference"),
                    "--candidate-binary",
                    str(pathlib.Path(temp) / "missing-candidate"),
                    "--reference-sha",
                    "a" * 40,
                    "--candidate-sha",
                    "b" * 40,
                    "--output-dir",
                    str(output),
                    "--dataset",
                    "ci-paired-w1-v1",
                    "--compression",
                    "none",
                    "--workers",
                    "1",
                    "--mode",
                    "diagnostic",
                    "--pairs",
                    "5",
                    "--go-version",
                    "go1.25.12",
                    "--postgres-version",
                    "16.14",
                    "--database-image-digest",
                    "sha256:" + "1" * 64,
                ]
            )
            self.assertEqual(exit_code, 2)
            gate.validate_checksums(output)
            report = gate.raw_gate.load_json_strict(output / "paired-comparison.json")
            validated = gate.validate_report_summary(report, expected_profile="none-w1")
            self.assertEqual(validated["classification"], "PAIR_INVENTORY_INVALID")

    def test_sigterm_and_manual_interrupt_write_non_performance_failures(self) -> None:
        base = [
            "sample",
            "--reference-binary",
            "/nonexistent/reference",
            "--candidate-binary",
            "/nonexistent/candidate",
            "--reference-sha",
            "a" * 40,
            "--candidate-sha",
            "b" * 40,
            "--dataset",
            "ci-paired-w1-v1",
            "--compression",
            "none",
            "--workers",
            "1",
            "--mode",
            "diagnostic",
            "--pairs",
            "10",
            "--go-version",
            "go1.25.12",
            "--postgres-version",
            "16.14",
            "--database-image-digest",
            "sha256:" + "1" * 64,
        ]
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)

            def terminate(_args: argparse.Namespace) -> int:
                gate.os.kill(gate.os.getpid(), gate.signal.SIGTERM)
                return 0

            terminated = root / "terminated"
            with mock.patch.object(gate, "sample_command", side_effect=terminate):
                self.assertEqual(
                    gate.main([*base, "--output-dir", str(terminated)]), 2
                )
            report = gate.load_json_strict(terminated / "paired-comparison.json")
            self.assertEqual(report["classification"], "CI_INFRASTRUCTURE_TIMEOUT")
            self.assertEqual(report["cleanup"]["status"], "incomplete")

            interrupted = root / "interrupted"
            with mock.patch.object(gate, "sample_command", side_effect=KeyboardInterrupt):
                self.assertEqual(
                    gate.main([*base, "--output-dir", str(interrupted)]), 2
                )
            report = gate.load_json_strict(interrupted / "paired-comparison.json")
            self.assertEqual(report["classification"], "PAIR_INVENTORY_INVALID")

    def test_cleanup_and_report_unknown_field_fail_closed(self) -> None:
        report = report_summary()
        report["cleanup"]["failed"] = 1
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.validate_report_summary(report, expected_profile="none-w1")
        self.assertEqual(caught.exception.classification, "EVIDENCE_INTEGRITY_FAILURE")
        report = report_summary()
        report["unknown"] = True
        with self.assertRaises(gate.PairedGateError) as caught:
            gate.validate_report_summary(report, expected_profile="none-w1")
        self.assertEqual(caught.exception.classification, "CONTRACT_INVALID")

    def test_complete_report_accepts_improvement_and_negative_delta(self) -> None:
        report = report_summary()
        for case in report["cases"]:
            if not case["performance_gated"]:
                continue
            case["paired_ratios"] = [0.9] * 5
            case["median_ratio"] = 0.9
            case["regression_pct"] = -10.0
        gate.validate_report_summary(report, expected_profile="none-w1")

    def test_checksum_inventory_detects_tampering_and_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root / "paired-comparison.json").write_text("{}\n", encoding="utf-8")
            gate._write_checksums(root)
            gate.validate_checksums(root)
            (root / "paired-comparison.json").write_text("{\"changed\":true}\n", encoding="utf-8")
            with self.assertRaises(gate.PairedGateError) as caught:
                gate.validate_checksums(root)
            self.assertEqual(caught.exception.classification, "EVIDENCE_INTEGRITY_FAILURE")

    def test_strict_json_rejects_duplicate_keys_and_trailing_envelopes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            duplicate = root / "duplicate.json"
            duplicate.write_text('{"status":"ok","status":"pass"}\n', encoding="utf-8")
            with self.assertRaises(gate.raw_gate.GateError):
                gate.load_json_strict(duplicate)
            trailing = root / "trailing.json"
            trailing.write_text('{}\n{}\n', encoding="utf-8")
            with self.assertRaises(gate.raw_gate.GateError):
                gate.load_json_strict(trailing)

    def test_checksum_contract_rejects_symlinks_traversal_and_unexpected_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp) / "artifact"
            root.mkdir()
            (root / "paired-comparison.json").write_text("{}\n", encoding="utf-8")
            gate._write_checksums(root)
            gate.validate_checksums(root, expected_files={"paired-comparison.json"})

            (root / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
            gate._write_checksums(root)
            with self.assertRaises(gate.PairedGateError):
                gate.validate_checksums(root, expected_files={"paired-comparison.json"})
            (root / "unexpected.txt").unlink()

            outside = pathlib.Path(temp) / "outside.txt"
            outside.write_text("outside\n", encoding="utf-8")
            (root / "linked.txt").symlink_to(outside)
            with self.assertRaises(gate.PairedGateError):
                gate._write_checksums(root)
            (root / "linked.txt").unlink()

            digest = gate._binary_hash(root / "paired-comparison.json")
            (root / "checksums.sha256").write_text(
                f"{digest}  ../paired-comparison.json\n", encoding="utf-8"
            )
            with self.assertRaises(gate.PairedGateError):
                gate.validate_checksums(root)

            report = report_summary()
            report["invocation_inventory"][0]["raw_file"] = "../escape.json"
            with self.assertRaises(gate.PairedGateError):
                gate.validate_report_summary(report, expected_profile="none-w1")

    def test_governed_and_output_paths_reject_symlink_redirection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            repository = root / "repository"
            governed = repository / "benchmarks" / "paired"
            governed.mkdir(parents=True)
            outside = root / "outside.json"
            outside.write_text("{}\n", encoding="utf-8")
            (governed / "reference-v1.13.json").symlink_to(outside)
            with self.assertRaises(gate.PairedGateError) as caught:
                gate._governed_repository_file(
                    repository,
                    gate.GOVERNED_MANIFEST_RELATIVE,
                    "governed reference manifest",
                )
            self.assertEqual(caught.exception.classification, "REFERENCE_GOVERNANCE_INVALID")

            output_target = root / "output-target"
            output_target.mkdir()
            output_link = root / "output-link"
            output_link.symlink_to(output_target, target_is_directory=True)
            with self.assertRaises(gate.PairedGateError):
                gate._create_output_directory(output_link)

    def test_captured_output_rejects_or_redacts_sensitive_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            raw = root / "raw"
            raw.mkdir()
            capture = raw / "failure.stderr"
            capture.write_text(
                "connection failed: postgresql://user:secret@localhost/private\n",
                encoding="utf-8",
            )
            with self.assertRaises(gate.PairedGateError) as caught:
                gate._read_artifact_capture(capture, "test stderr")
            self.assertEqual(caught.exception.classification, "EVIDENCE_INTEGRITY_FAILURE")

            gate._sanitize_failure_captures(root)
            self.assertEqual(
                capture.read_text(encoding="utf-8"),
                "[captured output omitted: sensitive content]\n",
            )
            gate._read_artifact_capture(capture, "redacted stderr")

    def test_decision_rejects_diagnostic_only_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            profiles = []
            for name in gate.PROFILE_MATRIX:
                directory = root / name
                write_summary_artifact(
                    directory, report_summary(profile=name, mode="diagnostic")
                )
                profiles.append(f"{name}={directory}")
            output = root / "decision"
            exit_code = gate.main(
                ["decision", *sum((["--profile", value] for value in profiles), []), "--output-dir", str(output)]
            )
            self.assertEqual(exit_code, 2)
            self.assertFalse(output.exists())

    def test_complete_artifact_is_recomputed_from_raw_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            repository = root / "repository"
            repository.mkdir()
            write_governed_repository(repository)
            artifact = root / "artifact"
            write_complete_profile_artifact(artifact, repository)
            with (
                mock.patch.object(gate, "PRODUCTION_SAMPLING_AUTHORIZED", True),
                mock.patch.object(gate, "_repository_root", return_value=repository),
                mock.patch.object(gate, "verify_reference_governance"),
            ):
                gate.validate_profile_artifact(artifact, expected_profile="none-w1")

                raw_path = artifact / "raw" / "pair-01" / "02-candidate.json"
                raw = gate.load_json_strict(raw_path)
                row = raw["data"]["rows"][0]
                row["duration_ms"] = 1100
                row["throughput_mbps"] = 1024 / (1024 * 1024) / 1.1
                gate.raw_gate.write_json(raw_path, raw)
                gate._write_checksums(artifact)
                with self.assertRaises(gate.PairedGateError) as caught:
                    gate.validate_profile_artifact(artifact, expected_profile="none-w1")
            self.assertEqual(caught.exception.classification, "EVIDENCE_INTEGRITY_FAILURE")

    def test_decision_rejects_missing_duplicate_and_cross_profile_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            with self.assertRaises(gate.PairedGateError):
                gate.decision_command(
                    argparse.Namespace(profile=[f"none-w1={root}"], output_dir=root / "out")
                )
            with self.assertRaises(gate.PairedGateError):
                gate.decision_command(
                    argparse.Namespace(
                        profile=[f"none-w1={root}", f"none-w1={root}"],
                        output_dir=root / "out",
                    )
                )

            reports = [report_summary(profile=name) for name in sorted(gate.PROFILE_MATRIX)]
            reports[1]["identity"]["candidate_sha"] = "c" * 40
            with mock.patch.object(gate, "validate_profile_artifact", side_effect=reports):
                with self.assertRaises(gate.PairedGateError) as caught:
                    gate.decision_command(
                        argparse.Namespace(
                            profile=[f"{name}={root / name}" for name in sorted(gate.PROFILE_MATRIX)],
                            output_dir=root / "out",
                        )
                    )
            self.assertEqual(caught.exception.classification, "EXECUTION_CONTRACT_MISMATCH")

            for field, value in (
                ("manifest_sha256", "1" * 64),
                ("threshold_policy_id", "different-policy"),
                ("threshold_policy_sha256", "2" * 64),
            ):
                with self.subTest(governance_field=field):
                    reports = [
                        report_summary(profile=name)
                        for name in sorted(gate.PROFILE_MATRIX)
                    ]
                    reports[3]["governance"][field] = value
                    with mock.patch.object(
                        gate, "validate_profile_artifact", side_effect=reports
                    ):
                        with self.assertRaises(gate.PairedGateError) as caught:
                            gate.decision_command(
                                argparse.Namespace(
                                    profile=[
                                        f"{name}={root / name}"
                                        for name in sorted(gate.PROFILE_MATRIX)
                                    ],
                                    output_dir=root / "out",
                                )
                            )
                    self.assertEqual(
                        caught.exception.classification,
                        "EXECUTION_CONTRACT_MISMATCH",
                    )

            reports = [report_summary(profile=name) for name in sorted(gate.PROFILE_MATRIX)]
            reports[2]["identity"]["candidate_binary_sha256"] = "0" * 64
            with mock.patch.object(gate, "validate_profile_artifact", side_effect=reports):
                with self.assertRaises(gate.PairedGateError) as caught:
                    gate.decision_command(
                        argparse.Namespace(
                            profile=[
                                f"{name}={root / name}"
                                for name in sorted(gate.PROFILE_MATRIX)
                            ],
                            output_dir=root / "out",
                        )
                    )
            self.assertEqual(caught.exception.classification, "EXECUTION_CONTRACT_MISMATCH")

    def test_decision_precedence_and_matrix_coverage(self) -> None:
        self.assertEqual(
            gate.decision_classification(["PASS", "PERFORMANCE_REGRESSION", "CORRECTNESS_REGRESSION"]),
            "CORRECTNESS_REGRESSION",
        )
        self.assertEqual(
            gate.decision_classification(["PERFORMANCE_REGRESSION", "CANDIDATE_TIMEOUT_INCONCLUSIVE"]),
            "CANDIDATE_TIMEOUT_INCONCLUSIVE",
        )
        for profile in gate.PROFILE_MATRIX:
            gate.validate_report_summary(report_summary(profile=profile), expected_profile=profile)

    def test_historical_absolute_fixture_has_no_paired_authority(self) -> None:
        self.assertNotIn("ci-stable-v1", gate.FIXTURES)
        self.assertNotIn("ci-stable-v1", {profile[2] for profile in gate.PROFILE_MATRIX.values()})


if __name__ == "__main__":
    unittest.main()
