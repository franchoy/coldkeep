#!/usr/bin/env python3
"""Focused contract tests for scripts/benchmark_gate.py."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import pathlib
import tempfile
import unittest
from copy import deepcopy

MODULE_PATH = pathlib.Path(__file__).with_name("benchmark_gate.py")
SPEC = importlib.util.spec_from_file_location("benchmark_gate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def fixture() -> dict:
    return {
        **gate.FIXTURE_FIELDS,
        "ordered_cases": [
            {"name": name, "seed": 1712 + index * 10}
            for index, name in enumerate(gate.EXPECTED_CASES)
        ],
    }


def diagnostic_final_state() -> dict:
    digest = "d" * 64
    return {
        "schema_version": 1,
        "logical_files": {"count": 1, "total_bytes": 1024, "sha256": digest},
        "logical_statuses": {"completed": 1, "processing": 0, "aborted": 0},
        "chunk_graph": {"count": 1, "total_bytes": 1024, "sha256": digest},
        "restored_tree": {"count": 0, "total_bytes": 0, "sha256": digest},
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


def aggregate(
    durations: list[float] | None = None,
    *,
    source: str = "a" * 40,
    runner_image: str = "image-a",
) -> dict:
    durations = durations or [5000, 5000, 5000, 5000, 5000]
    cases = []
    for index, name in enumerate(gate.EXPECTED_CASES):
        summary = gate.summarize(durations)
        logical_bytes = 1024 * (index + 1)
        cases.append(
            {
                "case": name,
                "seed": 1712 + index * 10,
                "logical_files": index + 1,
                "logical_bytes": logical_bytes,
                "sample_durations_ms": list(durations),
                "diagnostic_final_state": diagnostic_final_state(),
                "fixture_stats": {
                    "execution": {
                        "store_folder_workers": 4,
                        "pipeline_depth": 1,
                        "deterministic": True,
                    },
                    "execution_stats": {
                        "total_files": index + 1,
                        "total_bytes": logical_bytes,
                        "workers_used": 4,
                        "io": {
                            "container_opens": index,
                            "container_appends": index,
                            "fsyncs": index,
                            "bytes_written": logical_bytes,
                            "bytes_read": 0,
                        },
                    },
                },
                **summary,
                "throughput_mbps": logical_bytes
                / (1024 * 1024)
                / (summary["median_duration_ms"] / 1000),
            }
        )
    return {
        "schema_version": 2,
        "report_kind": gate.REPORT_KIND,
        "status": "ok",
        "provenance": {
            "source_commit": source,
            "source_tag": None,
            "generated_at_utc": "2026-07-25T00:00:00Z",
            "workflow_run_id": "1",
            "workflow_job_id": "benchmark",
            "workflow_run_attempt": "1",
            "runner_os": "Linux",
            "runner_image": runner_image,
            "runner_arch": "X64",
            "cpu_count": 4,
            "go_version": "go version go1.25.12 linux/amd64",
            "postgres_version": "PostgreSQL 16.14",
            "database_image_digest": "sha256:" + "b" * 64,
            "binary_sha256": "c" * 64,
        },
        "profile": {
            "codec": "aes-gcm",
            "compression": "none",
            "dataset": gate.FIXTURE_ID,
            "workers": 4,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "fixture": fixture(),
        "warmup_count": 1,
        "sample_count": len(durations),
        "sample_order": list(range(1, len(durations) + 1)),
        "command_durations_ms": [10000] * len(durations),
        "command_p95_ms": 10000,
        "host_observations": [],
        "cases": cases,
    }


def set_case_durations(report: dict, case_name: str, durations: list[float]) -> None:
    case = next(item for item in report["cases"] if item["case"] == case_name)
    summary = gate.summarize(durations)
    case["sample_durations_ms"] = list(durations)
    case.update(summary)
    case["throughput_mbps"] = (
        case["logical_bytes"]
        / (1024 * 1024)
        / (summary["median_duration_ms"] / 1000)
    )


class StatisticsTests(unittest.TestCase):
    def test_median_mad_cv_and_retained_outlier(self) -> None:
        result = gate.summarize([100, 100, 100, 100, 1000])
        self.assertEqual(result["median_duration_ms"], 100)
        self.assertEqual(result["mad_ms"], 0)
        self.assertGreater(result["coefficient_of_variation_pct"], 0)

    def test_percentile_nearest_rank(self) -> None:
        self.assertEqual(gate.percentile_nearest_rank([5, 1, 4, 2, 3], 0.95), 5)

    def test_empty_and_non_finite_samples_fail(self) -> None:
        with self.assertRaises(gate.GateError):
            gate.summarize([])
        with self.assertRaises(gate.GateError):
            gate.summarize([math.inf])


class StrictEvidenceTests(unittest.TestCase):
    def test_diagnostic_schema_excludes_sensitive_fields(self) -> None:
        state = diagnostic_final_state()
        gate.validate_diagnostic_final_state(state, "test")
        for forbidden in ("dsn", "password", "username", "database_name", "temporary_path"):
            mutated = deepcopy(state)
            mutated[forbidden] = "secret"
            with self.assertRaisesRegex(gate.GateError, "fields mismatch"):
                gate.validate_diagnostic_final_state(mutated, "test")

    def test_hard_final_state_is_separate_from_operational_counters(self) -> None:
        row = aggregate()["cases"][0]
        as_raw_row = {
            "case": row["case"],
            "diagnostic_final_state": deepcopy(row["diagnostic_final_state"]),
            **deepcopy(row["fixture_stats"]),
        }
        counter_variant = deepcopy(as_raw_row)
        counter_variant["execution_stats"]["fsync_count"] = 1
        self.assertEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(counter_variant))
        self.assertNotEqual(gate.fixture_stats(as_raw_row), gate.fixture_stats(counter_variant))

        layout_variant = deepcopy(as_raw_row)
        layout_variant["diagnostic_final_state"]["physical"]["container_count"] += 1
        layout_variant["diagnostic_final_state"]["physical"]["container_bytes"] += 64
        layout_variant["diagnostic_final_state"]["physical_layout_sha256"] = "a" * 64
        self.assertEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(layout_variant))

        state_variant = deepcopy(as_raw_row)
        state_variant["diagnostic_final_state"]["logical_files"]["sha256"] = "f" * 64
        self.assertNotEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(state_variant))
        self.assertEqual(gate.fixture_stats(as_raw_row), gate.fixture_stats(state_variant))

    def test_trailing_and_repeated_json_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "input.json")
            path.write_text("{}\n{}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "trailing"):
                gate.load_json_strict(path)
            path.write_text("{", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "malformed"):
                gate.load_json_strict(path)

    def test_non_finite_json_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "input.json")
            path.write_text('{"value": NaN}', encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "non-finite"):
                gate.load_json_strict(path)

    def test_legacy_and_empty_reports_fail(self) -> None:
        with self.assertRaises(gate.GateError):
            gate.validate_aggregate({"data": {"rows": []}}, require_gate_count=True)
        report = aggregate()
        report["cases"] = []
        with self.assertRaisesRegex(gate.GateError, "non-empty"):
            gate.validate_aggregate(report, require_gate_count=True)
        for field, value in (
            ("schema_version", 1),
            ("report_kind", "wrong"),
            ("status", "failed"),
        ):
            report = aggregate()
            report[field] = value
            with self.assertRaisesRegex(gate.GateError, "schema/report kind/status"):
                gate.validate_aggregate(report, require_gate_count=True)

    def test_duplicate_missing_and_wrong_order_fail(self) -> None:
        for mutate in (
            lambda report: report["cases"].pop(),
            lambda report: report["cases"].__setitem__(1, report["cases"][0]),
            lambda report: report["cases"].reverse(),
            lambda report: report["cases"][0].__setitem__("case", "unexpected-case"),
        ):
            report = aggregate()
            mutate(report)
            with self.assertRaisesRegex(gate.GateError, "case set/order"):
                gate.validate_aggregate(report, require_gate_count=True)

    def test_fixed_sample_order_and_count_are_required(self) -> None:
        report = aggregate()
        report["sample_order"] = [2, 1, 3, 4, 5]
        with self.assertRaisesRegex(gate.GateError, "sample order"):
            gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        report["sample_count"] = 3
        with self.assertRaisesRegex(gate.GateError, "one warmup and five"):
            gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        report["warmup_count"] = 2
        with self.assertRaisesRegex(gate.GateError, "one warmup and five"):
            gate.validate_aggregate(report, require_gate_count=True)

    def test_derived_throughput_and_statistics_are_recomputed(self) -> None:
        report = aggregate()
        report["cases"][0]["throughput_mbps"] *= 2
        with self.assertRaisesRegex(gate.GateError, "throughput"):
            gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        report["cases"][0]["seed"] += 1
        with self.assertRaisesRegex(gate.GateError, "seed mismatch"):
            gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        report["cases"][0]["logical_files"] += 1
        with self.assertRaisesRegex(gate.GateError, "logical totals"):
            gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        del report["provenance"]["workflow_run_attempt"]
        with self.assertRaisesRegex(gate.GateError, "workflow_run_attempt"):
            gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        report["fixture"]["many_small_file_count"] += 1
        with self.assertRaisesRegex(gate.GateError, "many_small_file_count"):
            gate.validate_aggregate(report, require_gate_count=True)

    def test_functional_sample_failure_is_not_aggregated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "coldkeep"
            binary.write_text("#!/usr/bin/env sh\nexit 7\n", encoding="utf-8")
            binary.chmod(0o700)
            args = argparse.Namespace(
                binary=binary,
                dataset=gate.FIXTURE_ID,
                workers=4,
                compression="none",
            )
            with self.assertRaisesRegex(gate.GateError, "exit 7"):
                gate.capture_sample(
                    args,
                    root / "sample.json",
                    gate.sha256_file(binary),
                )

        report = aggregate()
        report["cases"][0]["median_duration_ms"] += 1
        with self.assertRaisesRegex(gate.GateError, "statistic"):
            gate.validate_aggregate(report, require_gate_count=True)


class ComparisonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.thresholds = self.root / "thresholds.yaml"
        self.thresholds.write_text(
            """
defaults:
  uncompressed:
    duration_regression_pct: 5
  compressed:
    duration_regression_warning_pct: 15
per_case_overrides:
  uncompressed:
    snapshot-creation:
      duration_regression_pct: 3
  compressed:
    store-many-small-files:
      duration_regression_warning_pct: 20
""".lstrip(),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def run_compare(self, baseline: dict, candidate: dict) -> tuple[int, dict]:
        baseline_path = self.root / "baseline.json"
        candidate_path = self.root / "candidate.json"
        output_path = self.root / "comparison.json"
        gate.write_json(baseline_path, baseline)
        gate.write_json(candidate_path, candidate)
        code = gate.compare_command(
            argparse.Namespace(
                baseline=baseline_path,
                candidate=candidate_path,
                thresholds=self.thresholds,
                mode="uncompressed",
                manifest=None,
                output=output_path,
            )
        )
        return code, gate.load_json_strict(output_path)

    def test_exact_threshold_passes_and_above_threshold_fails(self) -> None:
        candidate = aggregate()
        set_case_durations(candidate, "store-large-file", [5250] * 5)
        code, report = self.run_compare(aggregate(), candidate)
        self.assertEqual(code, 0)
        store = next(item for item in report["outcomes"] if item["case"] == "store-large-file")
        self.assertEqual(store["classification"], "pass")

        candidate = aggregate()
        set_case_durations(candidate, "store-large-file", [5251] * 5)
        code, report = self.run_compare(aggregate(), candidate)
        self.assertEqual(code, 1)
        store = next(item for item in report["outcomes"] if item["case"] == "store-large-file")
        self.assertEqual(store["classification"], "PERFORMANCE_REGRESSION")

    def test_high_variability_precedes_regression(self) -> None:
        candidate = aggregate([5000, 5000, 5300, 5600, 5900])
        code, report = self.run_compare(aggregate(), candidate)
        self.assertEqual(code, 1)
        self.assertTrue(
            all(item["classification"] == "BENCHMARK_UNSTABLE" for item in report["outcomes"])
        )

    def test_one_extreme_outlier_is_retained_without_moving_median_or_mad(self) -> None:
        code, report = self.run_compare(aggregate(), aggregate([5000, 5000, 5000, 5000, 9000]))
        self.assertEqual(code, 0)
        self.assertTrue(all(item["classification"] == "pass" for item in report["outcomes"]))

    def test_environment_and_fixture_mismatch_fail_closed(self) -> None:
        baseline = aggregate()
        candidate = aggregate()
        candidate["provenance"]["go_version"] = "go version go1.25.13 linux/amd64"
        with self.assertRaisesRegex(gate.GateError, "go_version"):
            self.run_compare(baseline, candidate)

        candidate = aggregate()
        candidate["cases"][0]["fixture_stats"]["execution_stats"]["total_files"] += 1
        with self.assertRaises(gate.GateError):
            self.run_compare(baseline, candidate)

    def test_runner_image_drift_is_warning_only(self) -> None:
        code, report = self.run_compare(
            aggregate(runner_image="image-a"),
            aggregate(runner_image="image-b"),
        )
        self.assertEqual(code, 0)
        self.assertEqual(report["warnings"], ["resolved runner image differs from baseline"])


class ManifestTests(unittest.TestCase):
    def test_manifest_hash_validation_and_stale_hash_failure(self) -> None:
        with tempfile.TemporaryDirectory(dir=pathlib.Path.cwd()) as directory:
            root = pathlib.Path(directory)
            thresholds = root / "thresholds.yaml"
            thresholds.write_text("thresholds\n", encoding="utf-8")
            baselines = []
            for profile, (compression, workers) in gate.MANIFEST_PROFILES.items():
                path = root / f"baseline-{profile}.json"
                report = aggregate()
                report["profile"]["compression"] = compression
                report["profile"]["workers"] = workers
                for case in report["cases"]:
                    case["fixture_stats"]["execution"]["store_folder_workers"] = workers
                    case["fixture_stats"]["execution_stats"]["workers_used"] = workers
                gate.write_json(path, report)
                baselines.append(
                    f"{profile}={path.relative_to(pathlib.Path.cwd())}"
                )
            manifest = root / "manifest.json"
            code = gate.manifest_command(
                argparse.Namespace(
                    baseline=baselines,
                    thresholds=thresholds.relative_to(pathlib.Path.cwd()),
                    output=manifest,
                )
            )
            self.assertEqual(code, 0)
            self.assertEqual(
                gate.validate_manifest_command(argparse.Namespace(manifest=manifest)),
                0,
            )
            pathlib.Path(baselines[0].split("=", 1)[1]).write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "hash mismatch"):
                gate.validate_manifest_command(argparse.Namespace(manifest=manifest))


class CalibrationTests(unittest.TestCase):
    def test_fixed_matrix_passes_and_short_case_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            thresholds = root / "thresholds.yaml"
            thresholds.write_text(
                """
defaults:
  uncompressed:
    duration_regression_pct: 5
  compressed:
    duration_regression_warning_pct: 15
per_case_overrides:
  uncompressed:
    snapshot-creation:
      duration_regression_pct: 3
  compressed:
    store-many-small-files:
      duration_regression_warning_pct: 20
""".lstrip(),
                encoding="utf-8",
            )
            inputs = []
            for compression in ("none", "zstd"):
                for workers in (1, 4):
                    for replicate in (1, 2):
                        report = aggregate([5000] * 10)
                        report["profile"]["compression"] = compression
                        report["profile"]["workers"] = workers
                        for case in report["cases"]:
                            case["fixture_stats"]["execution"]["store_folder_workers"] = workers
                            case["fixture_stats"]["execution_stats"]["workers_used"] = workers
                        path = root / f"{compression}-w{workers}-r{replicate}.json"
                        gate.write_json(path, report)
                        inputs.append(f"{compression}-w{workers}-r{replicate}={path}")
            output = root / "calibration.json"
            code = gate.calibration_command(
                argparse.Namespace(
                    aggregate=inputs,
                    thresholds=thresholds,
                    output=output,
                )
            )
            self.assertEqual(code, 0)

            first_path = pathlib.Path(inputs[0].split("=", 1)[1])
            report = gate.load_json_strict(first_path)
            set_case_durations(report, "store-large-file", [4999] * 10)
            gate.write_json(first_path, report)
            code = gate.calibration_command(
                argparse.Namespace(
                    aggregate=inputs,
                    thresholds=thresholds,
                    output=output,
                )
            )
            self.assertEqual(code, 1)
            self.assertTrue(gate.load_json_strict(output)["failures"])

            # The three inclusive acceptance boundaries pass exactly.
            report = gate.load_json_strict(first_path)
            set_case_durations(report, "store-large-file", [5000] * 10)
            report["command_durations_ms"] = [120000] * 10
            report["command_p95_ms"] = 120000
            gate.write_json(first_path, report)
            second_path = pathlib.Path(inputs[1].split("=", 1)[1])
            second = gate.load_json_strict(second_path)
            set_case_durations(second, "store-large-file", [5250] * 10)
            gate.write_json(second_path, second)
            code = gate.calibration_command(
                argparse.Namespace(aggregate=inputs, thresholds=thresholds, output=output)
            )
            self.assertEqual(code, 0)

            # Strictly exceeding either 120 seconds or 5% fails.
            report["command_durations_ms"][-1] = 120001
            report["command_p95_ms"] = 120001
            gate.write_json(first_path, report)
            code = gate.calibration_command(
                argparse.Namespace(aggregate=inputs, thresholds=thresholds, output=output)
            )
            self.assertEqual(code, 1)
            report["command_durations_ms"][-1] = 120000
            report["command_p95_ms"] = 120000
            gate.write_json(first_path, report)
            set_case_durations(second, "store-large-file", [5251] * 10)
            gate.write_json(second_path, second)
            code = gate.calibration_command(
                argparse.Namespace(aggregate=inputs, thresholds=thresholds, output=output)
            )
            self.assertEqual(code, 1)

            # Fixed odd/even partitions are evaluated independently.
            set_case_durations(second, "store-large-file", [5000, 5000, 5200, 5000, 5400, 5000, 5600, 5000, 5800, 5000])
            gate.write_json(second_path, second)
            code = gate.calibration_command(
                argparse.Namespace(aggregate=inputs, thresholds=thresholds, output=output)
            )
            self.assertEqual(code, 1)
            self.assertTrue(
                any("odd five-sample MAD ratio" in item for item in gate.load_json_strict(output)["failures"])
            )


if __name__ == "__main__":
    unittest.main()
