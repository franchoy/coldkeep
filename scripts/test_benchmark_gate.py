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
from unittest import mock

MODULE_PATH = pathlib.Path(__file__).with_name("benchmark_gate.py")
SPEC = importlib.util.spec_from_file_location("benchmark_gate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def fixture(dataset: str = gate.FIXTURE_ID) -> dict:
    return {
        **gate.fixture_fields(dataset),
        "ordered_cases": [
            {"name": name, "seed": 1712 + index * 10}
            for index, name in enumerate(gate.EXPECTED_CASES)
        ],
    }


def diagnostic_final_state(
    *,
    logical_files: int = 1,
    logical_bytes: int = 1024,
    restored_files: int = 0,
) -> dict:
    digest = "d" * 64
    return {
        "schema_version": gate.DIAGNOSTIC_SCHEMA_VERSION,
        "active_logical_namespace": {
            "count": logical_files,
            "total_bytes": logical_bytes,
            "sha256": digest,
        },
        "logical_catalog": {"count": logical_files, "total_bytes": logical_bytes, "sha256": digest},
        "logical_statuses": {"completed": logical_files, "processing": 0, "aborted": 0},
        "chunk_graph": {"count": logical_files, "total_bytes": logical_bytes, "sha256": digest},
        "restored_tree": {
            "count": restored_files,
            "total_bytes": logical_bytes if restored_files else 0,
            "sha256": digest,
        },
        "snapshots": {"count": 0, "total_bytes": 0, "sha256": digest},
        "snapshot_count": 0,
        "gc": {
            "total_chunks": logical_files,
            "reachable_chunks": logical_files,
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
            "chunk_reference_count": logical_files,
            "payload_bytes": logical_bytes,
            "container_bytes": logical_bytes + 64,
            "canonical_sha256": digest,
        },
        "physical_layout_sha256": "e" * 64,
    }


def operational_sample(index: int = 0) -> dict[str, int]:
    opens = index + 1
    return {
        "container_append_count": index + 1,
        "container_open_count": opens,
        "container_close_count": opens,
        "fsync_count": index + 1,
        "bytes_written": 1024 * (index + 1),
        "bytes_read": 0,
        "snapshot_metadata_write_count": 0,
    }


def raw_row(case: str = "store-large-file", *, workers: int = 4) -> dict:
    counters = operational_sample()
    return {
        "case": case,
        "duration_ms": 1000,
        "throughput_mbps": 1024 / (1024 * 1024),
        "execution": {
            "store_folder_workers": workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "execution_stats": {
            "total_files": 1,
            "total_bytes": 1024,
            "workers_used": workers,
            "container_append_count": counters["container_append_count"],
            "container_open_count": counters["container_open_count"],
            "container_close_count": counters["container_close_count"],
            "fsync_count": counters["fsync_count"],
            "io": {
                "container_opens": counters["container_open_count"],
                "container_appends": counters["container_append_count"],
                "fsyncs": counters["fsync_count"],
                "bytes_written": counters["bytes_written"],
                "bytes_read": counters["bytes_read"],
            },
        },
        "diagnostic_final_state": diagnostic_final_state(
            restored_files=1 if case in {"restore-large-file", "restore-many-files"} else 0
        ),
    }


def raw_report(
    *, workers: int = 4, compression: str = "none", dataset: str = gate.FIXTURE_ID
) -> dict:
    rows = [raw_row(case, workers=workers) for case in gate.EXPECTED_CASES]
    total_io = {
        field: sum(row["execution_stats"]["io"][field] for row in rows)
        for field in gate.IO_COUNTER_FIELDS
    }
    data = {
        "schema_version": gate.SCHEMA_VERSION,
        "generated_at_utc": "2026-07-25T00:00:00Z",
        "dataset": dataset,
        "repeat": 1,
        "fixture": fixture(dataset),
        "execution": {
            "store_folder_workers": workers,
            "pipeline_depth": 1,
            "deterministic": True,
        },
        "execution_stats": {
            "total_files": sum(row["execution_stats"]["total_files"] for row in rows),
            "total_bytes": sum(row["execution_stats"]["total_bytes"] for row in rows),
            "workers_used": workers,
            "container_append_count": sum(
                row["execution_stats"]["container_append_count"] for row in rows
            ),
            "container_open_count": sum(
                row["execution_stats"]["container_open_count"] for row in rows
            ),
            "container_close_count": sum(
                row["execution_stats"]["container_close_count"] for row in rows
            ),
            "fsync_count": sum(row["execution_stats"]["fsync_count"] for row in rows),
            "snapshot_metadata_write_count": 0,
            "io": total_io,
        },
        "rows": rows,
    }
    report = {"status": "ok", "command": "benchmark", "data": data}
    gate.validate_raw_report(
        report, workers=workers, compression=compression, dataset=dataset
    )
    return report


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
        logical_files = index + 1
        restored_files = logical_files if name in {"restore-large-file", "restore-many-files"} else 0
        diagnostic = diagnostic_final_state(
            logical_files=logical_files,
            logical_bytes=logical_bytes,
            restored_files=restored_files,
        )
        operational_samples = [operational_sample(index) for _ in durations]
        cases.append(
            {
                "case": name,
                "seed": 1712 + index * 10,
                "logical_files": logical_files,
                "logical_bytes": logical_bytes,
                "workers_used": 4,
                "sample_durations_ms": list(durations),
                "diagnostic_final_state": diagnostic,
                "diagnostic_samples": [deepcopy(diagnostic) for _ in durations],
                "operational_samples": operational_samples,
                "operational_counter_distributions": gate.summarize_operational_counters(
                    operational_samples
                ),
                **summary,
                "throughput_mbps": logical_bytes
                / (1024 * 1024)
                / (summary["median_duration_ms"] / 1000),
            }
        )
    return {
        "schema_version": 2,
        "evidence_policy_version": gate.EVIDENCE_POLICY_VERSION,
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
        "host_observations": [
            {
                "before": {"load_1m": 0, "load_5m": 0, "load_15m": 0, "free_disk_bytes": 1},
                "after": {"load_1m": 0, "load_5m": 0, "load_15m": 0, "free_disk_bytes": 1},
            }
            for _ in durations
        ],
        "operation_totals": gate.operation_totals(len(durations)),
        "cleanup_totals": gate.cleanup_totals(len(durations)),
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
        as_raw_row = raw_row()
        counter_variant = deepcopy(as_raw_row)
        counter_variant["execution_stats"]["fsync_count"] = 2
        counter_variant["execution_stats"]["io"]["fsyncs"] = 2
        self.assertEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(counter_variant))
        self.assertNotEqual(
            gate.validate_operational_counters(as_raw_row, workers=4),
            gate.validate_operational_counters(counter_variant, workers=4),
        )

        layout_variant = deepcopy(as_raw_row)
        layout_variant["diagnostic_final_state"]["physical"]["container_count"] += 1
        layout_variant["diagnostic_final_state"]["physical"]["container_bytes"] += 64
        layout_variant["diagnostic_final_state"]["physical_layout_sha256"] = "a" * 64
        self.assertEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(layout_variant))

        state_variant = deepcopy(as_raw_row)
        state_variant["diagnostic_final_state"]["active_logical_namespace"]["sha256"] = "f" * 64
        self.assertNotEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(state_variant))
        self.assertEqual(
            gate.validate_operational_counters(as_raw_row, workers=4),
            gate.validate_operational_counters(state_variant, workers=4),
        )

        catalog_variant = deepcopy(as_raw_row)
        catalog_variant["diagnostic_final_state"]["logical_catalog"]["sha256"] = "a" * 64
        self.assertNotEqual(gate.hard_final_state(as_raw_row), gate.hard_final_state(catalog_variant))

    def test_logical_status_totals_are_tied_to_catalog_not_namespace(self) -> None:
        state = diagnostic_final_state(logical_files=2)
        state["active_logical_namespace"]["count"] = 1
        gate.validate_diagnostic_final_state(state, "test")

        state["logical_catalog"]["count"] = 3
        with self.assertRaisesRegex(gate.GateError, "logical catalog count"):
            gate.validate_diagnostic_final_state(state, "test")

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
            with self.assertRaisesRegex(gate.GateError, "schema/policy/report kind/status"):
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
        report["cases"][0]["logical_files"] = 0
        with self.assertRaisesRegex(gate.GateError, "logical_files"):
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


class OutcomeEPolicyTests(unittest.TestCase):
    def test_policy_categories_are_explicit_and_nonempty(self) -> None:
        self.assertEqual(
            set(gate.FIELD_POLICY),
            {"hard_equal", "derived_equal", "bounded_nonnegative", "informational", "excluded_sensitive"},
        )
        self.assertTrue(all(gate.FIELD_POLICY[category] for category in gate.FIELD_POLICY))

    def test_hard_equal_mutations_fail_closed(self) -> None:
        def diagnostic_mutator(section: str, field: str, value: object):
            def mutate(report: dict) -> None:
                report["cases"][0]["diagnostic_final_state"][section][field] = value
            return mutate

        mutations = [
            ("fixture identity", lambda report: report["fixture"].__setitem__("id", "wrong")),
            ("seed", lambda report: report["cases"][0].__setitem__("seed", 9999)),
            ("active namespace totals", diagnostic_mutator("active_logical_namespace", "count", 2)),
            ("active namespace digest", diagnostic_mutator("active_logical_namespace", "sha256", "a" * 64)),
            ("logical catalog totals", diagnostic_mutator("logical_catalog", "count", 2)),
            ("logical catalog digest", diagnostic_mutator("logical_catalog", "sha256", "a" * 64)),
            ("chunk graph", diagnostic_mutator("chunk_graph", "sha256", "a" * 64)),
            ("restored tree", diagnostic_mutator("restored_tree", "sha256", "a" * 64)),
            ("snapshot membership", diagnostic_mutator("snapshots", "sha256", "a" * 64)),
            ("GC state", diagnostic_mutator("gc", "reachable_chunks", 0)),
            ("verification", diagnostic_mutator("verification", "blocks_checked", 2)),
            ("physical content", diagnostic_mutator("physical", "canonical_sha256", "a" * 64)),
            ("physical payload bytes", diagnostic_mutator("physical", "payload_bytes", 2)),
            ("operation result", lambda report: report["operation_totals"].__setitem__("failure", 1)),
            ("cleanup", lambda report: report["cleanup_totals"].__setitem__("failed", 1)),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                report = aggregate()
                mutate(report)
                with self.assertRaises(gate.GateError):
                    gate.validate_aggregate(report, require_gate_count=True)

    def test_valid_scheduling_counter_variation_is_retained(self) -> None:
        report = aggregate()
        case = report["cases"][4]
        varied = deepcopy(case["operational_samples"][1])
        varied["container_open_count"] += 1
        varied["container_close_count"] += 1
        varied["fsync_count"] += 1
        case["operational_samples"][1] = varied
        case["operational_counter_distributions"] = gate.summarize_operational_counters(
            case["operational_samples"]
        )
        gate.validate_aggregate(report, require_gate_count=True)
        self.assertEqual(
            case["operational_counter_distributions"]["container_open_count"]["values"],
            [5, 6],
        )

    def test_invalid_operational_counters_fail_closed(self) -> None:
        mutations = [
            ("missing", lambda row: row["execution_stats"]["io"].pop("fsyncs")),
            ("negative", lambda row: row["execution_stats"]["io"].__setitem__("bytes_read", -1)),
            ("wrong type", lambda row: row["execution_stats"]["io"].__setitem__("bytes_read", "0")),
            ("non-finite", lambda row: row["execution_stats"]["io"].__setitem__("bytes_read", math.inf)),
            ("unbalanced", lambda row: row["execution_stats"].__setitem__("container_close_count", 0)),
            ("contradiction", lambda row: (
                row["execution_stats"].__setitem__("container_open_count", 0),
                row["execution_stats"].__setitem__("container_close_count", 0),
                row["execution_stats"]["io"].__setitem__("container_opens", 0),
            )),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                row = raw_row()
                mutate(row)
                with self.assertRaises(gate.GateError):
                    gate.validate_operational_counters(row, workers=4)

    def test_layout_may_differ_but_canonical_content_may_not(self) -> None:
        report = aggregate()
        case = report["cases"][0]
        case["diagnostic_samples"][1]["physical_layout_sha256"] = "a" * 64
        case["diagnostic_samples"][1]["physical"]["container_count"] += 1
        gate.validate_aggregate(report, require_gate_count=True)

        report = aggregate()
        report["cases"][0]["diagnostic_samples"][1]["physical"]["canonical_sha256"] = "a" * 64
        with self.assertRaisesRegex(gate.GateError, "hard diagnostic"):
            gate.validate_aggregate(report, require_gate_count=True)

    def test_unknown_and_sensitive_extensions_fail_closed(self) -> None:
        mutations = [
            lambda report: report["cases"][0].__setitem__("new_correctness_field", 1),
            lambda report: report["cases"][0]["diagnostic_final_state"].__setitem__("new_info", 1),
            lambda report: report["provenance"].__setitem__("password", "secret"),
            lambda report: report["provenance"].__setitem__("runner_image", "/tmp/private-runner"),
        ]
        for mutate in mutations:
            report = aggregate()
            mutate(report)
            with self.assertRaises(gate.GateError):
                gate.validate_aggregate(report, require_gate_count=True)

    def test_raw_schema_v2_requires_diagnostic_evidence(self) -> None:
        report = raw_report()
        del report["data"]["rows"][0]["diagnostic_final_state"]
        with self.assertRaisesRegex(gate.GateError, "fields mismatch"):
            gate.validate_raw_report(report, workers=4, compression="none")

    def test_revalidation_retains_every_sample_without_calibration_claim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            raw_dir = root / "raw"
            gate.write_json(raw_dir / "sample-01.json", raw_report())
            second = raw_report()
            second["data"]["rows"][4]["execution_stats"]["container_open_count"] += 1
            second["data"]["rows"][4]["execution_stats"]["container_close_count"] += 1
            second["data"]["rows"][4]["execution_stats"]["fsync_count"] += 1
            second["data"]["rows"][4]["execution_stats"]["io"]["container_opens"] += 1
            second["data"]["rows"][4]["execution_stats"]["io"]["fsyncs"] += 1
            second["data"]["execution_stats"]["container_open_count"] += 1
            second["data"]["execution_stats"]["container_close_count"] += 1
            second["data"]["execution_stats"]["fsync_count"] += 1
            second["data"]["execution_stats"]["io"]["container_opens"] += 1
            second["data"]["execution_stats"]["io"]["fsyncs"] += 1
            gate.write_json(raw_dir / "sample-02.json", second)
            output = root / "revalidation.json"
            code = gate.revalidate_raw_command(argparse.Namespace(
                raw_dir=raw_dir,
                compression="none",
                workers=4,
                output=output,
            ))
            self.assertEqual(code, 0)
            result = gate.load_json_strict(output)
            gate.validate_revalidation_report(result)
            self.assertEqual(result["sample_count"], 2)
            self.assertEqual(result["performance_calibration_status"], "not_evaluated")
            self.assertEqual(
                result["cases"][4]["operational_counter_distributions"]["fsync_count"]["values"],
                [1, 2],
            )


class IntegrityCommandTests(unittest.TestCase):
    def args(self, root: pathlib.Path, *, dataset: str = "ci-paired-w1-v2") -> argparse.Namespace:
        root.mkdir(parents=True, exist_ok=True)
        binary = root / "coldkeep"
        binary.write_bytes(b"binary")
        return argparse.Namespace(
            binary=binary,
            output_dir=root / "owned" / "integrity",
            compression="none",
            workers=1,
            dataset=dataset,
            command_timeout_seconds=600,
            source_commit="a" * 40,
            source_tag=None,
            go_version="go version go1.25.12 linux/amd64",
            postgres_version="postgres (PostgreSQL) 16",
            database_image_digest="sha256:" + "b" * 64,
        )

    def environment(self) -> dict[str, str]:
        return {
            "COLDKEEP_CODEC": "aes-gcm",
            "DB_HOST": "127.0.0.1",
            "DB_PORT": "5432",
            "DB_USER": "test",
            "DB_PASSWORD": "test",
            "DB_NAME": "test",
            "DB_SSLMODE": "disable",
        }

    def test_integrity_success_owns_output_and_checksums_every_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            args = self.args(root)
            report = raw_report(workers=1, dataset=args.dataset)

            def capture(_args, path, _binary_hash):
                gate.write_json(path, report)
                path.with_suffix(".stderr").write_text("", encoding="utf-8")
                point = {"load_1m": 0, "load_5m": 0, "load_15m": 0, "free_disk_bytes": 1}
                return deepcopy(report), 1000.0, {"before": point, "after": point}

            with mock.patch.dict(gate.os.environ, self.environment(), clear=False), mock.patch.object(
                gate, "capture_sample", side_effect=capture
            ):
                self.assertEqual(gate.integrity_command(args), 0)
            result = gate.load_json_strict(args.output_dir / "benchmark-integrity.json")
            gate.validate_integrity_report(result)
            self.assertEqual(result["classification"], "BENCHMARK_INTEGRITY_PASS")
            self.assertEqual(result["completed_sample_count"], 2)
            checksum_lines = (args.output_dir / "checksums.sha256").read_text().splitlines()
            self.assertEqual(len(checksum_lines), 6)
            self.assertTrue(any(line.endswith("  aggregate.json") for line in checksum_lines))

    def test_integrity_failure_is_checksummed_without_aggregate_claims(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            args = self.args(root)
            with mock.patch.dict(gate.os.environ, self.environment(), clear=False), mock.patch.object(
                gate, "capture_sample", side_effect=gate.GateError("raw contract mismatch")
            ):
                self.assertEqual(gate.integrity_command(args), 2)
            result = gate.load_json_strict(args.output_dir / "benchmark-integrity.json")
            gate.validate_integrity_report(result)
            self.assertEqual(result["classification"], "BENCHMARK_INTEGRITY_FAILURE")
            self.assertIsNone(result["aggregate_file"])
            self.assertEqual(result["completed_prefix"], [])
            self.assertIsNone(result["active_invocation"])
            self.assertEqual(result["incomplete_invocation"]["sample_index"], 1)
            self.assertFalse((args.output_dir / "aggregate.json").exists())
            self.assertTrue((args.output_dir / "checksums.sha256").is_file())

    def test_integrity_rejects_existing_output_and_worker_fixture_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            args = self.args(root)
            args.output_dir.mkdir(parents=True)
            with self.assertRaisesRegex(gate.GateError, "must not exist"):
                gate.integrity_command(args)
            args = self.args(root / "second", dataset="ci-paired-w4-v2")
            with self.assertRaisesRegex(gate.GateError, "worker profile mismatch"):
                gate.integrity_command(args)


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
        candidate["cases"][0]["logical_files"] += 1
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
                    case["workers_used"] = workers
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
                            case["workers_used"] = workers
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
