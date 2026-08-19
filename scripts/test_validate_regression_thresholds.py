import contextlib
import copy
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

from scripts import validate_regression_thresholds as validator
from scripts.test_benchmark_gate import raw_report


ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE = (
    ROOT
    / "benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json"
)
THRESHOLDS = ROOT / "benchmarks/v1.9/regression-thresholds.yaml"


def candidate_from_baseline() -> dict:
    candidate = copy.deepcopy(json.loads(BASELINE.read_text(encoding="utf-8")))
    data = candidate["data"]
    data["schema_version"] = 2
    data["fixture"] = {
        **validator.SMALL_FIXTURE,
        "ordered_cases": [
            {"name": name, "seed": 1712 + index * 10}
            for index, name in enumerate(validator.EXPECTED_CASES)
        ],
    }
    return candidate


def valid_diagnostic_final_state() -> dict:
    return raw_report(workers=1, dataset="ci-paired-w1-v2")["data"]["rows"][0][
        "diagnostic_final_state"
    ]


class AdvisoryComparatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.candidate = self.root / "candidate.json"
        self.report = self.root / "timing-advisory.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def invoke(self, *arguments: str) -> int:
        with mock.patch.object(sys, "argv", ["validate_regression_thresholds.py", *arguments]):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                return validator.main()

    def write_candidate(self, candidate: dict) -> None:
        self.candidate.write_text(json.dumps(candidate) + "\n", encoding="utf-8")

    def check(self) -> int:
        return self.invoke(
            "check",
            str(self.candidate),
            "--baseline",
            str(BASELINE),
            "--mode",
            "compressed",
            "--thresholds",
            str(THRESHOLDS),
            "--policy",
            "hosted-advisory",
            "--json-report",
            str(self.report),
        )

    def test_within_reference_exit_and_report_verify_exactly(self) -> None:
        self.write_candidate(candidate_from_baseline())
        self.assertEqual(self.check(), 0)
        report = json.loads(self.report.read_text(encoding="utf-8"))
        self.assertEqual(report["classification"], "BENCHMARK_TIMING_WITHIN_REFERENCE")
        self.assertEqual(report["reference_kind"], "historical_v1.9_absolute")
        self.assertEqual(
            self.invoke(
                "verify-advisory-exit",
                "--report",
                str(self.report),
                "--observed-exit-code",
                "0",
            ),
            0,
        )
        self.assertEqual(
            self.invoke(
                "verify-advisory-exit",
                "--report",
                str(self.report),
                "--observed-exit-code",
                "10",
            ),
            2,
        )

    def test_real_shape_small_observation_does_not_require_diagnostic_state(self) -> None:
        candidate = candidate_from_baseline()
        self.assertTrue(all("diagnostic_final_state" not in row for row in candidate["data"]["rows"]))
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 0)

    def test_go_omitempty_operational_zeroes_may_all_be_absent(self) -> None:
        candidate = candidate_from_baseline()
        for stats in [
            candidate["data"]["execution_stats"],
            *(row["execution_stats"] for row in candidate["data"]["rows"]),
        ]:
            for field in validator.EXECUTION_STATS_OMITTABLE_ZERO_FIELDS:
                stats.pop(field, None)
            stats["io"]["container_opens"] = 0
            stats["io"]["container_appends"] = 0
            stats["io"]["fsyncs"] = 0
            stats["io"]["bytes_written"] = 0
            stats["io"]["bytes_read"] = 0
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 0)

    def test_optional_diagnostic_final_state_is_structurally_validated(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["rows"][0]["diagnostic_final_state"] = valid_diagnostic_final_state()
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 0)

        candidate["data"]["rows"][0]["diagnostic_final_state"] = {"schema_version": 2}
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        self.assert_failure_report()

    def test_warning_exit_is_ten_and_never_pass(self) -> None:
        candidate = candidate_from_baseline()
        row = candidate["data"]["rows"][0]
        row["duration_ms"] *= 2
        row["throughput_mbps"] = (
            row["execution_stats"]["total_bytes"]
            / (1024 * 1024)
            / (row["duration_ms"] / 1000.0)
        )
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 10)
        report = json.loads(self.report.read_text(encoding="utf-8"))
        self.assertEqual(report["classification"], "BENCHMARK_TIMING_WARNING")
        self.assertGreater(report["violations_count"], 0)
        self.assertNotIn("passed", report)

    def test_unknown_candidate_data_is_error_with_failure_report(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["unknown"] = True
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        self.assert_failure_report()

    def test_unknown_row_and_execution_stat_fields_fail_closed(self) -> None:
        for location in ("row", "stats"):
            with self.subTest(location=location):
                candidate = candidate_from_baseline()
                target = candidate["data"]["rows"][0]
                if location == "stats":
                    target = target["execution_stats"]
                target["unknown"] = True
                self.write_candidate(candidate)
                self.assertEqual(self.check(), 2)
                self.assert_failure_report()

    def test_fixture_order_and_seed_mismatches_are_errors(self) -> None:
        mutations = (
            lambda candidate: candidate["data"]["fixture"].__setitem__("seed", 1702),
            lambda candidate: candidate["data"]["rows"].reverse(),
            lambda candidate: candidate["data"]["fixture"]["ordered_cases"][0].__setitem__("seed", 1713),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                candidate = candidate_from_baseline()
                mutation(candidate)
                self.write_candidate(candidate)
                self.assertEqual(self.check(), 2)
                self.assert_failure_report()

    def test_counter_total_mismatch_is_error(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["execution_stats"]["total_files"] += 1
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        self.assert_failure_report()

    def test_duplicated_io_counter_mismatch_is_error(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["rows"][0]["execution_stats"]["io"]["container_opens"] += 1
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)

    def test_derived_throughput_mismatch_is_error(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["rows"][0]["throughput_mbps"] += 0.1
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)

    def assert_failure_report(self) -> dict:
        report = json.loads(self.report.read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["classification"], "BENCHMARK_TIMING_EVALUATION_FAILURE")
        self.assertEqual(report["authority"], "informational")
        self.assertEqual(report["violations_count"], 0)
        self.assertEqual(report["violations"], [])
        self.assertNotIn("passed", report)
        return report

    def test_missing_and_malformed_candidate_emit_evaluator_failure(self) -> None:
        self.assertEqual(self.check(), 2)
        missing = self.assert_failure_report()
        self.assertEqual(missing["error"]["category"], "missing_input")
        self.assertNotIn("candidate_sha256", missing)

        self.candidate.write_text("{malformed\n", encoding="utf-8")
        self.assertEqual(self.check(), 2)
        malformed = self.assert_failure_report()
        self.assertEqual(malformed["error"]["category"], "malformed_input")
        self.assertIn("candidate_sha256", malformed)

    def test_evaluator_failure_report_exit_two_verifies_exactly(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["unknown"] = True
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        self.assertEqual(validator.verify_advisory_exit(self.report, 2), 0)
        with self.assertRaises(RuntimeError):
            validator.verify_advisory_exit(self.report, 12)

    def test_evaluator_failure_cannot_claim_not_evaluated_timing_or_production_authority(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["unknown"] = True
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        base = self.assert_failure_report()
        mutations = (
            {"classification": "BENCHMARK_TIMING_NOT_EVALUATED"},
            {"timing_within_reference": True},
            {"performance_authority": True},
            {"authority": "production"},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                report = {**base, **mutation}
                self.report.write_text(json.dumps(report), encoding="utf-8")
                with self.assertRaises(RuntimeError):
                    validator.verify_advisory_exit(self.report, 2)

    def test_all_declared_advisory_exit_classifications_verify(self) -> None:
        base = {
            "schema_version": 1,
            "report_kind": validator.ADVISORY_REPORT_KIND,
            "status": "complete",
            "classification": "",
            "authority": "informational",
            "reference_kind": "historical_v1.9_absolute",
            "mode": "compressed",
            "candidate_sha256": "a" * 64,
            "baseline_sha256": "b" * 64,
            "violations_count": 0,
            "violations": [],
        }
        for classification, exit_code in validator.ADVISORY_EXIT_CODES.items():
            with self.subTest(classification=classification):
                report = copy.deepcopy(base)
                report["classification"] = classification
                if classification == "BENCHMARK_TIMING_WARNING":
                    report["violations"] = [{"case": "store-large-file"}]
                    report["violations_count"] = 1
                if classification == "BENCHMARK_TIMING_NOT_EVALUATED":
                    report["status"] = "not_evaluated"
                if classification == "BENCHMARK_TIMING_EVALUATION_FAILURE":
                    report["status"] = "failed"
                    report["error"] = {
                        "category": "contract_error",
                        "message": "benchmark advisory evidence violates its contract",
                    }
                self.report.write_text(json.dumps(report), encoding="utf-8")
                self.assertEqual(validator.verify_advisory_exit(self.report, exit_code), 0)

    def test_legacy_default_keeps_exit_one_for_hard_regression(self) -> None:
        candidate = json.loads(BASELINE.read_text(encoding="utf-8"))
        row = candidate["data"]["rows"][0]
        row["duration_ms"] *= 2
        self.write_candidate(candidate)
        self.assertEqual(
            self.invoke(
                "check",
                str(self.candidate),
                "--baseline",
                str(BASELINE),
                "--mode",
                "uncompressed",
                "--thresholds",
                str(THRESHOLDS),
            ),
            1,
        )


if __name__ == "__main__":
    unittest.main()
