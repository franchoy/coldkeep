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
    diagnostic_rows = raw_report(workers=1, dataset="ci-paired-w1-v2")["data"]["rows"]
    data["schema_version"] = 2
    data["fixture"] = {
        **validator.SMALL_FIXTURE,
        "ordered_cases": [
            {"name": name, "seed": 1712 + index * 10}
            for index, name in enumerate(validator.EXPECTED_CASES)
        ],
    }
    for row, diagnostic_row in zip(data["rows"], diagnostic_rows):
        row["diagnostic_final_state"] = diagnostic_row["diagnostic_final_state"]
    return candidate


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

    def test_malformed_or_unknown_candidate_evidence_is_error(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["unknown"] = True
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        self.assertFalse(self.report.exists())

    def test_counter_total_and_derived_throughput_are_hard_contracts(self) -> None:
        candidate = candidate_from_baseline()
        candidate["data"]["execution_stats"]["total_files"] += 1
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)

    def test_missing_diagnostic_final_state_is_hard_contract_error(self) -> None:
        candidate = candidate_from_baseline()
        del candidate["data"]["rows"][0]["diagnostic_final_state"]
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)
        candidate = candidate_from_baseline()
        candidate["data"]["rows"][0]["throughput_mbps"] += 0.1
        self.write_candidate(candidate)
        self.assertEqual(self.check(), 2)

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
