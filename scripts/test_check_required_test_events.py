import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import check_required_test_events as checker


class RequiredTestEventsTests(unittest.TestCase):
    def complete_events(self, profile_name):
        profile = checker.PROFILES[profile_name]
        package = profile["package"]
        events = [{"Action": "start", "Package": package}]
        for test_name in profile["tests"]:
            events.extend(
                [
                    {"Action": "run", "Package": package, "Test": test_name},
                    {"Action": "pass", "Package": package, "Test": test_name},
                ]
            )
        events.append({"Action": "pass", "Package": package})
        return events

    def run_checker(self, profile_name, events=None, raw=None):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.json"
            if raw is not None:
                path.write_text(raw, encoding="utf-8")
            else:
                path.write_text(
                    "".join(json.dumps(event) + "\n" for event in events or []),
                    encoding="utf-8",
                )
            return subprocess.run(
                [
                    sys.executable,
                    str(Path(checker.__file__)),
                    "--profile",
                    profile_name,
                    "--events",
                    str(path),
                ],
                text=True,
                capture_output=True,
                check=False,
            )

    def assert_rejected(self, profile_name, events=None, raw=None, message=None):
        result = self.run_checker(profile_name, events, raw)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        if message:
            self.assertIn(message, result.stderr)

    def test_accepts_complete_plain_profile(self):
        result = self.run_checker(
            "integration-correctness-plain",
            self.complete_events("integration-correctness-plain"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_complete_aes_gcm_profile(self):
        result = self.run_checker(
            "integration-correctness-aes-gcm",
            self.complete_events("integration-correctness-aes-gcm"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_complete_internal_verify_profile(self):
        result = self.run_checker(
            "ck014-internal-verify",
            self.complete_events("ck014-internal-verify"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_complete_ck015_sqlite_profile(self):
        profile = "ck015-initial-lookup-sqlite"
        result = self.run_checker(profile, self.complete_events(profile))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_complete_ck015_postgres_profile(self):
        profile = "ck015-initial-lookup-postgres"
        result = self.run_checker(profile, self.complete_events(profile))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_ck015_rejects_skipped_routing_child_when_parent_passes(self):
        profile = "ck015-initial-lookup-sqlite"
        events = self.complete_events(profile)
        child = "TestCKV11316015InitialLookupSupportedStatusRoutingSQLite/processing"
        for event in events:
            if event.get("Test") == child and event.get("Action") == "pass":
                event["Action"] = "skip"
        self.assert_rejected(profile, events, message=child)

    def test_ck015_rejects_missing_postgres_obligation(self):
        profile = "ck015-initial-lookup-postgres"
        missing = "TestCKV11316015PostgresSharedChunkHealingBetweenValidationAndPlanReclassifies"
        events = [
            event
            for event in self.complete_events(profile)
            if event.get("Test") != missing
        ]
        self.assert_rejected(profile, events, message=missing)

    def test_ck015_rejects_required_event_under_wrong_package(self):
        profile = "ck015-initial-lookup-postgres"
        test_name = checker.PROFILES[profile]["tests"][0]
        events = self.complete_events(profile)
        for event in events:
            if event.get("Test") == test_name:
                event["Package"] = "github.com/franchoy/coldkeep/internal/verify"
        self.assert_rejected(profile, events, message="required run count mismatch")

    def test_ck015_rejects_contradictory_terminal_event(self):
        profile = "ck015-initial-lookup-sqlite"
        test_name = checker.PROFILES[profile]["tests"][0]
        events = self.complete_events(profile)
        events.insert(
            -1,
            {
                "Action": "fail",
                "Package": checker.PROFILES[profile]["package"],
                "Test": test_name,
            },
        )
        self.assert_rejected(profile, events, message="contradictory")

    def test_ck015_allows_unrelated_optional_skip(self):
        profile = "ck015-initial-lookup-postgres"
        events = self.complete_events(profile)
        package = checker.PROFILES[profile]["package"]
        events.insert(-1, {"Action": "run", "Package": package, "Test": "TestOptional"})
        events.insert(-1, {"Action": "skip", "Package": package, "Test": "TestOptional"})
        result = self.run_checker(profile, events)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_wrong_package_for_required_test(self):
        profile = "integration-correctness-aes-gcm"
        events = self.complete_events(profile)
        test_name = checker.PROFILES[profile]["tests"][0]
        for event in events:
            if event.get("Test") == test_name:
                event["Package"] = "github.com/franchoy/coldkeep/internal/verify"
        self.assert_rejected(profile, events, message="required run count mismatch")

    def test_rejects_pass_without_run(self):
        profile = "integration-correctness-aes-gcm"
        events = [event for event in self.complete_events(profile) if event.get("Action") != "run"]
        self.assert_rejected(profile, events, message="required run count mismatch")

    def test_rejects_fail_then_pass_for_required_test(self):
        profile = "integration-correctness-aes-gcm"
        events = self.complete_events(profile)
        events.insert(-1, {"Action": "fail", "Package": checker.PROFILES[profile]["package"], "Test": "TestRoundTripStoreRestore"})
        self.assert_rejected(profile, events, message="contradictory")

    def test_rejects_skip_then_pass_for_required_test(self):
        profile = "integration-correctness-aes-gcm"
        events = self.complete_events(profile)
        events.insert(-1, {"Action": "skip", "Package": checker.PROFILES[profile]["package"], "Test": "TestRoundTripStoreRestore"})
        self.assert_rejected(profile, events, message="contradictory")

    def test_rejects_missing_expected_package(self):
        self.assert_rejected("integration-correctness-aes-gcm", [], message="contained no events")

    def test_rejects_incomplete_expected_package(self):
        profile = "integration-correctness-aes-gcm"
        events = self.complete_events(profile)[:-1]
        self.assert_rejected(profile, events, message="did not complete")

    def test_rejects_expected_package_failure(self):
        profile = "integration-correctness-aes-gcm"
        events = self.complete_events(profile)
        events[-1]["Action"] = "fail"
        self.assert_rejected(profile, events, message="did not complete")

    def test_rejects_unknown_profile(self):
        result = self.run_checker("unknown-profile", [])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid choice", result.stderr)

    def test_rejects_empty_evidence(self):
        self.assert_rejected("integration-correctness-aes-gcm", [], message="contained no events")

    def test_rejects_malformed_evidence(self):
        self.assert_rejected("integration-correctness-aes-gcm", raw="not-json\n", message="malformed JSON")

    def test_rejects_missing_required_selector(self):
        profile = "integration-correctness-plain"
        events = [
            event
            for event in self.complete_events(profile)
            if event.get("Test") != "TestSimulationMatchesRealSizeMetrics"
        ]
        self.assert_rejected(profile, events, message="TestSimulationMatchesRealSizeMetrics")

    def test_allows_unrelated_optional_skip(self):
        profile = "integration-correctness-aes-gcm"
        events = self.complete_events(profile)
        events.insert(-1, {"Action": "run", "Package": checker.PROFILES[profile]["package"], "Test": "TestOptional"})
        events.insert(-1, {"Action": "skip", "Package": checker.PROFILES[profile]["package"], "Test": "TestOptional"})
        result = self.run_checker(profile, events)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
