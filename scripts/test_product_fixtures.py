"""Evidence must prove actual fixture execution and the fixed expected outcomes."""
import copy
import unittest

import run_product_fixtures as runner


class FixtureEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.package = "github.com/franchoy/coldkeep/v2/tests/productfixtures"
        self.oracle = {"fixtures": [{"id": "valid", "expected": {"outcome": "RESTORED", "outputs": [
            {"path": "x", "sha256": "expected-hash", "size": 5}]}}]}
        self.report = {"fixtures": [{"fixture_id": "valid", "observed_outcome": "RESTORED",
            "observed_outputs": copy.deepcopy(self.oracle["fixtures"][0]["expected"]["outputs"]),
            "catalog_semantic_unchanged": True, "payload_bytes_unchanged": True}]}
        self.events = [{"Package": self.package, "Test": "TestProductFixtures/valid", "Action": a}
                       for a in ("run", "pass")]
        self.events.append({"Package": self.package, "Action": "pass"})

    def check(self, report=None, events=None):
        return runner.validate_report(self.report if report is None else report, self.oracle,
                                      self.events if events is None else events, self.package)

    def test_real_execution_and_expected_bytes_are_required(self):
        self.assertEqual(self.check(), [])
        for field, value in (("observed_outcome", "REFUSED"), ("observed_outputs", []),
                             ("catalog_semantic_unchanged", False), ("payload_bytes_unchanged", False)):
            report = copy.deepcopy(self.report)
            report["fixtures"][0][field] = value
            self.assertTrue(self.check(report=report), field)

    def test_missing_duplicate_skipped_and_legacy_package_evidence_fails(self):
        for report in ({"fixtures": []}, {"fixtures": self.report["fixtures"] * 2}):
            self.assertTrue(self.check(report=report))
        for events in ([], self.events[:-1], self.events[1:], self.events + [self.events[0]],
                       [dict(e, Package=self.package.replace("/v2", "")) for e in self.events],
                       [dict(e, Action="skip") if e.get("Test") and e["Action"] == "pass" else e for e in self.events]):
            self.assertTrue(self.check(events=events))

    def test_refusal_and_known_limit_cannot_substitute_for_restore(self):
        self.oracle["fixtures"][0]["expected"] = {"outcome": "REFUSED", "outputs": [], "error_contains": "missing key"}
        result = self.report["fixtures"][0]
        result.update(observed_outcome="REFUSED", observed_outputs=[], observed_refusal="missing key")
        self.assertEqual(self.check(), [])
        result["observed_refusal"] = "corrupt payload"
        self.assertTrue(self.check())
        result.update(observed_outcome="KNOWN_LIMIT", observed_refusal="missing key")
        self.assertTrue(self.check())


if __name__ == "__main__":
    unittest.main()
