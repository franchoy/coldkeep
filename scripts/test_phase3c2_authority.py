"""Integrated authority is source-bound structure, not runtime certification."""
import hashlib
import json
import shutil

import phase3c2_authority as c2
import validate_release_state as release
from test_v2_authority import V2AuthorityFixture, ROOT


class IntegratedC2AuthorityTests(V2AuthorityFixture):
    fixture_phase = "2.04"

    def setUp(self):
        super().setUp()
        self.directory = self.root / "docs/release/v2.0"
        for name in c2.PREDECESSORS:
            shutil.copy2(ROOT / "docs/release/v2.0" / name, self.directory / name)
        # Deliberately synthetic source/results: only authority parsing is tested.
        for name in c2.INPUT_ROOTS:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / "internal/model.go").write_text("package fixture\n")
        self.record = {
            "schema": "coldkeep-phase3c2-local-qualification/v1",
            "accepted_c1": "06bde38380dd3a42be5c180beb99bf1aa509b8ac",
            "head": "1" * 40, "tree": "2" * 40,
            "input_sha256": c2.input_digest(self.root), "compiler": "go1.26.11",
            "checks": {key: "PASS" for key in (
                "focused-race", "recovery", "passive", "plain-packages", "aes-gcm-packages",
                "current-oracle", "d19-oracle", "python", "go-ci-enforcement", "build-vet-lint",
                "dependency-boundaries", "ci-audit-mutations", "phase2-integrated",
                "reachable-vulnerability", "index-continuity", "query-work", "dto-consumer", "fixture-cleanup")},
        }
        self.raw = (json.dumps(self.record) + "\n").encode()
        self.body = "\n".join((
            "Phase 3C2: COMPLETE", "Phase 3C: COMPLETE", "Local-profile action 2.04: COMPLETE",
            "Implementation: ACTIVE", "Product gates: INCOMPLETE",
            "Profile: installed Linux/PostgreSQL16 schema20",
            "Governing BUILD: coldkeep_phase3c2_build_2026-11-12.md",
            "Accepted C1: 06bde38380dd3a42be5c180beb99bf1aa509b8ac",
            "Qualification HEAD: " + "1" * 40, "Qualification tree: " + "2" * 40,
            "Qualification SHA-256: " + hashlib.sha256(self.raw).hexdigest(),
            "Ordinary schema18/19/20 writers and reclamation: REFUSED",
            "IMPORT_READY maintenance/activation: REFUSED", "Action 2.05: NOT IMPLEMENTED",
        )) + "\n"
        self.result = self.directory / "phase3c2-result.md"
        self.qualification = self.directory / "phase3c2-qualification.json"
        self.result.write_text(self.body)
        self.qualification.write_bytes(self.raw)
        self.change(phase_status="COMPLETE")

    def test_integrated_complete_preserves_product_and_later_obligations(self):
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(result.evidence_scope, "LOCAL_ACTION_2_04_INTEGRATED_AUTHORITY_ONLY")
        self.assertEqual(result.outstanding_obligations,
                         ["2.05-2.13", "2.15", "2.17", "separate-publication-authorization"])
        for state in ("pre-release", "released", "post-release-closed"):
            self.assertTrue(release.validate(self.root, state).violations)
        for change in ({"phase": "2.05"}, {"product_gates": "COMPLETE"}, {"implementation": "COMPLETE"}):
            with self.subTest(change=change):
                original = json.loads((self.root / "docs/release/current.json").read_text())
                self.change(**change)
                self.assert_rejected()
                (self.root / "docs/release/current.json").write_text(json.dumps(original))

    def test_each_result_field_is_required_exact_and_unambiguous(self):
        for line in self.body.splitlines():
            for replacement in ("", line + " pending", line + "\n" + line):
                with self.subTest(field=line, replacement=replacement):
                    self.result.write_text(self.body.replace(line, replacement))
                    self.assert_rejected()
        self.result.write_text(self.body)

    def test_missing_stale_conflicting_and_relocated_qualification_refused(self):
        for key in self.record:
            value = dict(self.record)
            value.pop(key)
            self.qualification.write_text(json.dumps(value))
            self.assert_rejected()
        for key in self.record["checks"]:
            value = json.loads(self.raw)
            value["checks"][key] = "SKIP"
            raw = json.dumps(value).encode()
            self.qualification.write_bytes(raw)
            self.result.write_text(self.body.replace(hashlib.sha256(self.raw).hexdigest(), hashlib.sha256(raw).hexdigest()))
            self.assert_rejected()
        self.result.write_text(self.body)
        self.qualification.write_bytes(self.raw[:-2] + b',"compiler":"go1.26.11"}\n')
        self.assert_rejected()
        self.qualification.unlink()
        self.qualification.symlink_to(self.result)
        self.assert_rejected()

    def test_accepted_predecessors_cannot_be_missing_changed_or_replaced(self):
        for name in c2.PREDECESSORS:
            with self.subTest(name=name):
                path = self.directory / name
                data = path.read_bytes()
                path.unlink()
                self.assert_rejected()
                path.write_bytes(data + b"\nConflicting completion\n")
                self.assert_rejected()
                path.write_bytes(data)

    def test_c1_or_old_reports_cannot_close_c2(self):
        self.result.unlink()
        self.assert_rejected()
        self.result.write_bytes((self.directory / "phase3c1-result.md").read_bytes())
        self.assert_rejected()
        self.change(phase_status="ACTIVE")
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertIn("2.04-phase3c2", result.outstanding_obligations)
        self.assertIn("2.04-phase3c", result.outstanding_obligations)

    def test_input_bytes_modes_and_added_files_invalidate_result(self):
        path = self.root / "internal/model.go"
        for kind in ("bytes", "mode", "addition"):
            with self.subTest(kind=kind):
                original = path.read_bytes()
                if kind == "bytes":
                    path.write_bytes(original + b"// changed\n")
                elif kind == "mode":
                    path.chmod(0o755)
                else:
                    (self.root / "internal/added.go").write_text("package fixture\n")
                self.assert_rejected()
                path.write_bytes(original)
                path.chmod(0o644)
                (self.root / "internal/added.go").unlink(missing_ok=True)
        (self.directory / "unrelated-status.md").write_text("Documentation successor\n")
        self.assertEqual(release.validate(self.root, "auto").violations, [])
