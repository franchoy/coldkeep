"""Explicit Phase 0 / action 2.01 authority and prerelease regressions."""
from pathlib import Path
import json
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

import release_state_support as support
import validate_governance as governance
import validate_release_state as release
import check_required_test_events as events

ROOT = Path(__file__).resolve().parent.parent


class V2AuthorityFixture(unittest.TestCase):
    fixture_phase = "0"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="coldkeep-v2-authority-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        paths = (*support.V2_MIRRORS, support.CURRENT_AUTHORITY_PATH,
                 "docs/release/v2.0/README.md", "go.mod", "internal/version/version.go")
        for relative in paths:
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        # Bind the fixture to its declared phase, independently of live authority.
        self.change(phase=self.fixture_phase, phase_status="ACTIVE")
        for relative in support.V2_MIRRORS:
            path = self.root / relative
            path.write_text(re.sub(r"(?m)^CURRENT_PHASE: .*?$",
                                   "CURRENT_PHASE: " + self.fixture_phase, path.read_text()))
        # Preserve real archived documents in the fixture, without touching them.
        for directory in (ROOT / "docs/release").glob("v1*"):
            if directory.is_dir():
                shutil.copytree(directory, self.root / "docs/release" / directory.name)
        self.env = patch.dict("os.environ", {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def change(self, **values):
        path = self.root / support.CURRENT_AUTHORITY_PATH
        state = json.loads(path.read_text())
        state.update(values)
        path.write_text(json.dumps(state))

    def assert_rejected(self):
        self.assertTrue(governance.validate(self.root))
        self.assertTrue(release.validate(self.root, "auto").violations)


class V2AuthorityTests(V2AuthorityFixture):
    fixture_phase = "0"

    def test_current_and_historical_dispatch_are_distinct(self):
        self.assertEqual(governance.validate(self.root), [])
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(result.active_version, "2.0.0-dev.0")
        self.assertEqual(result.state, "development")
        self.assertEqual(result.evidence_scope, "LOCAL_PHASE0_AUTHORITY_ONLY")
        self.assertTrue(result.outstanding_obligations)
        self.assertTrue(governance.validate(self.root, "historical-v1"))
        self.assertTrue(release.validate(self.root, "auto", authority="historical-v1").violations)

    def test_missing_duplicate_unknown_and_mixed_authority_fail(self):
        path = self.root / support.CURRENT_AUTHORITY_PATH
        original = path.read_bytes()
        mutations = [b'{}', b'{"family":"v2.0","family":"v1.13"}']
        for key, value in [("family", "v1.13"), ("family", "v9.0"), ("state", "released"),
                           ("version", "2.0.0"), ("product_gates", "COMPLETE"),
                           ("latest_stable", "2.0.0-dev.0"), ("phase_status", "QUALIFIED")]:
            state = json.loads(original)
            state[key] = value
            mutations.append(json.dumps(state).encode())
        for value in mutations:
            with self.subTest(value=value):
                path.write_bytes(value)
                self.assert_rejected()
        path.unlink()
        self.assert_rejected()
        path.symlink_to(self.root / "go.mod")
        self.assert_rejected()

    def test_v2_patch18_and19_never_select_v1(self):
        path = self.root / "internal/version/version.go"
        original = path.read_text()
        for patch_number in (18, 19):
            with self.subTest(patch=patch_number):
                # gofmt spacing is not the contract.
                path.write_text(re.sub(r"(?m)^(\s*Patch\s*=\s*)0$", rf"\g<1>{patch_number}", original))
                self.assertFalse(governance._v18_selected(self.root))
                self.assertFalse(governance._v19_selected(self.root))
                self.assert_rejected()

    def test_complete_phase_is_not_product_qualification(self):
        self.change(phase_status="COMPLETE")
        self.assertEqual(governance.validate(self.root), [])
        for requested in ("pre-release", "released", "post-release-closed"):
            self.assertTrue(release.validate(self.root, requested).violations)
        with patch.dict("os.environ", {"GITHUB_REF": "refs/tags/v2.0.0-dev.0"}):
            self.assert_rejected()

    def test_mirror_module_and_frozen_history_must_agree(self):
        for relative in ("AGENTS.md", "go.mod", "docs/release/v1.13/v1.13.19-release-state.md"):
            path = self.root / relative
            original = path.read_bytes()
            with self.subTest(path=relative):
                path.write_bytes(original.replace(b"/v2", b"/v3") if relative == "go.mod" else original.replace(b"SOURCE_VERSION:", b"WRONG_VERSION:", 1))
                self.assert_rejected()
                path.write_bytes(original)

    def test_frozen_history_roots_and_ancestors_must_be_real_directories(self):
        for relative in ("docs/release/v1.13", "docs/release", "docs"):
            with self.subTest(path=relative):
                path = self.root / relative
                moved = self.root / "relocated-history"
                path.rename(moved)
                try:
                    path.symlink_to(moved, target_is_directory=True)
                    self.assert_rejected()
                    with self.assertRaisesRegex(ValueError, "historical"):
                        support.frozen_v1_digest(self.root)
                finally:
                    if path.is_symlink():
                        path.unlink()
                    moved.rename(path)

    def test_frozen_history_family_and_descendant_links_are_rejected(self):
        file_target = self.root / "external-history-file"
        file_target.write_text("external history")
        directory_target = self.root / "external-history-directory"
        directory_target.mkdir()
        (directory_target / "evidence.md").write_text("external history")
        for relative, target in (
            ("docs/release/v1.link", file_target),
            ("docs/release/v1.13/linked-file.md", file_target),
            ("docs/release/v1.13/linked-directory", directory_target),
        ):
            with self.subTest(path=relative):
                path = self.root / relative
                path.symlink_to(target, target_is_directory=target.is_dir())
                try:
                    self.assert_rejected()
                    with self.assertRaisesRegex(ValueError, "historical"):
                        support.frozen_v1_digest(self.root)
                finally:
                    path.unlink()

    def test_missing_protected_history_is_rejected(self):
        for relative in ("docs", "docs/release", "docs/release/v1.13",
                         "docs/release/v1.13/v1.13.19-release-state.md"):
            with self.subTest(path=relative):
                path = self.root / relative
                moved = self.root / "removed-history"
                path.rename(moved)
                try:
                    self.assert_rejected()
                finally:
                    moved.rename(path)

    def test_frozen_digest_preserves_real_tree_bytes_and_path_order(self):
        state = json.loads((self.root / support.CURRENT_AUTHORITY_PATH).read_text())
        self.assertEqual(support.frozen_v1_digest(self.root), state["frozen_v1_sha256"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative, content in (("a.md", b"flat\n"), ("a/x", b"nested\n")):
                path = root / "docs/release/v1.13" / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            # The original sorted(Path) encoding places a/x before a.md.
            self.assertEqual(support.frozen_v1_digest(root),
                             "d8cee37b3841a8f9ebdf93038c019035a0c10af780fb8c77d6cb23a9866635f5")

    def test_core_version_expressions_and_trailing_tokens_are_rejected(self):
        path = self.root / "internal/version/version.go"
        original = path.read_text()
        for name in ("Major", "Minor", "Patch"):
            for suffix in (" + 1", " / 1", " << 1", " trailing"):
                with self.subTest(component=name, suffix=suffix):
                    text = re.sub(rf"(?m)^([ \t]*{name}[ \t]*=[ \t]*[0-9]+)[ \t]*$",
                                  lambda match: match.group(1) + suffix, original)
                    self.assertNotEqual(text, original)
                    path.write_text(text)
                    result = support.ValidationResult(None, None)
                    self.assertIsNone(support.parse_source_version(self.root, result))
                    self.assertTrue(result.violations)
                    self.assert_rejected()

    def test_active_providers_cannot_remove_rules_or_restore_v1_prohibition(self):
        path = self.root / "AGENTS.md"
        original = path.read_text()
        for text in (original.replace("GC must never delete reachable data.", ""),
                     original + "\nV2 implementation has not started.\n"):
            path.write_text(text)
            self.assert_rejected()


class Action201AuthorityTests(V2AuthorityFixture):
    fixture_phase = "2.01"

    def test_only_declared_action_has_its_own_scope_and_future_obligations(self):
        for status in ("ACTIVE", "COMPLETE"):
            with self.subTest(status=status):
                self.change(phase_status=status)
                self.assertEqual(governance.validate(self.root), [])
                result = release.validate(self.root, "auto")
                self.assertEqual(result.violations, [])
                self.assertEqual(result.evidence_scope, "LOCAL_ACTION_2_01_AUTHORITY_ONLY")
                self.assertEqual(result.outstanding_obligations[0],
                                 "2.02-2.13" if status == "COMPLETE" else "2.01-2.13")
                self.assertIn("2.17", result.outstanding_obligations)
                self.assertTrue(governance.validate(self.root, "historical-v1"))

    def test_unknown_and_conflicting_action_fail_without_phase0_fallback(self):
        for phase in ("2.05", "2.17", "Phase 1A", "2.1", "", None, 2.01, []):
            with self.subTest(phase=phase):
                self.change(phase=phase)
                self.assert_rejected()
                self.assertEqual(release.validate(self.root, "auto").evidence_scope,
                                 "UNRESOLVED_ACTIVE_SCOPE")
        self.change(phase="2.04")  # Known bounded scope with contradictory mirrors.
        self.assert_rejected()
        self.change(phase="2.02")  # Known new scope with contradictory 2.01 mirrors.
        self.assert_rejected()
        self.change(phase="2.03")  # Known new scope with contradictory 2.01 mirrors.
        self.assert_rejected()
        self.change(phase="0")  # Known phase with contradictory 2.01 mirrors.
        self.assert_rejected()

    def test_each_stale_phase_mirror_is_rejected(self):
        for relative in support.V2_MIRRORS:
            path = self.root / relative
            original = path.read_text()
            with self.subTest(path=relative):
                path.write_text(original.replace("CURRENT_PHASE: 2.01", "CURRENT_PHASE: 0"))
                self.assert_rejected()
            path.write_text(original)

    def test_completion_never_qualifies_release_or_product(self):
        self.change(phase_status="COMPLETE")
        for requested in ("pre-release", "released", "post-release-closed"):
            self.assertTrue(release.validate(self.root, requested).violations)
        with patch.dict("os.environ", {"GITHUB_REF": "refs/tags/v2.0.0-dev.0"}):
            self.assert_rejected()
        self.change(product_gates="COMPLETE")
        self.assert_rejected()

    def test_action_preserves_corrected_history_and_version_guards(self):
        history = self.root / "docs/release/v1.13"
        moved = self.root / "relocated-history"
        history.rename(moved)
        history.symlink_to(moved, target_is_directory=True)
        self.assert_rejected()
        history.unlink()
        moved.rename(history)
        path = self.root / "internal/version/version.go"
        path.write_text(re.sub(r"(?m)^(\s*Major\s*=\s*2)$", r"\1 + 1", path.read_text()))
        self.assert_rejected()


class Action203InventoryAuthorityTests(V2AuthorityFixture):
    fixture_phase = "2.03"

    def test_inventory_scope_keeps_parent_active_and_obligations(self):
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(governance.validate(self.root), [])
        self.assertEqual(result.evidence_scope, "LOCAL_ACTION_2_03_AUTHORITY_ONLY")
        self.assertIn("2.03-recovery-copy-completion-import-isolated-restoration", result.outstanding_obligations)
        self.change(phase_status="COMPLETE")
        self.assert_rejected()

    def test_phase1d_complete_is_local_structural_authority_only(self):
        self.change(phase_status="COMPLETE")
        self.assert_rejected()
        path = self.root / "docs/release/v2.0/phase1d-result.md"
        tokens = ("Local-profile action 2.03: COMPLETE", "Profile: Linux/manual PostgreSQL 16",
                  "Product gates: INCOMPLETE", "Implementation: ACTIVE",
                  "coldkeep_phase1d_codex_2026-11-08.md")
        path.write_text("\n".join(tokens))
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(governance.validate(self.root), [])
        self.assertEqual(result.evidence_scope, "LOCAL_ACTION_2_03_AUTHORITY_ONLY")
        self.assertEqual(result.outstanding_obligations,
                         ["2.02", "2.04-2.13", "2.15", "2.17", "separate-publication-authorization"])
        for token in tokens:
            with self.subTest(missing=token):
                path.write_text("\n".join(t for t in tokens if t != token))
                self.assert_rejected()
        path.write_text("\n".join(tokens))
        self.change(product_gates="COMPLETE")
        self.assert_rejected()
        self.change(product_gates="INCOMPLETE", phase="2.04")
        self.assert_rejected()

    def test_conflicting_or_publication_scope_refused(self):
        for phase in ("0", "2.01", "2.04", "Phase 1B", None):
            self.change(phase=phase)
            self.assert_rejected()
        self.change(phase="2.03")
        for requested in ("released", "pre-release", "post-release-closed"):
            self.assertTrue(release.validate(self.root, requested).violations)
        with patch.dict("os.environ", {"GITHUB_REF": "refs/tags/v2.0.0-dev.0"}):
            self.assert_rejected()
        self.change(product_gates="COMPLETE")
        self.assert_rejected()


class VersionBoundaryTests(unittest.TestCase):
    def test_complete_version_at_every_parser_boundary(self):
        for label in ("dev.0", "alpha.1", "beta.2", "rc.1", ""):
            version = "2.0.0" + ("-" + label if label else "")
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "internal/version/version.go"
                path.parent.mkdir(parents=True)
                path.write_text(f'package version\nconst (\n Major = 2\n Minor = 0\n Patch = 0\n Prerelease = "{label}"\n)\n')
                result = support.ValidationResult(None, None)
                self.assertEqual(support.parse_source_version(root, result), version)
                self.assertEqual(result.violations, [])
                self.assertEqual(support.release_channel(version), label.split(".")[0] if label else "stable")
                self.assertEqual(release.release_paths(version), ("docs/release/v2.0", "v" + version))
                for heading in (f"## v{version} — title", f"## [{version}] — title"):
                    match = support.CHANGELOG_HEADING.fullmatch(heading)
                    self.assertIsNotNone(match)
                    self.assertEqual(match.group(1), version)
                self.assertEqual(support.version_from_release(f"`v{version} — title`"), (version, "title"))
                self.assertEqual(support.validate_distribution_identity(version, "github.com/franchoy/coldkeep/v2", "v" + version, version), [])
                self.assertTrue(support.validate_distribution_identity(version, "github.com/franchoy/coldkeep"))
                self.assertTrue(support.validate_distribution_identity(version, "github.com/franchoy/coldkeep/v2", "v" + version + ".wrong", version))
                if label:
                    self.assertTrue(support.validate_distribution_identity(version, "github.com/franchoy/coldkeep/v2", "v2.0.0", "2.0.0"))

    def test_malformed_versions_do_not_truncate_to_stable(self):
        for version in ("02.0.0", "2.00.0", "2.0.0-", "2.0.0-alpha.01", "2.0.0-rc..1", "2.0.0+", "2.0.0.1"):
            with self.subTest(version=version):
                self.assertIsNone(support.SEMVER.fullmatch(version))
                self.assertIsNone(support.CHANGELOG_HEADING.fullmatch(f"## v{version} — title"))
                self.assertIsNone(support.version_from_release(f"v{version} — title"))
                with self.assertRaises(ValueError):
                    release.release_paths(version)

    def test_ambiguous_or_unsupported_source_suffix_never_becomes_stable(self):
        for declaration in ('', 'Prerelease string = "rc.1"', 'Prerelease = suffix',
                            'Prerelease = "dev.01"', 'Prerelease = "rc.1"\n Prerelease = ""',
                            'Prerelease = "dev.0+dirty"'):
            with self.subTest(declaration=declaration), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "internal/version/version.go"
                path.parent.mkdir(parents=True)
                path.write_text(f'package version\nconst (\n Major = 2\n Minor = 0\n Patch = 0\n {declaration}\n)\n')
                result = support.ValidationResult(None, None)
                self.assertIsNone(support.parse_source_version(root, result))
                self.assertTrue(result.violations)

    def test_core_declarations_count_unsupported_duplicates(self):
        source = 'package version\nconst (\n Major = 2\n Minor = 0\n Patch = 0\n Prerelease = "dev.0"\n)\n'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "internal/version/version.go"
            path.parent.mkdir(parents=True)
            for name in ("Major", "Minor", "Patch"):
                for text in (source.replace("\n)\n", f"\n {name} int = 2\n)\n"),
                             source + f"const {name} = 2\n"):
                    with self.subTest(component=name, source=text):
                        path.write_text(text)
                        result = support.ValidationResult(None, None)
                        self.assertIsNone(support.parse_source_version(root, result))
                        self.assertTrue(result.violations)

    def test_literal_versions_preserve_trailing_line_comments(self):
        for major, minor, patch_number, prerelease in ((1, 13, 19, None), (2, 0, 0, "dev.0")):
            with self.subTest(major=major), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "internal/version/version.go"
                path.parent.mkdir(parents=True)
                source = (f"package version\nconst (\n Major = {major} // major\n"
                          f"\tMinor = {minor}\t// minor\n Patch = {patch_number}// patch\n")
                if prerelease is not None:
                    source += f' Prerelease = "{prerelease}" // channel\n'
                path.write_text(source + ")\n")
                result = support.ValidationResult(None, None)
                expected = f"{major}.{minor}.{patch_number}" + (f"-{prerelease}" if prerelease else "")
                self.assertEqual(support.parse_source_version(root, result), expected)
                self.assertEqual(result.violations, [])

    def test_literal_package_event_identity(self):
        # Deliberately independent of PROFILES: a stale profile cannot self-confirm.
        package = "github.com/franchoy/coldkeep/v2/internal/verify"
        tests = (
            "TestVerifySystemDeepPreflightFailureDoesNotInvokeDownstreamReader",
            "TestVerifySystemDeepCollectsTwoInjectedDownstreamPhysicalFaults",
            "TestVerifySystemDeepInjectedDownstreamReaderCleanPipelinePasses",
            "TestVerifySystemDeepRejectsNilDownstreamReaderAfterPreflight",
        )
        stream = [{"Package": package, "Action": "start"}]
        for name in tests:
            stream.extend({"Package": package, "Action": action, "Test": name} for action in ("run", "pass"))
        stream.append({"Package": package, "Action": "pass"})
        self.assertEqual(events.validate_events("ck014-internal-verify", stream), [])
        self.assertTrue(events.validate_events("ck014-internal-verify", stream[:-3] + stream[-1:]))
        legacy = [dict(event, Package="github.com/franchoy/coldkeep/internal/verify") for event in stream]
        self.assertTrue(events.validate_events("ck014-internal-verify", legacy))


if __name__ == "__main__":
    unittest.main()


class Action202AuthorityTests(V2AuthorityFixture):
    fixture_phase = "2.02"

    def test_exact_scope_and_later_obligations(self):
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(result.evidence_scope, "LOCAL_ACTION_2_02_AUTHORITY_ONLY")
        self.assertIn("2.04-2.13", result.outstanding_obligations)
        for phase in ("2.04", "2.17", "2.2", "future"):
            self.change(phase=phase)
            self.assert_rejected()

    def test_completion_requires_own_bounded_report_and_never_product(self):
        self.change(phase_status="COMPLETE")
        self.assert_rejected()
        report = self.root / "docs/release/v2.0/phase2-result.md"
        body = ("Local action 2.02: COMPLETE — internal delivery and Linux feasibility proof\n"
                "Product gates: INCOMPLETE\nImplementation: ACTIVE\n"
                "coldkeep_phase2_codex_2026-11-08.md\nPackaged process: PASS\n"
                "Authenticated engine journey: PASS\nLinux SQLite and PostgreSQL Docker: PASS\n")
        report.write_text(body)
        self.assert_rejected()  # The preserved submission alone cannot close P1–P3.
        followup = self.root / "docs/release/v2.0/phase2-followup-result.md"
        followup_body = ("Local action 2.02: COMPLETE — internal delivery and Linux feasibility proof\n"
                         "Product gates: INCOMPLETE\nImplementation: ACTIVE\n"
                         "coldkeep_phase2_followup_codex_2026-11-08.md\n"
                         "P1 restore destination: PASS\nP2 TCP classification: PASS\n"
                         "P3 complete-value byte bounds: PASS\nFresh integrated qualification: PASS\n")
        for line in followup_body.splitlines():
            followup.write_text(followup_body.replace(line, ""))
            self.assert_rejected()
        followup.write_text(followup_body + "x" * 65536)
        self.assert_rejected()
        followup.unlink()
        followup.symlink_to(report)
        self.assert_rejected()
        followup.unlink()
        followup.write_text(followup_body)
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertNotIn("2.02-delivery-native-proof", result.outstanding_obligations)
        for line in body.splitlines():
            report.write_text(body.replace(line, ""))
            self.assert_rejected()
        report.write_text(body + "x" * 65536)
        self.assert_rejected()
        report.unlink()
        other = self.root / "report"
        other.write_text(body)
        report.symlink_to(other)
        self.assert_rejected()
        report.unlink()
        report.write_text(body)
        self.change(product_gates="COMPLETE")
        self.assert_rejected()
        self.change(product_gates="INCOMPLETE")
        self.assertTrue(release.validate(self.root, "released").violations)


class Action204Phase3AAuthorityTests(V2AuthorityFixture):
    fixture_phase = "2.04"

    def setUp(self):
        super().setUp()
        for name in ("phase1a-result.md", "phase1d-result.md", "phase2-followup-result.md"):
            shutil.copy2(ROOT / "docs/release/v2.0" / name, self.root / "docs/release/v2.0" / name)

    def test_unit_complete_keeps_parent_active(self):
        report = self.root / "docs/release/v2.0/phase3a-result.md"
        body = ("Phase 3A unit: COMPLETE\nParent action 2.04: ACTIVE\nProduct gates: INCOMPLETE\n"
                "Implementation: ACTIVE\ncoldkeep_phase3a_codex_2026-11-08.md\n"
                "Identity/adoption/recovery qualification: PASS\n")
        self.assertEqual(release.validate(self.root, "auto").violations, [])
        self.assertIn("2.04-phase3a-qualification", release.validate(self.root, "auto").outstanding_obligations)
        report.write_text(body)
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertNotIn("2.04-phase3a-qualification", result.outstanding_obligations)
        self.assertIn("2.04-phase3a-followup-a1-a2", result.outstanding_obligations)
        self.assertIn("2.04-phase3b", result.outstanding_obligations)
        self.assertIn("2.04-phase3c", result.outstanding_obligations)
        self.change(phase_status="COMPLETE")
        self.assert_rejected()
        self.change(phase_status="ACTIVE", product_gates="COMPLETE")
        self.assert_rejected()

    def test_incomplete_reports_and_future_authority_do_not_pass(self):
        report = self.root / "docs/release/v2.0/phase3a-result.md"
        for body in ("Phase 3A unit: COMPLETE", "Phase 3A unit: ACTIVE", "x" * 65537):
            report.write_text(body)
            self.assertIn("2.04-phase3a-qualification", release.validate(self.root, "auto").outstanding_obligations)
        for phase in ("2.04.1", "2.05", "2.17", "3A"):
            self.change(phase=phase)
            self.assert_rejected()

    def test_predecessor_and_publication_fences(self):
        self.assertTrue(release.validate(self.root, "released").violations)
        (self.root / "docs/release/v2.0/phase2-followup-result.md").unlink()
        self.assert_rejected()

    def test_followup_clears_only_its_qualified_obligation(self):
        shutil.copy2(ROOT / "docs/release/v2.0/phase3a-result.md",
                     self.root / "docs/release/v2.0/phase3a-result.md")
        followup = self.root / "docs/release/v2.0/phase3a-followup-result.md"
        body = ("Phase 3A follow-up A1–A2: COMPLETE, awaiting independent review\n"
                "Parent action 2.04: ACTIVE\nImplementation: ACTIVE\nProduct gates: INCOMPLETE\n"
                "coldkeep_phase3a_followup_codex_2026-11-08.md\n"
                "A1 exact relation-bound constraint set: PASS\n"
                "A2 bounded metadata before deparsing/transfer: PASS\n"
                "Fresh recovery qualification: PASS\n")
        before = release.validate(self.root, "auto")
        self.assertEqual(before.violations, [])
        self.assertIn("2.04-phase3a-followup-a1-a2", before.outstanding_obligations)
        for text in (body.replace("COMPLETE, awaiting", "ACTIVE, awaiting"),
                     body.replace("A2 bounded metadata before deparsing/transfer: PASS", ""),
                     body.replace("Fresh recovery qualification: PASS", ""), "x" * 65537):
            followup.write_text(text)
            self.assertIn("2.04-phase3a-followup-a1-a2",
                          release.validate(self.root, "auto").outstanding_obligations)
        followup.write_text(body)
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(result.outstanding_obligations,
                         [x for x in before.outstanding_obligations if x != "2.04-phase3a-followup-a1-a2"])
        self.assertIn("2.04-phase3b", result.outstanding_obligations)
        self.assertIn("2.04-phase3c", result.outstanding_obligations)
        self.assertTrue(release.validate(self.root, "released").violations)
        self.change(phase_status="COMPLETE")
        self.assert_rejected()
        self.change(phase_status="ACTIVE", product_gates="COMPLETE")
        self.assert_rejected()
        self.change(product_gates="INCOMPLETE", phase="2.05")
        self.assert_rejected()


class Action204Phase3BAuthorityTests(Action204Phase3AAuthorityTests):
    def test_binding_cannot_close_parent_or_membership(self):
        for name in ("phase3a-result.md", "phase3a-followup-result.md"):
            shutil.copy2(ROOT / "docs/release/v2.0" / name, self.root / "docs/release/v2.0" / name)
        report = self.root / "docs/release/v2.0/phase3b-result.md"
        body = "\n".join((
            "Phase 3B unit: COMPLETE, awaiting independent review", "Parent action 2.04: ACTIVE",
            "Implementation: ACTIVE", "Product gates: INCOMPLETE", "coldkeep_phase3b_codex_2026-11-09.md",
            "Physical binding qualification: PASS", "Fresh recovery qualification: PASS",
            "Fresh integrated qualification: PASS", "Ordinary schema18/19 writers: REFUSED",
            "IMPORT_READY maintenance/activation: REFUSED", "Phase 3C: OUTSTANDING"))
        self.assertIn("2.04-phase3b", release.validate(self.root, "auto").outstanding_obligations)
        for line in body.splitlines():
            report.write_text(body.replace(line, ""))
            self.assertIn("2.04-phase3b", release.validate(self.root, "auto").outstanding_obligations)
        report.write_text(body + "x" * 65536)
        self.assertIn("2.04-phase3b", release.validate(self.root, "auto").outstanding_obligations)
        report.write_text(body)
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertNotIn("2.04-phase3b", result.outstanding_obligations)
        self.assertIn("2.04-phase3c", result.outstanding_obligations)
        self.change(phase_status="COMPLETE")
        self.assert_rejected()
        self.change(phase_status="ACTIVE", product_gates="COMPLETE")
        self.assert_rejected()


class Action204Phase3BR1AuthorityTests(V2AuthorityFixture):
    fixture_phase = "2.04"

    def setUp(self):
        super().setUp()
        for name in ("phase1a-result.md", "phase1d-result.md", "phase2-followup-result.md",
                     "phase3a-result.md", "phase3a-followup-result.md", "phase3b-result.md"):
            shutil.copy2(ROOT / "docs/release/v2.0" / name, self.root / "docs/release/v2.0" / name)
        self.followup = self.root / "docs/release/v2.0/phase3b-followup-result.md"
        # Independent literal expectations: deleting the implementation's R1
        # obligation or a completion requirement must not self-confirm here.
        self.body = "\n".join((
            "Phase 3B follow-up R1: COMPLETE, awaiting independent review",
            "R1 lossless operator root selection: PASS",
            "Submitted-source decoder and operator counterexamples: REPRODUCED",
            "Fresh recovery/operator qualification: PASS",
            "Parent action 2.04: ACTIVE", "Implementation: ACTIVE",
            "Product gates: INCOMPLETE", "Phase 3C: OUTSTANDING",
            "Ordinary schema18/19 writers: REFUSED",
            "IMPORT_READY maintenance/activation: REFUSED",
            "Publication: NOT AUTHORIZED",
            "Governing BUILD: coldkeep_phase3b_followup_codex_2026-11-09.md",
        )) + "\n"

    def assert_r1_outstanding(self):
        result = release.validate(self.root, "auto")
        self.assertIn("2.04-phase3b-followup-r1", result.outstanding_obligations)
        self.assertIn("2.04-phase3c", result.outstanding_obligations)
        self.assertIn("separate-publication-authorization", result.outstanding_obligations)
        return result

    def test_submitted_phase3b_cannot_erase_required_r1_obligation(self):
        before = self.assert_r1_outstanding()
        self.assertEqual(before.violations, [])
        self.assertNotIn("2.04-phase3b", before.outstanding_obligations)
        # Even completion-looking text in the original report is not its owner.
        submitted = self.root / "docs/release/v2.0/phase3b-result.md"
        submitted.write_text(submitted.read_text() + "\n" + self.body)
        self.assertEqual(self.assert_r1_outstanding().violations, [])
        self.followup.write_text(self.body.replace(
            "COMPLETE, awaiting independent review", "ACTIVE"))
        self.assertEqual(self.assert_r1_outstanding().violations, [])

    def test_every_completion_field_is_required_and_semantic(self):
        for line in self.body.splitlines():
            key, value = line.split(": ", 1)
            mutations = (
                self.body.replace(line + "\n", ""),
                self.body.replace(line, key + ": UNKNOWN"),
                self.body.replace(line, key + ": NOT " + value),
                self.body.replace(line, line + "-pending"),
                self.body + line + "\n",
                self.body + key + ": CONFLICT\n",
            )
            for index, body in enumerate(mutations):
                with self.subTest(field=key, mutation=index):
                    self.followup.write_text(body)
                    self.assertEqual(self.assert_r1_outstanding().violations, [])

    def test_deleted_missing_or_nonregular_followup_keeps_r1(self):
        self.followup.write_text(self.body)
        self.assertNotIn("2.04-phase3b-followup-r1",
                         release.validate(self.root, "auto").outstanding_obligations)
        self.followup.unlink()
        self.assert_r1_outstanding()
        alternate = self.root / "otherwise-valid-followup.md"
        alternate.write_text(self.body)
        self.followup.symlink_to(alternate)
        self.assert_r1_outstanding()
        self.followup.unlink()
        self.followup.mkdir()
        self.assert_r1_outstanding()
        self.followup.rmdir()
        self.followup.write_bytes(self.body.encode() + b"\xff")
        self.assert_r1_outstanding()
        self.followup.write_text(self.body + "x" * 65536)
        self.assert_r1_outstanding()

    def test_valid_followup_clears_exactly_r1_and_keeps_later_authority(self):
        before = self.assert_r1_outstanding()
        self.followup.write_text(self.body)
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(result.outstanding_obligations,
                         [item for item in before.outstanding_obligations
                          if item != "2.04-phase3b-followup-r1"])
        self.assertEqual(result.evidence_scope, "LOCAL_ACTION_2_04_PHASE3A_AUTHORITY_ONLY")
        for requested in ("pre-release", "released", "post-release-closed"):
            with self.subTest(requested=requested):
                self.assertTrue(release.validate(self.root, requested).violations)
        for change, restore in (
            ({"phase_status": "COMPLETE"}, {"phase_status": "ACTIVE"}),
            ({"product_gates": "COMPLETE"}, {"product_gates": "INCOMPLETE"}),
            ({"implementation": "COMPLETE"}, {"implementation": "ACTIVE"}),
            ({"phase": "2.05"}, {"phase": "2.04"}),
        ):
            with self.subTest(change=change):
                self.change(**change)
                self.assert_rejected()
                self.change(**restore)
        with patch.dict("os.environ", {"GITHUB_REF": "refs/tags/v2.0.0-dev.0"}):
            self.assert_rejected()

    def test_r1_cannot_clear_without_qualified_predecessor(self):
        self.followup.write_text(self.body)
        for name in ("phase3a-result.md", "phase3a-followup-result.md", "phase3b-result.md"):
            report = self.root / "docs/release/v2.0" / name
            original = report.read_bytes()
            with self.subTest(predecessor=name):
                report.unlink()
                self.assert_r1_outstanding()
                report.write_bytes(original)
