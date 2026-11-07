"""Phase 0 authority and prerelease boundary regressions on disposable files."""
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


class V2AuthorityTests(unittest.TestCase):
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

    def test_current_and_historical_dispatch_are_distinct(self):
        self.assertEqual(governance.validate(self.root), [])
        result = release.validate(self.root, "auto")
        self.assertEqual(result.violations, [])
        self.assertEqual(result.active_version, "2.0.0-dev.0")
        self.assertEqual(result.state, "development")
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

    def test_active_providers_cannot_remove_rules_or_restore_v1_prohibition(self):
        path = self.root / "AGENTS.md"
        original = path.read_text()
        for text in (original.replace("GC must never delete reachable data.", ""),
                     original + "\nV2 implementation has not started.\n"):
            path.write_text(text)
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
