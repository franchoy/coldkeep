from pathlib import Path
import shutil
import tempfile
import unittest

import validate_governance as governance


class ReleaseBodyValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.body = self.root / governance.CANONICAL_RELEASE_BODY
        self.checksum = self.root / governance.CANONICAL_RELEASE_BODY_CHECKSUM
        self.body.parent.mkdir(parents=True)
        self.body.write_bytes(
            (governance.ROOT / governance.CANONICAL_RELEASE_BODY).read_bytes()
        )
        self.checksum.write_bytes(
            (governance.ROOT / governance.CANONICAL_RELEASE_BODY_CHECKSUM).read_bytes()
        )

    def assert_invalid(self) -> None:
        self.assertNotEqual(governance.validate_release_body(self.root), [])

    def test_exact_valid_identity_passes(self) -> None:
        self.assertEqual(governance.validate_release_body(self.root), [])

    def test_one_byte_body_drift_fails(self) -> None:
        body = self.body.read_bytes()
        self.body.write_bytes(body.replace(b"Coldkeep", b"coldkeep", 1))
        self.assert_invalid()

    def test_crlf_conversion_fails(self) -> None:
        self.body.write_bytes(self.body.read_bytes().replace(b"\n", b"\r\n"))
        self.assert_invalid()

    def test_missing_body_terminal_lf_fails(self) -> None:
        self.body.write_bytes(self.body.read_bytes().removesuffix(b"\n"))
        self.assert_invalid()

    def test_extra_body_terminal_lf_fails(self) -> None:
        self.body.write_bytes(self.body.read_bytes() + b"\n")
        self.assert_invalid()

    def test_utf8_bom_fails(self) -> None:
        self.body.write_bytes(b"\xef\xbb\xbf" + self.body.read_bytes())
        self.assert_invalid()

    def test_malformed_checksum_fails(self) -> None:
        self.checksum.write_bytes(b"not a checksum\n")
        self.assert_invalid()

    def test_changed_checksum_digest_fails(self) -> None:
        self.checksum.write_bytes(
            self.checksum.read_bytes().replace(
                governance.CANONICAL_RELEASE_BODY_SHA256.encode("ascii"), b"0" * 64
            )
        )
        self.assert_invalid()

    def test_changed_checksum_path_fails(self) -> None:
        self.checksum.write_bytes(
            self.checksum.read_bytes().replace(
                governance.CANONICAL_RELEASE_BODY.as_posix().encode("ascii"),
                b"docs/release/v1.13/not-the-body.md",
            )
        )
        self.assert_invalid()

    def test_missing_body_fails(self) -> None:
        self.body.unlink()
        self.assert_invalid()

    def test_missing_checksum_fails(self) -> None:
        self.checksum.unlink()
        self.assert_invalid()

    def test_body_symlink_fails(self) -> None:
        target = self.root / "body-target"
        target.write_bytes(self.body.read_bytes())
        self.body.unlink()
        self.body.symlink_to(target)
        self.assert_invalid()

    def test_checksum_symlink_fails(self) -> None:
        target = self.root / "checksum-target"
        target.write_bytes(self.checksum.read_bytes())
        self.checksum.unlink()
        self.checksum.symlink_to(target)
        self.assert_invalid()


class GovernanceValidatorTests(unittest.TestCase):
    def test_repository_contracts_pass(self) -> None:
        self.assertEqual(governance.validate(), [])

    def test_stale_active_provider_context_fails(self) -> None:
        violations = governance.active_text_violations(
            Path(".github/instructions/ci.instructions.md"),
            "During v1.10.x, change the gate.",
        )
        self.assertEqual(len(violations), 1)

    def test_historical_provider_is_classified_separately(self) -> None:
        self.assertEqual(
            governance.classify_path(Path(".github/prompts/v110-phase.prompt.md")),
            "historical-provider",
        )

    def test_completed_release_evidence_is_historical(self) -> None:
        self.assertEqual(
            governance.classify_path(
                Path(
                    "docs/release/v1.13/"
                    "v1.13.14-phase26-post-publication-truth-reconciliation-and-final-cleanup.md"
                )
            ),
            "historical-release",
        )

    def test_v11316_current_authority_is_classified_current(self) -> None:
        self.assertEqual(
            governance.classify_path(
                Path("docs/release/v1.13/v1.13.16-release-state.md")
            ),
            "current-authority",
        )

    def test_v11315_release_control_is_historical(self) -> None:
        self.assertEqual(
            governance.classify_path(
                Path("docs/release/v1.13/v1.13.15-release-state.md")
            ),
            "historical-release",
        )

    def test_stale_v11315_active_provider_wording_fails(self) -> None:
        violations = governance.active_text_violations(
            Path(".github/copilot-instructions.md"),
            "The active v1.13.15 final v1.x closure train is authoritative.",
        )
        self.assertEqual(len(violations), 1)


class CurrentStateGovernanceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        inputs = (
            governance.ACTIVE_PROVIDER_FILES
            + governance.CURRENT_AUTHORITY_FILES
            + (
                governance.HISTORICAL_PROVIDER_FILE,
                governance.CANONICAL_RELEASE_BODY,
                governance.CANONICAL_RELEASE_BODY_CHECKSUM,
            )
        )
        for relative in inputs:
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(governance.ROOT / relative, destination)
        fixture_mirror = governance_test_mirror_from_canonical(self.root)
        for relative in GOVERNANCE_TEST_MIRROR_FILES:
            path = self.root / relative
            text = path.read_text(encoding="utf-8")
            if "<!-- coldkeep-current-state:start -->" in text:
                text = governance_test_remove_mirror(text)
            path.write_text(fixture_mirror + "\n\n" + text, encoding="utf-8")

    def replace_once(self, relative: Path, old: str, new: str) -> None:
        path = self.root / relative
        text = path.read_text(encoding="utf-8")
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def assert_governance_fails(self) -> None:
        self.assertNotEqual(governance.validate(self.root), [])

    def canonical_state(self) -> dict[str, str]:
        path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        state, violations = governance.canonical_current_state(
            path.read_text(encoding="utf-8")
        )
        self.assertEqual(violations, [])
        return state

    def replace_canonical_value(self, key: str, replacement: str) -> None:
        path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        text = path.read_text(encoding="utf-8")
        preamble, separator, remainder = text.partition("\n## ")
        self.assertEqual(separator, "\n## ")
        current = f"{key}: {self.canonical_state()[key]}"
        self.assertEqual(preamble.count(current), 1)
        path.write_text(
            preamble.replace(current, replacement, 1) + separator + remainder,
            encoding="utf-8",
        )

    def test_current_repository_authority_passes(self) -> None:
        self.assertEqual(governance.validate(self.root), [])

    def test_stale_current_readme_prose_fails(self) -> None:
        self.replace_once(
            Path("README.md"),
            "## Current release state\n",
            "## Current release state\n\n"
            "The active train has seven confirmed findings and none closed. "
            "Phase 2 is Next.\n",
        )
        self.assert_governance_fails()

    def test_historical_stale_prose_remains_accepted(self) -> None:
        historical = self.root / governance.HISTORICAL_PROVIDER_FILE
        historical.write_text(
            historical.read_text(encoding="utf-8")
            + "\nHistorical quote: FINDINGS_CLOSED: 0/7; Phase 2 is Next.\n",
            encoding="utf-8",
        )
        self.assertEqual(governance.validate(self.root), [])

    def test_future_canonical_state_requires_mirror_updates(self) -> None:
        self.replace_canonical_value("PHASE_13", "PHASE_13: FUTURE")
        self.assert_governance_fails()

    def test_missing_required_canonical_field_fails(self) -> None:
        self.replace_canonical_value("SOURCE_VERSION", "")
        self.assert_governance_fails()

    def test_duplicate_required_canonical_field_fails(self) -> None:
        source_version = self.canonical_state()["SOURCE_VERSION"]
        self.replace_canonical_value(
            "SOURCE_VERSION",
            f"SOURCE_VERSION: {source_version}\nSOURCE_VERSION: {source_version}",
        )
        self.assert_governance_fails()

    def test_malformed_confirmed_count_fails(self) -> None:
        self.replace_canonical_value(
            "FINDINGS_CONFIRMED", "FINDINGS_CONFIRMED: malformed"
        )
        self.assert_governance_fails()

    def test_findings_denominator_mismatch_fails(self) -> None:
        closed, total = self.canonical_state()["FINDINGS_CLOSED"].split("/")
        self.replace_canonical_value(
            "FINDINGS_CLOSED",
            f"FINDINGS_CLOSED: {closed}/{int(total) + 1}",
        )
        self.assert_governance_fails()

    def test_ck_row_aggregate_mismatch_fails(self) -> None:
        self.replace_once(
            Path("docs/release/v1.13/v1.13.16-release-state.md"),
            "CK-V11316-015: CLOSED_CERTIFIED\n",
            "",
        )
        self.assert_governance_fails()

    def test_missing_live_mirror_fails(self) -> None:
        path = self.root / Path("AGENTS.md")
        text = path.read_text(encoding="utf-8")
        text = governance_test_remove_mirror(text)
        path.write_text(text, encoding="utf-8")
        self.assert_governance_fails()

    def test_duplicate_live_mirror_fails(self) -> None:
        path = self.root / Path("AGENTS.md")
        text = path.read_text(encoding="utf-8")
        mirror = governance_test_extract_mirror(text)
        path.write_text(text + "\n" + mirror, encoding="utf-8")
        self.assert_governance_fails()

    def test_stale_live_mirror_fails(self) -> None:
        phase_13 = self.canonical_state()["PHASE_13"]
        self.replace_once(
            Path("AGENTS.md"),
            f"PHASE_13: {phase_13}",
            "PHASE_13: STALE_TEST_VALUE",
        )
        self.assert_governance_fails()


def governance_test_extract_mirror(text: str) -> str:
    start_marker = "<!-- coldkeep-current-state:start -->"
    end_marker = "<!-- coldkeep-current-state:end -->"
    start = text.index(start_marker)
    end = text.index(end_marker, start) + len(end_marker)
    return text[start:end]


def governance_test_remove_mirror(text: str) -> str:
    return text.replace(governance_test_extract_mirror(text), "", 1)


def governance_test_mirror_from_canonical(root: Path) -> str:
    canonical = root / governance.CANONICAL_CURRENT_STATE_FILE
    state, violations = governance.canonical_current_state(
        canonical.read_text(encoding="utf-8")
    )
    if violations:
        raise AssertionError("; ".join(violations))
    payload = "\n".join(
        f"{key}: {state[key]}" for key in governance.CURRENT_STATE_KEYS
    )
    return (
        f"{governance.CURRENT_STATE_MIRROR_START}\n"
        f"```text\n{payload}\n```\n"
        f"{governance.CURRENT_STATE_MIRROR_END}"
    )


GOVERNANCE_TEST_MIRROR_FILES = (
    Path("AGENTS.md"),
    Path(".github/copilot-instructions.md"),
    Path(".github/instructions/ci.instructions.md"),
    Path(".github/prompts/critical-path-coverage.prompt.md"),
    Path(".github/prompts/regression-fix.prompt.md"),
    Path("README.md"),
    Path("SECURITY.md"),
    Path("docs/release/v1.13/README.md"),
)

if __name__ == "__main__":
    unittest.main()
