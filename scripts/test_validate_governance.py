from pathlib import Path
import hashlib
import re
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
        for relative in (
            governance.CANONICAL_CURRENT_STATE_FILE,
            Path("docs/release/v1.13/v1.13.17-phase-list.md"),
        ):
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(governance.ROOT / relative, destination)

    def assert_invalid(self) -> None:
        self.assertNotEqual(governance.validate_release_body(self.root), [])

    def set_successor_phase(self, next_phase: int | None) -> None:
        key = f"{next_phase}_NEXT" if next_phase is not None else "NONE_CLOSURE_CANDIDATE"
        state_value = (
            "READY_PRE_RELEASE" if next_phase == 5 else
            "PUBLISHED_CLOSURE_PENDING" if next_phase == 9 else
            "CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT" if next_phase is None else
            "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT"
        )
        state_path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        state = state_path.read_text(encoding="utf-8")
        state = re.sub(r"(?m)^CURRENT_PHASE: \S+$", f"CURRENT_PHASE: {key}", state, count=1)
        state = re.sub(
            r"(?m)^V1_13_17_STATE: \S+$",
            f"V1_13_17_STATE: {state_value}",
            state,
            count=1,
        )
        state_path.write_text(state, encoding="utf-8")
        phase_path = self.root / Path("docs/release/v1.13/v1.13.17-phase-list.md")
        phase = phase_path.read_text(encoding="utf-8")
        for number in range(10):
            status = (
                "Complete" if next_phase is None or number < next_phase else
                "Next" if number == next_phase else
                "Not started"
            )
            pattern = rf"(?ms)(^## Phase {number}\b.*?^\*\*Status:\*\*) (?:Complete|Next|Not started)$"
            phase, count = re.subn(pattern, rf"\1 {status}", phase, count=1)
            self.assertEqual(count, 1)
        phase_path.write_text(phase, encoding="utf-8")

    def freeze_successor_body(self, next_phase: int | None = 5) -> tuple[Path, Path]:
        body_path = self.root / governance.CURRENT_RELEASE_BODY
        checksum_path = self.root / governance.CURRENT_RELEASE_BODY_CHECKSUM
        body_path.parent.mkdir(parents=True, exist_ok=True)
        for candidate in (body_path, checksum_path):
            if candidate.is_symlink() or candidate.is_file():
                candidate.unlink()
            elif candidate.exists():
                candidate.rmdir()
        body = (
            "# v1.13.17 Annotated-Tag Certification Recovery\n\n"
            "This source-only release has no custom assets. It preserves the "
            "v1.13.15 stable state and the v1.13.16 failed publication while "
            "distinguishing inherited product repairs from this release-tool correction.\n"
        ).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest()
        body_path.write_bytes(body)
        checksum_path.write_text(
            f"{digest}  {governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            encoding="ascii",
        )
        self.set_successor_phase(next_phase)
        state_path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        state = state_path.read_text(encoding="utf-8")
        state = re.sub(
            r"(?m)^TRACKED_PUBLICATION_MATERIAL: \S+$",
            "TRACKED_PUBLICATION_MATERIAL: FROZEN",
            state,
            count=1,
        )
        state = re.sub(
            r"(?m)^RELEASE_BODY_SHA256: \S+$",
            f"RELEASE_BODY_SHA256: {digest}",
            state,
            count=1,
        )
        state_path.write_text(state, encoding="utf-8")
        return body_path, checksum_path

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

    def test_absent_successor_pair_rejects_partial_presence(self) -> None:
        body = self.root / governance.CURRENT_RELEASE_BODY
        body.parent.mkdir(parents=True, exist_ok=True)
        body.write_text("fixture\n", encoding="utf-8")
        self.assert_invalid()

    def test_absent_successor_matrix_for_phases_2_3_and_4(self) -> None:
        for phase in (2, 3, 4):
            with self.subTest(phase=phase):
                self.set_successor_phase(phase)
                self.assertEqual(governance.validate_release_body(self.root), [])

    def test_absent_successor_rejects_files_digest_and_late_phase(self) -> None:
        body = self.root / governance.CURRENT_RELEASE_BODY
        checksum = self.root / governance.CURRENT_RELEASE_BODY_CHECKSUM
        body.parent.mkdir(parents=True, exist_ok=True)
        for case in ("body", "checksum", "both", "digest", "phase"):
            with self.subTest(case=case):
                if body.exists():
                    body.unlink()
                if checksum.exists():
                    checksum.unlink()
                self.set_successor_phase(2)
                state = self.root / governance.CANONICAL_CURRENT_STATE_FILE
                if case in ("body", "both"):
                    body.write_text("fixture\n", encoding="utf-8")
                if case in ("checksum", "both"):
                    checksum.write_text("fixture\n", encoding="utf-8")
                if case == "digest":
                    state.write_text(
                        state.read_text(encoding="utf-8").replace(
                            "RELEASE_BODY_SHA256: ABSENT",
                            f"RELEASE_BODY_SHA256: {'0' * 64}",
                            1,
                        ),
                        encoding="utf-8",
                    )
                if case == "phase":
                    self.set_successor_phase(5)
                self.assert_invalid()

    def test_frozen_successor_body_passes(self) -> None:
        for phase in (5, 9, None):
            with self.subTest(phase=phase):
                self.freeze_successor_body(phase)
                self.assertEqual(governance.validate_release_body(self.root), [])

    def test_frozen_successor_byte_boundaries(self) -> None:
        mutations = {
            "empty": b"",
            "over-limit": b"x" * 65537,
            "bom": b"\xef\xbb\xbfvalid\n",
            "invalid-utf8": b"\xff\n",
            "crlf": b"valid\r\n",
            "missing-lf": b"valid",
            "extra-lf": b"valid\n\n",
            "trailing-space": b"valid \n",
        }
        for name, content in mutations.items():
            with self.subTest(name=name):
                body, _ = self.freeze_successor_body()
                body.write_bytes(content)
                self.assert_invalid()

    def test_frozen_successor_file_types_and_checksum_grammar(self) -> None:
        for case in ("body-symlink", "body-directory", "checksum-symlink", "checksum-directory"):
            with self.subTest(case=case):
                body, checksum = self.freeze_successor_body()
                selected = body if case.startswith("body") else checksum
                original = selected.read_bytes()
                selected.unlink()
                if case.endswith("symlink"):
                    target = self.root / f"TEST_FIXTURE_ONLY-{case}"
                    target.write_bytes(original)
                    selected.symlink_to(target)
                else:
                    selected.mkdir()
                self.assert_invalid()
        for value in (
            "0" * 64 + f" {governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            "g" * 64 + f"  {governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            "0" * 64 + "  wrong.md\n",
            "0" * 64 + f"  {governance.CURRENT_RELEASE_BODY.as_posix()}\nextra\n",
        ):
            with self.subTest(checksum=value):
                _, checksum = self.freeze_successor_body()
                checksum.write_text(value, encoding="ascii")
                self.assert_invalid()

    def test_frozen_successor_semantic_requirements_and_future_assertions(self) -> None:
        required = (
            "v1.13.17", "v1.13.16", "v1.13.15", "inherited",
            "failed publication", "source-only", "no custom assets",
        )
        for marker in required:
            with self.subTest(missing=marker):
                body, checksum = self.freeze_successor_body()
                text = body.read_text(encoding="utf-8")
                body.write_text(text.replace(marker, "omitted", 1), encoding="utf-8")
                digest = hashlib.sha256(body.read_bytes()).hexdigest()
                checksum.write_text(
                    f"{digest}  {governance.CURRENT_RELEASE_BODY.as_posix()}\n",
                    encoding="ascii",
                )
                state = self.root / governance.CANONICAL_CURRENT_STATE_FILE
                state.write_text(
                    re.sub(
                        r"(?m)^RELEASE_BODY_SHA256: [0-9a-f]{64}$",
                        f"RELEASE_BODY_SHA256: {digest}",
                        state.read_text(encoding="utf-8"),
                        count=1,
                    ),
                    encoding="utf-8",
                )
                self.assert_invalid()
        for assertion in (
            "M17", "run_id", "certificate is successful", "closure is established",
        ):
            with self.subTest(assertion=assertion):
                body, checksum = self.freeze_successor_body()
                body.write_text(
                    body.read_text(encoding="utf-8").removesuffix("\n")
                    + f" {assertion}\n",
                    encoding="utf-8",
                )
                digest = hashlib.sha256(body.read_bytes()).hexdigest()
                checksum.write_text(
                    f"{digest}  {governance.CURRENT_RELEASE_BODY.as_posix()}\n",
                    encoding="ascii",
                )
                state = self.root / governance.CANONICAL_CURRENT_STATE_FILE
                state.write_text(
                    re.sub(
                        r"(?m)^RELEASE_BODY_SHA256: [0-9a-f]{64}$",
                        f"RELEASE_BODY_SHA256: {digest}",
                        state.read_text(encoding="utf-8"),
                        count=1,
                    ),
                    encoding="utf-8",
                )
                self.assert_invalid()

    def test_frozen_successor_digest_mismatch_fails(self) -> None:
        body, _ = self.freeze_successor_body()
        body.write_bytes(body.read_bytes().replace(b"source-only", b"source only", 1))
        self.assert_invalid()

    def test_frozen_successor_checksum_path_mismatch_fails(self) -> None:
        _, checksum = self.freeze_successor_body()
        checksum.write_text("0" * 64 + "  wrong.md\n", encoding="ascii")
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

    def test_v11316_failed_predecessor_is_historical(self) -> None:
        self.assertEqual(
            governance.classify_path(
                Path("docs/release/v1.13/v1.13.16-release-state.md")
            ),
            "historical-release",
        )

    def test_v11317_current_authority_is_classified_current(self) -> None:
        self.assertEqual(
            governance.classify_path(
                Path("docs/release/v1.13/v1.13.17-release-state.md")
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
        self.replace_canonical_value("CURRENT_PHASE", "CURRENT_PHASE: 3_NEXT")
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
            governance.CANONICAL_CURRENT_STATE_FILE,
            "CK-V11316-015: CLOSED_AT_V1.13.16_SOURCE_SCOPE\n",
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
        current_phase = self.canonical_state()["CURRENT_PHASE"]
        self.replace_once(
            Path("AGENTS.md"),
            f"CURRENT_PHASE: {current_phase}",
            "CURRENT_PHASE: STALE_TEST_VALUE",
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
