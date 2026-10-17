from pathlib import Path
import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import validate_governance as governance


CURRENT_GOVERNANCE_INPUTS = tuple(dict.fromkeys(
    governance.ACTIVE_PROVIDER_FILES
    + governance.CURRENT_AUTHORITY_FILES
    + (
        governance.HISTORICAL_PROVIDER_FILE,
        governance.CANONICAL_RELEASE_BODY,
        governance.CANONICAL_RELEASE_BODY_CHECKSUM,
        governance.CURRENT_RELEASE_BODY,
        governance.CURRENT_RELEASE_BODY_CHECKSUM,
        governance.TERMINAL_DISPOSITION_REFERENCE_FILE,
    )
))


def copy_fixture_paths(
    source_root: Path,
    destination_root: Path,
    paths: tuple[Path, ...],
) -> None:
    """Faithfully copy declared paths without deriving them from a Git index."""
    for relative in paths:
        source = source_root / relative
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_symlink():
            destination.symlink_to(os.readlink(source))
        elif source.is_dir():
            destination.mkdir()
        elif source.exists():
            shutil.copy2(source, destination)


def copy_current_governance_inputs(
    source_root: Path,
    destination_root: Path,
) -> None:
    copy_fixture_paths(source_root, destination_root, CURRENT_GOVERNANCE_INPUTS)


def fixture_git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        [shutil.which("git") or "git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def replace_terminal_identity(root: Path, key: str, value: str) -> None:
    for relative, prefix in (
        (
            Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
            "EXPECTED_",
        ),
        (governance.TERMINAL_DISPOSITION_REFERENCE_FILE, ""),
    ):
        path = root / relative
        text = path.read_text(encoding="utf-8")
        text, count = re.subn(
            rf"(?m)^{prefix}{re.escape(key)}: \S+$",
            f"{prefix}{key}: {value}",
            text,
            count=1,
        )
        if count != 1:
            raise AssertionError(f"missing terminal identity {prefix}{key} in {relative}")
        path.write_text(text, encoding="utf-8")


def replace_progression_identity(root: Path, key: str, value: str) -> None:
    """Remap one progression anchor in its expectation and reference owners."""
    for relative, prefix in (
        (
            Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
            "EXPECTED_",
        ),
        (governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE, ""),
    ):
        path = root / relative
        text = path.read_text(encoding="utf-8")
        text, count = re.subn(
            rf"(?m)^{prefix}{re.escape(key)}: \S+$",
            f"{prefix}{key}: {value}",
            text,
            count=1,
        )
        if count != 1:
            raise AssertionError(
                f"missing progression identity {prefix}{key} in {relative}"
            )
        path.write_text(text, encoding="utf-8")


def prepare_owned_terminal_graph(root: Path) -> dict[str, str]:
    """Create M -> C and annotated A entirely inside the fixture object store."""
    fixture_git(root, "init", "--quiet")
    fixture_git(root, "config", "user.name", "fixture")
    fixture_git(root, "config", "user.email", "fixture@example.invalid")
    fixture_git(root, "add", ".")
    fixture_git(root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY published M17")
    published = fixture_git(root, "rev-parse", "HEAD")
    fixture_git(root, "tag", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY", published)
    annotated_tag = fixture_git(root, "rev-parse", "refs/tags/v1.13.17")
    fixture_git(root, "commit", "--quiet", "--allow-empty", "-m", "TEST_FIXTURE_ONLY accepted C")
    accepted = fixture_git(root, "rev-parse", "HEAD")
    accepted_tree = fixture_git(root, "rev-parse", "HEAD^{tree}")
    values = {
        "ACCEPTED_C": accepted,
        "ACCEPTED_C_TREE": accepted_tree,
        "PUBLISHED_M17": published,
        "ANNOTATED_TAG_A17": annotated_tag,
        "TAG_DIRECT_TARGET": published,
    }
    for key, value in values.items():
        replace_terminal_identity(root, key, value)
    fixture_git(
        root,
        "add",
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md",
        governance.TERMINAL_DISPOSITION_REFERENCE_FILE.as_posix(),
    )
    fixture_git(
        root,
        "commit",
        "--quiet",
        "-m",
        "TEST_FIXTURE_ONLY historical H1",
    )
    historical_h1 = fixture_git(root, "rev-parse", "HEAD")
    historical_h1_tree = fixture_git(root, "rev-parse", "HEAD^{tree}")
    progression_values = {
        "BASE_C": accepted,
        "BASE_C_TREE": accepted_tree,
        "HISTORICAL_H1": historical_h1,
        "HISTORICAL_H1_PARENT": accepted,
        "HISTORICAL_H1_TREE": historical_h1_tree,
    }
    for key, value in progression_values.items():
        replace_progression_identity(root, key, value)
    fixture_git(
        root,
        "add",
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md",
        governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE.as_posix(),
    )
    fixture_git(root, "commit", "--quiet", "--allow-empty", "-m", "TEST_FIXTURE_ONLY published H2")
    published_h2 = fixture_git(root, "rev-parse", "HEAD")
    published_h2_tree = fixture_git(root, "rev-parse", "HEAD^{tree}")
    h2_values = {
        "PUBLISHED_H2": published_h2,
        "PUBLISHED_H2_PARENT": historical_h1,
        "PUBLISHED_H2_TREE": published_h2_tree,
        "SUCCESSOR_PARENT": published_h2,
    }
    for key, value in h2_values.items():
        replace_progression_identity(root, key, value)
    progression_reference = (
        root / governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE
    ).read_bytes()
    progression_digest = hashlib.sha256(progression_reference).hexdigest()
    contract = root / Path(
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
    )
    contract_text, count = re.subn(
        r"(?m)^EXPECTED_REFERENCE_SHA256: [0-9a-f]{64}$",
        f"EXPECTED_REFERENCE_SHA256: {progression_digest}",
        contract.read_text(encoding="utf-8"),
        count=1,
    )
    if count != 1:
        raise AssertionError("missing progression reference digest")
    contract.write_text(contract_text, encoding="utf-8")
    values.update(progression_values)
    values.update(h2_values)
    return values


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
            governance.CURRENT_RELEASE_BODY,
            governance.CURRENT_RELEASE_BODY_CHECKSUM,
        ):
            copy_fixture_paths(governance.ROOT, self.root, (relative,))

    def replace_state_value(self, key: str, value: str) -> None:
        state_path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        state = state_path.read_text(encoding="utf-8")
        state, count = re.subn(
            rf"(?m)^{re.escape(key)}: \S+$",
            f"{key}: {value}",
            state,
            count=1,
        )
        self.assertEqual(count, 1, key)
        state_path.write_text(state, encoding="utf-8")

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
        self.replace_state_value("CURRENT_PHASE", key)
        self.replace_state_value("V1_13_17_STATE", state_value)
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
        self.replace_state_value("TRACKED_PUBLICATION_MATERIAL", "FROZEN")
        self.replace_state_value("RELEASE_BODY_SHA256", digest)
        return body_path, checksum_path

    def synchronize_successor_body(self, content: bytes) -> list[str]:
        body, checksum = self.freeze_successor_body()
        body.write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()
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
        return governance.validate_release_body(self.root)

    def reset_absent_successor(self, next_phase: int | None) -> None:
        for relative in (
            governance.CURRENT_RELEASE_BODY,
            governance.CURRENT_RELEASE_BODY_CHECKSUM,
        ):
            candidate = self.root / relative
            if candidate.is_symlink() or candidate.is_file():
                candidate.unlink()
            elif candidate.exists():
                candidate.rmdir()
        self.set_successor_phase(next_phase)
        self.replace_state_value("TRACKED_PUBLICATION_MATERIAL", "ABSENT")
        self.replace_state_value("RELEASE_BODY_SHA256", "ABSENT")

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
        self.reset_absent_successor(2)
        body = self.root / governance.CURRENT_RELEASE_BODY
        body.parent.mkdir(parents=True, exist_ok=True)
        body.write_text("fixture\n", encoding="utf-8")
        self.assert_invalid()

    def test_absent_successor_matrix_for_phases_2_3_and_4(self) -> None:
        for phase in (2, 3, 4):
            with self.subTest(phase=phase):
                self.reset_absent_successor(phase)
                self.assertEqual(governance.validate_release_body(self.root), [])

    def test_absent_successor_rejects_files_digest_and_late_phase(self) -> None:
        body = self.root / governance.CURRENT_RELEASE_BODY
        checksum = self.root / governance.CURRENT_RELEASE_BODY_CHECKSUM
        body.parent.mkdir(parents=True, exist_ok=True)
        for case in ("body", "checksum", "both", "digest", "phase"):
            with self.subTest(case=case):
                self.reset_absent_successor(2)
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
        body, _ = self.freeze_successor_body()
        valid = body.read_bytes()
        exact_limit = valid.removesuffix(b"\n")
        exact_limit += b"x" * (65535 - len(exact_limit)) + b"\n"
        self.assertEqual(len(exact_limit), 65536)
        self.assertEqual(self.synchronize_successor_body(exact_limit), [])

        cases = (
            ("empty", b"", "body size must be 1..65536 bytes", True),
            ("one-byte", b"\n", "body size must be 1..65536 bytes", False),
            (
                "over-limit",
                exact_limit.removesuffix(b"\n") + b"x\n",
                "body size must be 1..65536 bytes",
                True,
            ),
            ("bom", b"\xef\xbb\xbf" + valid, "UTF-8 BOM is forbidden", True),
            ("invalid-utf8", valid.removesuffix(b"\n") + b"\xff\n", "invalid UTF-8", True),
            ("crlf", valid.replace(b"\n", b"\r\n", 1), "only LF newlines are allowed", True),
            ("missing-lf", valid.removesuffix(b"\n"), "exactly one terminal LF is required", True),
            ("extra-lf", valid + b"\n", "exactly one terminal LF is required", True),
            ("trailing-space", valid.removesuffix(b"\n") + b" \n", "trailing whitespace is forbidden", True),
        )
        for name, content, message, present in cases:
            with self.subTest(name=name):
                violations = self.synchronize_successor_body(content)
                matches = [item for item in violations if message in item]
                if present:
                    self.assertEqual(len(matches), 1, violations)
                else:
                    self.assertEqual(matches, [], violations)
                    self.assertTrue(
                        any("missing semantic marker" in item for item in violations),
                        violations,
                    )
                self.assertFalse(
                    any("digest" in item or "checksum" in item for item in violations),
                    violations,
                )

    def test_invalid_successor_material_phase_cross_product(self) -> None:
        for phase in (5, 9, None):
            with self.subTest(material="ABSENT", phase=phase):
                self.reset_absent_successor(phase)
                violations = governance.validate_release_body(self.root)
                key = f"{phase}_NEXT" if phase is not None else "NONE_CLOSURE_CANDIDATE"
                self.assertIn(
                    f"{governance.CANONICAL_CURRENT_STATE_FILE}: ABSENT publication material is invalid for {key}",
                    violations,
                )
        for phase in (2, 3, 4):
            with self.subTest(material="FROZEN", phase=phase):
                self.freeze_successor_body(phase)
                self.assertIn(
                    f"{governance.CANONICAL_CURRENT_STATE_FILE}: publication material state and phase are inconsistent",
                    governance.validate_release_body(self.root),
                )

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
                relative = (
                    governance.CURRENT_RELEASE_BODY
                    if case.startswith("body")
                    else governance.CURRENT_RELEASE_BODY_CHECKSUM
                )
                self.assertIn(
                    f"{relative}: required regular non-symlink file is missing",
                    governance.validate_release_body(self.root),
                )
        for case in ("malformed", "wrong-digest", "wrong-path", "extra-line"):
            with self.subTest(checksum=case):
                body, checksum = self.freeze_successor_body()
                digest = hashlib.sha256(body.read_bytes()).hexdigest()
                values = {
                    "malformed": "not a checksum\n",
                    "wrong-digest": "0" * 64 + f"  {governance.CURRENT_RELEASE_BODY.as_posix()}\n",
                    "wrong-path": f"{digest}  wrong.md\n",
                    "extra-line": f"{digest}  {governance.CURRENT_RELEASE_BODY.as_posix()}\nextra\n",
                }
                value = values[case]
                checksum.write_text(value, encoding="ascii")
                self.assertIn(
                    f"{governance.CURRENT_RELEASE_BODY_CHECKSUM}: bytes do not match body digest and exact path",
                    governance.validate_release_body(self.root),
                )

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
        copy_current_governance_inputs(governance.ROOT, self.root)
        self.identities = prepare_owned_terminal_graph(self.root)
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

    def test_security_progression_reference_is_exact_and_selected(self) -> None:
        """SP-GOV-01: the pointer, remapped anchors, and digest parse together."""
        state = self.canonical_state()
        self.assertEqual(
            state["SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE"],
            governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE.as_posix(),
        )
        self.assertEqual(governance.validate(self.root), [])

    def test_security_progression_partial_and_mutated_material_fails(self) -> None:
        """SP-GOV-02: selected progression material has no downgrade route."""
        state_path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        reference_path = (
            self.root / governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE
        )
        state_original = state_path.read_text(encoding="utf-8")
        reference_original = reference_path.read_bytes()

        state_path.write_text(
            re.sub(
                r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE: .*\n",
                "",
                state_original,
                count=1,
            ),
            encoding="utf-8",
        )
        self.assert_governance_fails()
        state_path.write_text(state_original, encoding="utf-8")

        reference_path.write_bytes(
            reference_original.replace(b"DELTA_COUNT: 9", b"DELTA_COUNT: 8", 1)
        )
        self.assert_governance_fails()
        reference_path.write_bytes(reference_original)

        reference_path.unlink()
        violations = governance.validate(self.root)
        self.assertTrue(
            any("security progression reference unavailable:" in item for item in violations),
            violations,
        )

    def test_security_progression_selected_mixed_and_absent_matrix(self) -> None:
        """SP-GOV-03: all three selectors are atomic; total absence is historical."""
        state_path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        contract_path = self.root / Path(
            "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
        )
        reference_path = (
            self.root / governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE
        )
        originals = {
            "state": state_path.read_text(encoding="utf-8"),
            "contract": contract_path.read_text(encoding="utf-8"),
            "reference": reference_path.read_bytes(),
        }

        def remove_pointer() -> None:
            state_path.write_text(
                re.sub(
                    r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_(?:SCHEMA|REFERENCE): .*\n",
                    "",
                    originals["state"],
                ),
                encoding="utf-8",
            )

        def remove_expectation() -> None:
            contract_path.write_text(
                re.sub(
                    r"(?ms)^<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->\n.*?^<!-- coldkeep-security-toolchain-progression-v2-expectations:end -->\n?",
                    "",
                    originals["contract"],
                    count=1,
                ),
                encoding="utf-8",
            )

        def texts() -> dict[Path, str]:
            result = {}
            for relative in (
                governance.CANONICAL_CURRENT_STATE_FILE,
                Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
                governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE,
            ):
                path = self.root / relative
                if path.is_file() and not path.is_symlink():
                    result[relative] = path.read_text(encoding="utf-8")
            return result

        self.assertEqual(
            governance.validate_security_progression_contract(self.root, texts()),
            [],
        )
        for name, pointer, expectation, reference in (
            ("expectation-and-reference", False, True, True),
            ("pointer-and-reference", True, False, True),
            ("pointer-only", True, False, False),
            ("expectation-only", False, True, False),
            ("reference-only", False, False, True),
        ):
            with self.subTest(name=name):
                state_path.write_text(originals["state"], encoding="utf-8")
                contract_path.write_text(originals["contract"], encoding="utf-8")
                reference_path.write_bytes(originals["reference"])
                if not pointer:
                    remove_pointer()
                if not expectation:
                    remove_expectation()
                if not reference:
                    reference_path.unlink()
                violations = governance.validate_security_progression_contract(
                    self.root, texts()
                )
                self.assertNotEqual(violations, [])
                self.assertTrue(
                    any("selected together" in item for item in violations),
                    violations,
                )

        state_path.write_text(originals["state"], encoding="utf-8")
        contract_path.write_text(originals["contract"], encoding="utf-8")
        if reference_path.exists() or reference_path.is_symlink():
            reference_path.unlink()
        remove_pointer()
        remove_expectation()
        self.assertEqual(
            governance.validate_security_progression_contract(self.root, texts()),
            [],
        )

    def test_terminal_serialization_vocabulary_is_accepted(self) -> None:
        """TS-GOV-01: the exact canonical/eight-mirror terminal state is valid."""
        state = self.canonical_state()
        self.assertEqual(
            state["CURRENT_PHASE"], "NONE_TERMINAL_DISPOSITION_DECLARED"
        )
        self.assertEqual(
            state["TRACKED_TERMINAL_SERIALIZATION"],
            "DECLARATION_PRESENT_NON_AUTHORIZING",
        )
        self.assertEqual(governance.validate(self.root), [])

    def test_terminal_serialization_keys_fail_closed(self) -> None:
        """TS-GOV-02: accepted terminal state fails on one-key mutations."""
        cases = (
            ("missing", "TRACKED_TERMINAL_SERIALIZATION", ""),
            (
                "unsupported",
                "TRACKED_TERMINAL_SERIALIZATION",
                "TRACKED_TERMINAL_SERIALIZATION: EFFECTIVE",
            ),
            (
                "duplicate",
                "EXTERNAL_TERMINAL_DISPOSITION",
                "EXTERNAL_TERMINAL_DISPOSITION: ESTABLISHED_AT_C_SOURCE_SCOPED_QUALIFIED\n"
                "EXTERNAL_TERMINAL_DISPOSITION: ESTABLISHED_AT_C_SOURCE_SCOPED_QUALIFIED",
            ),
        )
        for name, key, replacement in cases:
            with self.subTest(name=name):
                self.assertEqual(governance.validate(self.root), [])
                path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
                original = path.read_text(encoding="utf-8")
                value = governance.canonical_current_state(original)[0][key]
                path.write_text(
                    original.replace(f"{key}: {value}", replacement, 1),
                    encoding="utf-8",
                )
                self.assertNotEqual(governance.validate(self.root), [])
                path.write_text(original, encoding="utf-8")

    def test_selected_terminal_requires_expectation_block(self) -> None:
        """TS-GOV-02: selected terminal vocabulary cannot omit expectations."""
        self.assertEqual(governance.validate(self.root), [])
        contract = self.root / Path(
            "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
        )
        text = contract.read_text(encoding="utf-8")
        start = "<!-- coldkeep-terminal-disposition-expectations:start -->"
        end = "<!-- coldkeep-terminal-disposition-expectations:end -->"
        before, selected, remainder = text.partition(start)
        self.assertTrue(selected)
        _, selected_end, after = remainder.partition(end)
        self.assertTrue(selected_end)
        contract.write_text(before + after, encoding="utf-8")
        violations = governance.validate_terminal_disposition_contract(
            self.root,
            {
                governance.CANONICAL_CURRENT_STATE_FILE: (
                    self.root / governance.CANONICAL_CURRENT_STATE_FILE
                ).read_text(encoding="utf-8"),
                Path(
                    "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
                ): contract.read_text(encoding="utf-8"),
            },
        )
        self.assertTrue(
            any("expectation block is missing" in item for item in violations),
            violations,
        )

    def test_selected_terminal_malformed_expectation_marker_fails(self) -> None:
        self.assertEqual(governance.validate(self.root), [])
        contract = self.root / Path(
            "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
        )
        contract.write_text(
            contract.read_text(encoding="utf-8").replace(
                "coldkeep-terminal-disposition-expectations:end",
                "coldkeep-terminal-disposition-expectations:wrong",
                1,
            ),
            encoding="utf-8",
        )
        violations = governance.validate(self.root)
        self.assertTrue(any("reference invalid" in item for item in violations), violations)

    def test_fixture_owns_its_object_graph(self) -> None:
        self.assertEqual(governance.validate(self.root), [])
        for relative in (
            ".git/objects/info/alternates",
            ".git/info/grafts",
        ):
            self.assertFalse((self.root / relative).exists(), relative)
        replacements = fixture_git(
            self.root, "for-each-ref", "--format=%(refname)", "refs/replace"
        )
        self.assertEqual(replacements, "")
        self.assertEqual(
            fixture_git(
                self.root,
                "merge-base",
                "--is-ancestor",
                self.identities["PUBLISHED_M17"],
                self.identities["ACCEPTED_C"],
            ),
            "",
        )

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

    def test_current_pair_copy_is_independent_of_index_membership(self) -> None:
        pair = (
            governance.CURRENT_RELEASE_BODY,
            governance.CURRENT_RELEASE_BODY_CHECKSUM,
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            copy_fixture_paths(governance.ROOT, source, pair)
            git = shutil.which("git")
            self.assertIsNotNone(git)
            subprocess.run([git, "-C", str(source), "init"], check=True, capture_output=True)
            copies = []
            for index_state in ("untracked", "tracked"):
                if index_state == "tracked":
                    subprocess.run(
                        [git, "-C", str(source), "add", *(item.as_posix() for item in pair)],
                        check=True,
                        capture_output=True,
                    )
                destination = Path(directory) / index_state
                copy_fixture_paths(source, destination, pair)
                copies.append(tuple((destination / item).read_bytes() for item in pair))
            self.assertEqual(copies[0], copies[1])
            self.assertEqual(
                copies[0], tuple((governance.ROOT / item).read_bytes() for item in pair)
            )

    def test_current_copy_does_not_heal_missing_partial_or_inconsistent_pair(self) -> None:
        pair = (
            governance.CURRENT_RELEASE_BODY,
            governance.CURRENT_RELEASE_BODY_CHECKSUM,
        )
        for case in ("missing", "partial", "inconsistent"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / "source"
                source.mkdir()
                copy_fixture_paths(governance.ROOT, source, CURRENT_GOVERNANCE_INPUTS)
                body, checksum = (source / item for item in pair)
                if case == "missing":
                    body.unlink()
                    checksum.unlink()
                elif case == "partial":
                    checksum.unlink()
                else:
                    checksum.write_text(
                        "0" * 64 + f"  {pair[0].as_posix()}\n",
                        encoding="ascii",
                    )
                destination = Path(directory) / "copy"
                copy_current_governance_inputs(source, destination)
                self.assertEqual((destination / pair[0]).exists(), case != "missing")
                self.assertEqual((destination / pair[1]).exists(), case == "inconsistent")
                self.assertNotEqual(governance.validate(destination), [])

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


class SecurityProgressionV2GovernanceTests(unittest.TestCase):
    """Explicit governance coverage for the closed v2 progression selection."""

    EXPECTED_PATHS = (
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md",
        "docs/release/v1.13/v1.13.17-release-state.md",
        "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md",
        "docs/release/v1.13/v1.13.17-source-test-allowlist.md",
        "scripts/release_state_support.py", "scripts/test_validate_governance.py",
        "scripts/test_validate_release_state.py", "scripts/validate_governance.py",
        "scripts/validate_release_state.py",
    )

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        copy_current_governance_inputs(governance.ROOT, self.root)
        self.identities = prepare_owned_terminal_graph(self.root)

    def fixture_texts(self) -> dict[Path, str]:
        paths = (
            governance.CANONICAL_CURRENT_STATE_FILE,
            Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
            governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE,
        )
        return {relative: (self.root / relative).read_text(encoding="utf-8") for relative in paths}

    def test_v2_selection_reference_and_expectations_are_exact(self) -> None:
        state, violations = governance.canonical_current_state(
            self.fixture_texts()[governance.CANONICAL_CURRENT_STATE_FILE]
        )
        self.assertEqual(violations, [])
        self.assertEqual(state["SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA"], "coldkeep-v1-security-toolchain-progression-selection/v2")
        self.assertEqual(governance.validate_security_progression_contract(self.root, self.fixture_texts()), [])

    def test_v2_reference_digest_and_closed_nine_path_inventory_match(self) -> None:
        reference = (self.root / governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE).read_bytes()
        contract = self.fixture_texts()[Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md")]
        self.assertIn(f"EXPECTED_REFERENCE_SHA256: {hashlib.sha256(reference).hexdigest()}", contract)
        rows = re.findall(r"(?m)^M (100644|100755) (\S+)$", reference.decode("utf-8"))
        self.assertEqual(tuple(path for _, path in rows), self.EXPECTED_PATHS)
        self.assertEqual(rows[7][0], "100755")

    def test_v2_partial_mixed_unknown_and_duplicate_selection_rejects(self) -> None:
        state_path = self.root / governance.CANONICAL_CURRENT_STATE_FILE
        original = state_path.read_text(encoding="utf-8")
        replacements = (
            "", "SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: unknown/v2\n",
            "SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: coldkeep-v1-security-toolchain-progression-selection/v2\n" * 2,
        )
        for replacement in replacements:
            with self.subTest(replacement=replacement):
                state_path.write_text(re.sub(r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: .*\n", replacement, original, count=1), encoding="utf-8")
                self.assertNotEqual(governance.validate_security_progression_contract(self.root, self.fixture_texts()), [])
        state_path.write_text(original, encoding="utf-8")

    def test_v2_historical_absence_and_v1_object_compatibility_are_preserved(self) -> None:
        source = governance.ROOT
        state = fixture_git(source, "show", "HEAD:docs/release/v1.13/v1.13.17-release-state.md")
        contract = fixture_git(source, "show", "HEAD:docs/release/v1.13/v1.13.17-release-state-validator-contract.md")
        reference = fixture_git(source, "show", "HEAD:docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md")
        self.assertNotIn("SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA:", state)
        self.assertIn("coldkeep-v1-security-toolchain-progression-expectations/v1", contract)
        self.assertIn("coldkeep-v1-security-toolchain-progression-reference/v1", reference)

if __name__ == "__main__":
    unittest.main()
