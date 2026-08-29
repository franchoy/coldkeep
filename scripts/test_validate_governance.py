from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
