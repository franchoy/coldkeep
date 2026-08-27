from pathlib import Path
import unittest

import validate_governance as governance


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


if __name__ == "__main__":
    unittest.main()
