import subprocess
import sys
import unittest
from pathlib import Path


class Phase3ContractTests(unittest.TestCase):
    def test_repository_contracts_pass(self) -> None:
        root = Path(__file__).resolve().parent.parent
        completed = subprocess.run(
            [sys.executable, str(root / "scripts" / "validate_phase3_contracts.py")],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "PHASE3_CONTAINER_CONTRACTS: PASS\n")


if __name__ == "__main__":
    unittest.main()
