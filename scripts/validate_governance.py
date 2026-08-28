#!/usr/bin/env python3
"""Deterministically validate current Coldkeep repository authority."""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent

ACTIVE_PROVIDER_FILES = (
    Path("AGENTS.md"),
    Path(".github/copilot-instructions.md"),
    Path(".github/instructions/ci.instructions.md"),
    Path(".github/prompts/critical-path-coverage.prompt.md"),
    Path(".github/prompts/regression-fix.prompt.md"),
)
CURRENT_AUTHORITY_FILES = (
    Path("README.md"),
    Path("SECURITY.md"),
    Path("CONTRIBUTING.md"),
    Path("PRE_RELEASE_CHECKLIST.md"),
    Path("docs/architecture/engine-boundary-plan.md"),
    Path("docs/release/v1.13/README.md"),
    Path("docs/release/v1.13/v1.13.x-release-train.md"),
    Path("docs/release/v1.13/v1.13.15-scope.md"),
    Path("docs/release/v1.13/v1.13.15-phase-list.md"),
    Path("docs/release/v1.13/v1.13.15-release-state.md"),
)
HISTORICAL_PROVIDER_FILE = Path(".github/prompts/v110-phase.prompt.md")
CANONICAL_RELEASE_BODY = Path(
    "docs/release/v1.13/v1.13.15-release-body.md"
)
CANONICAL_RELEASE_BODY_CHECKSUM = Path(
    "docs/release/v1.13/v1.13.15-release-body.sha256"
)
CANONICAL_RELEASE_BODY_SHA256 = (
    "477796fc1c44151ddc77825559c48876c49ab742540586a190abc2c878eea357"
)


def classify_path(path: Path) -> str:
    value = path.as_posix()
    if path in ACTIVE_PROVIDER_FILES:
        return "active-provider"
    if path == HISTORICAL_PROVIDER_FILE:
        return "historical-provider"
    if re.match(r"docs/release/v1\.(?:10|11|12)/", value):
        return "historical-release"
    if re.match(r"docs/release/v1\.13/v1\.13\.(?:[0-9]|1[0-4])(?:[-./])", value):
        return "historical-release"
    if path in CURRENT_AUTHORITY_FILES or value.startswith("docs/release/v1.13/v1.13.15-"):
        return "current-authority"
    return "other"


def active_text_violations(path: Path, text: str) -> list[str]:
    violations: list[str] = []
    stale_patterns = (
        r"During v1\.10\.x",
        r"## v1\.10\.x Release Boundary",
        r"Phase 2 — Certified Toolchain and Security Gates — is Next",
        r"Phase 3 is Next: Local Development",
        r"Active v1\.9 blockers are the current release-gate sections",
    )
    for pattern in stale_patterns:
        if re.search(pattern, text):
            violations.append(f"{path}: stale active authority matches {pattern!r}")
    return violations


def require_markers(path: Path, text: str, markers: tuple[str, ...]) -> list[str]:
    normalized_text = " ".join(text.split())
    return [
        f"{path}: missing required marker {marker!r}"
        for marker in markers
        if " ".join(marker.split()) not in normalized_text
    ]


def validate_release_body(root: Path = ROOT) -> list[str]:
    """Validate the frozen publication body as exact raw bytes."""
    violations: list[str] = []
    body_path = root / CANONICAL_RELEASE_BODY
    checksum_path = root / CANONICAL_RELEASE_BODY_CHECKSUM

    for relative, path in (
        (CANONICAL_RELEASE_BODY, body_path),
        (CANONICAL_RELEASE_BODY_CHECKSUM, checksum_path),
    ):
        if not path.is_file() or path.is_symlink():
            violations.append(
                f"{relative}: required regular non-symlink file is missing"
            )

    if violations:
        return violations

    body = body_path.read_bytes()
    checksum = checksum_path.read_bytes()
    expected_checksum = (
        f"{CANONICAL_RELEASE_BODY_SHA256}  {CANONICAL_RELEASE_BODY.as_posix()}\n"
    ).encode("ascii")

    if body.startswith(b"\xef\xbb\xbf"):
        violations.append(f"{CANONICAL_RELEASE_BODY}: UTF-8 BOM is forbidden")
    try:
        body.decode("utf-8")
    except UnicodeDecodeError:
        violations.append(f"{CANONICAL_RELEASE_BODY}: invalid UTF-8")
    if b"\r" in body:
        violations.append(f"{CANONICAL_RELEASE_BODY}: only LF newlines are allowed")
    if not body.endswith(b"\n"):
        violations.append(f"{CANONICAL_RELEASE_BODY}: terminal LF is required")
    elif body.endswith(b"\n\n"):
        violations.append(
            f"{CANONICAL_RELEASE_BODY}: exactly one terminal LF is required"
        )
    if any(line.endswith((b" ", b"\t")) for line in body.splitlines()):
        violations.append(f"{CANONICAL_RELEASE_BODY}: trailing whitespace is forbidden")

    actual_digest = hashlib.sha256(body).hexdigest()
    if actual_digest != CANONICAL_RELEASE_BODY_SHA256:
        violations.append(
            f"{CANONICAL_RELEASE_BODY}: SHA-256 {actual_digest} does not match "
            f"frozen {CANONICAL_RELEASE_BODY_SHA256}"
        )
    if checksum != expected_checksum:
        violations.append(
            f"{CANONICAL_RELEASE_BODY_CHECKSUM}: bytes do not match frozen checksum"
        )

    return violations


def validate(root: Path = ROOT) -> list[str]:
    violations = validate_release_body(root)
    files = ACTIVE_PROVIDER_FILES + CURRENT_AUTHORITY_FILES + (HISTORICAL_PROVIDER_FILE,)
    texts: dict[Path, str] = {}
    for relative in files:
        path = root / relative
        if not path.is_file() or path.is_symlink():
            violations.append(f"{relative}: required regular non-symlink file is missing")
            continue
        texts[relative] = path.read_text(encoding="utf-8")

    for relative in ACTIVE_PROVIDER_FILES + CURRENT_AUTHORITY_FILES:
        if relative in texts:
            violations.extend(active_text_violations(relative, texts[relative]))

    marker_contracts = {
        Path("AGENTS.md"): (
            "never lose user data",
            "v1.13.15",
            "v1.13.14",
            "Do not implement v2",
            "phase's `PLAN` or `BUILD` mode",
            "python3 scripts/validate_governance.py",
            "GOTOOLCHAIN=local",
        ),
        Path(".github/copilot-instructions.md"): (
            "active v1.13.15 final v1.x closure train",
            "v1.13.14 as immutable historical release state",
            "SQLite-first local productization belongs to v2.x",
        ),
        Path(".github/instructions/ci.instructions.md"): (
            "v1.13.15 Release Boundary",
            "Treat v1.13.14 release evidence as immutable historical state",
        ),
        Path("README.md"): (
            "v1.13.15 — Final v1.x Security, Reproducibility, and Operational Closure",
            "V2 implementation has not started",
        ),
        Path("SECURITY.md"): (
            "v1.13.15 is the active final v1.x",
            "V2 implementation has not started",
        ),
        Path("docs/architecture/engine-boundary-plan.md"): (
            "SQLite-default repository-local product",
            "V2 owns",
            "PostgreSQL compatibility",
        ),
        Path("docs/release/v1.13/README.md"): (
            "v1.x completed the frozen Engine/Catalog correctness work",
            "Older v1.x documents",
            "historical and superseded",
        ),
    }
    for relative, markers in marker_contracts.items():
        if relative in texts:
            violations.extend(require_markers(relative, texts[relative], markers))

    historical = texts.get(HISTORICAL_PROVIDER_FILE)
    if historical is not None:
        violations.extend(
            require_markers(
                HISTORICAL_PROVIDER_FILE,
                historical,
                (
                    "Historical Coldkeep v1.10 Phase Prompt",
                    "HISTORICAL_ONLY",
                    "not current repository authority",
                ),
            )
        )
        if classify_path(HISTORICAL_PROVIDER_FILE) != "historical-provider":
            violations.append(f"{HISTORICAL_PROVIDER_FILE}: classification drift")

    return violations


def main() -> int:
    violations = validate()
    if violations:
        for violation in sorted(violations):
            print(f"GOVERNANCE_ERROR: {violation}", file=sys.stderr)
        return 1
    print("GOVERNANCE_AUTHORITY: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
