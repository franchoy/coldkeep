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
    Path("docs/release/v1.13/v1.13.16-scope.md"),
    Path("docs/release/v1.13/v1.13.16-phase-list.md"),
    Path("docs/release/v1.13/v1.13.16-release-state.md"),
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
CANONICAL_CURRENT_STATE_FILE = Path(
    "docs/release/v1.13/v1.13.16-release-state.md"
)
CURRENT_STATE_MIRROR_FILES = (
    Path("AGENTS.md"),
    Path(".github/copilot-instructions.md"),
    Path(".github/instructions/ci.instructions.md"),
    Path(".github/prompts/critical-path-coverage.prompt.md"),
    Path(".github/prompts/regression-fix.prompt.md"),
    Path("README.md"),
    Path("SECURITY.md"),
    Path("docs/release/v1.13/README.md"),
)
CURRENT_STATE_KEYS = (
    "SOURCE_VERSION",
    "PHASE_12",
    "PHASE_13",
    "CK-V11316-007",
    "FINDINGS_CONFIRMED",
    "FINDINGS_CLOSED",
    "V1_X_TECHNICAL_CORRECTNESS",
    "V1_X_FULL_CLOSURE",
)
CURRENT_STATE_MIRROR_START = "<!-- coldkeep-current-state:start -->"
CURRENT_STATE_MIRROR_END = "<!-- coldkeep-current-state:end -->"


def classify_path(path: Path) -> str:
    value = path.as_posix()
    if path in ACTIVE_PROVIDER_FILES:
        return "active-provider"
    if path == HISTORICAL_PROVIDER_FILE:
        return "historical-provider"
    if re.match(r"docs/release/v1\.(?:10|11|12)/", value):
        return "historical-release"
    if re.match(r"docs/release/v1\.13/v1\.13\.(?:[0-9]|1[0-5])(?:[-./])", value):
        return "historical-release"
    if path in CURRENT_AUTHORITY_FILES or value.startswith("docs/release/v1.13/v1.13.16-"):
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
        r"active v1\.13\.15 final v1\.x closure train",
        r"## v1\.13\.15 Release Boundary",
    )
    for pattern in stale_patterns:
        if re.search(pattern, text):
            violations.append(f"{path}: stale active authority matches {pattern!r}")

    live_text = ""
    if path in ACTIVE_PROVIDER_FILES:
        live_text = text
    elif path == Path("README.md"):
        live_text = markdown_section(text, "## Current release state")
    elif path == Path("SECURITY.md"):
        live_text = markdown_section(text, "## Status")

    current_stale_patterns = (
        r"seven (?:confirmed )?(?:Open )?findings[^\n.]*(?:none|zero) (?:is |are )?(?:fixed|closed)",
        r"seven confirmed findings, zero closed findings",
        r"FINDINGS_CONFIRMED:\s*7(?:\D|$)",
        r"FINDINGS_CLOSED:\s*0/7(?:\D|$)",
        r"PHASE_2:\s*NEXT(?:\D|$)",
        r"Phase 2(?:\s+—[^\n]*)?\s+(?:is )?Next",
        r"technical correctness closure is withheld",
        r"V1_X_TECHNICAL_CORRECTNESS_CLOSURE:\s*WITHHELD",
    )
    for pattern in current_stale_patterns:
        if re.search(pattern, live_text, re.IGNORECASE):
            violations.append(
                f"{path}: stale current-state prose matches {pattern!r}"
            )
    return violations


def markdown_section(text: str, heading: str) -> str:
    """Return one level-two Markdown section, excluding later sections."""
    match = re.search(rf"(?m)^{re.escape(heading)}\s*$", text)
    if not match:
        return ""
    remainder = text[match.start():]
    next_heading = re.search(r"(?m)^##\s+", remainder[len(heading):])
    if next_heading:
        return remainder[: len(heading) + next_heading.start()]
    return remainder


def parse_state_lines(
    path: Path, body: str, required: tuple[str, ...], label: str
) -> tuple[dict[str, str], list[str]]:
    """Parse colon-delimited state lines and fail closed on required duplicates."""
    violations: list[str] = []
    values: dict[str, list[str]] = {}
    for line in body.splitlines():
        match = re.fullmatch(r"([A-Za-z0-9_.-]+):\s*(\S.*?)\s*", line)
        if match:
            values.setdefault(match.group(1), []).append(match.group(2))

    parsed: dict[str, str] = {}
    for key in required:
        found = values.get(key, [])
        if len(found) != 1:
            violations.append(
                f"{path}: {label} requires exactly one {key}, found {len(found)}"
            )
        else:
            parsed[key] = found[0]
    return parsed, violations


def canonical_current_state(text: str) -> tuple[dict[str, str], list[str]]:
    """Read the unique fenced text block in the release-state preamble."""
    path = CANONICAL_CURRENT_STATE_FILE
    preamble = re.split(r"(?m)^##\s+", text, maxsplit=1)[0]
    blocks = re.findall(r"(?ms)^```text\s*\n(.*?)^```\s*$", preamble)
    if len(blocks) != 1:
        return {}, [
            f"{path}: canonical preamble requires exactly one fenced text block, "
            f"found {len(blocks)}"
        ]

    state, violations = parse_state_lines(
        path, blocks[0], CURRENT_STATE_KEYS, "canonical current-state block"
    )
    confirmed_raw = state.get("FINDINGS_CONFIRMED", "")
    closed_raw = state.get("FINDINGS_CLOSED", "")
    if not re.fullmatch(r"\d+", confirmed_raw):
        violations.append(f"{path}: FINDINGS_CONFIRMED must be numeric")
        confirmed = None
    else:
        confirmed = int(confirmed_raw)

    closed_match = re.fullmatch(r"(\d+)/(\d+)", closed_raw)
    if not closed_match:
        violations.append(f"{path}: FINDINGS_CLOSED must use numeric closed/total")
        closed = total = None
    else:
        closed, total = (int(value) for value in closed_match.groups())
        if confirmed is not None and total != confirmed:
            violations.append(
                f"{path}: FINDINGS_CLOSED denominator {total} does not match "
                f"FINDINGS_CONFIRMED {confirmed}"
            )

    all_state, all_violations = parse_state_lines(
        path,
        blocks[0],
        tuple(
            sorted(
                set(
                    re.findall(
                        r"(?m)^(CK-V11316-\d{3}):", blocks[0]
                    )
                )
            )
        ),
        "canonical finding rows",
    )
    violations.extend(all_violations)
    if confirmed is not None and len(all_state) != confirmed:
        violations.append(
            f"{path}: canonical finding row count {len(all_state)} does not match "
            f"FINDINGS_CONFIRMED {confirmed}"
        )
    if closed is not None:
        closed_rows = sum(value.startswith("CLOSED") for value in all_state.values())
        if closed_rows != closed:
            violations.append(
                f"{path}: canonical closed finding row count {closed_rows} does not "
                f"match FINDINGS_CLOSED numerator {closed}"
            )
    return state, violations


def current_state_mirror(
    path: Path, text: str
) -> tuple[dict[str, str], list[str]]:
    """Read one uniquely delimited current-state mirror."""
    starts = [match.start() for match in re.finditer(re.escape(CURRENT_STATE_MIRROR_START), text)]
    ends = [match.start() for match in re.finditer(re.escape(CURRENT_STATE_MIRROR_END), text)]
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        return {}, [
            f"{path}: requires exactly one ordered current-state mirror, found "
            f"{len(starts)} start and {len(ends)} end delimiters"
        ]
    body = text[starts[0] + len(CURRENT_STATE_MIRROR_START):ends[0]]
    blocks = re.findall(r"(?ms)^```text\s*\n(.*?)^```\s*$", body)
    if len(blocks) != 1:
        return {}, [
            f"{path}: current-state mirror requires exactly one fenced text block, "
            f"found {len(blocks)}"
        ]
    state, violations = parse_state_lines(
        path, blocks[0], CURRENT_STATE_KEYS, "current-state mirror"
    )
    present = set(re.findall(r"(?m)^([A-Za-z0-9_.-]+):", blocks[0]))
    extras = sorted(present - set(CURRENT_STATE_KEYS))
    if extras:
        violations.append(f"{path}: current-state mirror has extra keys {extras}")
    return state, violations


def validate_current_state_contract(texts: dict[Path, str]) -> list[str]:
    """Cross-check canonical current authority against all live mirrors."""
    canonical_text = texts.get(CANONICAL_CURRENT_STATE_FILE)
    if canonical_text is None:
        return []
    canonical, violations = canonical_current_state(canonical_text)
    for path in CURRENT_STATE_MIRROR_FILES:
        text = texts.get(path)
        if text is None:
            continue
        mirror, mirror_violations = current_state_mirror(path, text)
        violations.extend(mirror_violations)
        for key in CURRENT_STATE_KEYS:
            if key in canonical and key in mirror and mirror[key] != canonical[key]:
                violations.append(
                    f"{path}: mirror {key}={mirror[key]!r} does not match "
                    f"canonical {canonical[key]!r}"
                )
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

    violations.extend(validate_current_state_contract(texts))

    marker_contracts = {
        Path("AGENTS.md"): (
            "never lose user data",
            "v1.13.16",
            "v1.13.15",
            "v1.13.14",
            "Do not implement v2",
            "phase's `PLAN` or `BUILD` mode",
            "python3 scripts/validate_governance.py",
            "GOTOOLCHAIN=local",
        ),
        Path(".github/copilot-instructions.md"): (
            "v1.13.16 is the active exceptional critical-maintenance train",
            "v1.13.14 as immutable historical release state",
            "SQLite-first local productization belongs to v2.x",
        ),
        Path(".github/instructions/ci.instructions.md"): (
            "v1.13.16 Maintenance Boundary",
            "Treat v1.13.14 and v1.13.15 release evidence as immutable historical state",
        ),
        Path("README.md"): (
            "v1.13.16 — Snapshot Retention Integrity, Observability Truth, and Final v1.x Closure",
            "V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED",
            "V2 implementation has not started",
        ),
        Path("SECURITY.md"): (
            "v1.13.16 is the active exceptional critical-maintenance source train",
            "V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED",
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
            "v1.13.16",
            "V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED",
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
