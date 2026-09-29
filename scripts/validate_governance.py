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
    Path("docs/release/v1.13/v1.13.17-scope.md"),
    Path("docs/release/v1.13/v1.13.17-phase-list.md"),
    Path("docs/release/v1.13/v1.13.17-validation-checklist.md"),
    Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
    Path("docs/release/v1.13/v1.13.17-release-state.md"),
    Path("docs/release/v1.13/v1.13.17-release-train-reconciliation.md"),
    Path("docs/release/v1.13/v1.13.17-source-test-allowlist.md"),
    Path("docs/release/v1.13/v1.13.17-release-gate.md"),
    Path("docs/release/v1.13/v1.13.17-predecessor-disposition.md"),
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
    "docs/release/v1.13/v1.13.17-release-state.md"
)
CURRENT_RELEASE_BODY = Path("docs/release/v1.13/v1.13.17-release-body.md")
CURRENT_RELEASE_BODY_CHECKSUM = Path(
    "docs/release/v1.13/v1.13.17-release-body.sha256"
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
    "RECOVERY_ROUTE",
    "CURRENT_PHASE",
    "TRACKED_PUBLICATION_MATERIAL",
    "V1_13_15_STATE",
    "V1_13_16_STATE",
    "V1_13_17_STATE",
    "CK-V11316-007",
    "FINDINGS_CONFIRMED",
    "FINDINGS_CLOSED",
    "V1_X_TECHNICAL_CORRECTNESS",
    "V1_X_FULL_CLOSURE",
)
CANONICAL_ONLY_STATE_KEYS = ("RELEASE_BODY_SHA256",)
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
    if re.match(r"docs/release/v1\.13/v1\.13\.(?:[0-9]|1[0-6])(?:[-./])", value):
        return "historical-release"
    if path in CURRENT_AUTHORITY_FILES or value.startswith("docs/release/v1.13/v1.13.17-"):
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
        path,
        blocks[0],
        CURRENT_STATE_KEYS + CANONICAL_ONLY_STATE_KEYS,
        "canonical current-state block",
    )
    expected = {
        "SOURCE_VERSION": "1.13.17",
        "RECOVERY_ROUTE": "C_SUCCESSOR_VERSION",
        "V1_13_15_STATE": "PUBLISHED_STABLE_IMMUTABLE",
        "V1_13_16_STATE": "PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE",
        "CK-V11316-007": "CLOSED_AT_V1.13.16_SOURCE_SCOPE",
        "FINDINGS_CONFIRMED": "15",
        "FINDINGS_CLOSED": "15/15",
        "V1_X_TECHNICAL_CORRECTNESS": "ESTABLISHED",
        "V1_X_FULL_CLOSURE": "NOT_ESTABLISHED",
    }
    for key, value in expected.items():
        if key in state and state[key] != value:
            violations.append(
                f"{path}: canonical {key}={state[key]!r} does not match {value!r}"
            )
    allowed_phases = {"2_NEXT", "3_NEXT", "4_NEXT", "5_NEXT", "9_NEXT", "NONE_CLOSURE_CANDIDATE"}
    if state.get("CURRENT_PHASE") not in allowed_phases:
        violations.append(f"{path}: CURRENT_PHASE is unsupported")
    successor_by_phase = {
        "2_NEXT": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        "3_NEXT": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        "4_NEXT": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        "5_NEXT": "READY_PRE_RELEASE",
        "9_NEXT": "PUBLISHED_CLOSURE_PENDING",
        "NONE_CLOSURE_CANDIDATE": "CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT",
    }
    expected_successor = successor_by_phase.get(state.get("CURRENT_PHASE", ""))
    if expected_successor and state.get("V1_13_17_STATE") != expected_successor:
        violations.append(
            f"{path}: V1_13_17_STATE does not match CURRENT_PHASE"
        )
    material = state.get("TRACKED_PUBLICATION_MATERIAL")
    digest = state.get("RELEASE_BODY_SHA256")
    if material not in {"ABSENT", "FROZEN"}:
        violations.append(f"{path}: TRACKED_PUBLICATION_MATERIAL is unsupported")
    if material == "ABSENT" and digest != "ABSENT":
        violations.append(f"{path}: ABSENT publication material requires RELEASE_BODY_SHA256: ABSENT")
    if material == "FROZEN" and not re.fullmatch(r"[0-9a-f]{64}", digest or ""):
        violations.append(f"{path}: FROZEN publication material requires a lowercase SHA-256")
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
    present = set(re.findall(r"(?m)^([A-Za-z0-9_.-]+):", blocks[0]))
    finding_keys = {f"CK-V11316-{number:03d}" for number in range(1, 16)}
    allowed = set(CURRENT_STATE_KEYS + CANONICAL_ONLY_STATE_KEYS) | finding_keys
    extras = sorted(present - allowed)
    if extras:
        violations.append(f"{path}: canonical current-state block has extra keys {extras}")
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
    """Validate the immutable v1.13.15 body and phase-aware successor pair."""
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

    state_path = root / CANONICAL_CURRENT_STATE_FILE
    phase_path = root / Path("docs/release/v1.13/v1.13.17-phase-list.md")
    if not state_path.is_file() or state_path.is_symlink():
        violations.append(f"{CANONICAL_CURRENT_STATE_FILE}: required regular non-symlink file is missing")
        return violations
    state, state_violations = canonical_current_state(
        state_path.read_text(encoding="utf-8")
    )
    violations.extend(state_violations)
    if not phase_path.is_file() or phase_path.is_symlink():
        violations.append("docs/release/v1.13/v1.13.17-phase-list.md: required regular non-symlink file is missing")
        return violations
    phase_text = phase_path.read_text(encoding="utf-8")
    phase_rows = re.findall(
        r"(?ms)^## Phase (\d+)\b.*?^\*\*Status:\*\*\s*(Complete|Next|Not started)\s*$",
        phase_text,
    )
    next_phases = [int(number) for number, status in phase_rows if status == "Next"]
    all_complete = bool(phase_rows) and all(status == "Complete" for _, status in phase_rows)
    derived_phase = (
        f"{next_phases[0]}_NEXT" if len(next_phases) == 1
        else "NONE_CLOSURE_CANDIDATE" if all_complete
        else None
    )
    if derived_phase != state.get("CURRENT_PHASE"):
        violations.append(
            f"{CANONICAL_CURRENT_STATE_FILE}: CURRENT_PHASE does not match the phase list"
        )

    material = state.get("TRACKED_PUBLICATION_MATERIAL")
    current_phase = state.get("CURRENT_PHASE")
    body_path = root / CURRENT_RELEASE_BODY
    checksum_path = root / CURRENT_RELEASE_BODY_CHECKSUM
    body_present = body_path.exists()
    checksum_present = checksum_path.exists()
    if material == "ABSENT":
        if current_phase not in {"2_NEXT", "3_NEXT", "4_NEXT"}:
            violations.append(
                f"{CANONICAL_CURRENT_STATE_FILE}: ABSENT publication material is invalid for {current_phase}"
            )
        if body_present or checksum_present:
            violations.append("v1.13.17 publication material must be wholly absent in development")
        return violations
    if material != "FROZEN" or current_phase not in {"5_NEXT", "9_NEXT", "NONE_CLOSURE_CANDIDATE"}:
        violations.append(
            f"{CANONICAL_CURRENT_STATE_FILE}: publication material state and phase are inconsistent"
        )
        return violations
    for relative, candidate in (
        (CURRENT_RELEASE_BODY, body_path),
        (CURRENT_RELEASE_BODY_CHECKSUM, checksum_path),
    ):
        if not candidate.is_file() or candidate.is_symlink():
            violations.append(f"{relative}: required regular non-symlink file is missing")
    if not body_path.is_file() or body_path.is_symlink() or not checksum_path.is_file() or checksum_path.is_symlink():
        return violations
    current_body = body_path.read_bytes()
    current_checksum = checksum_path.read_bytes()
    digest = hashlib.sha256(current_body).hexdigest()
    expected_digest = state.get("RELEASE_BODY_SHA256", "")
    expected_current_checksum = (
        f"{digest}  {CURRENT_RELEASE_BODY.as_posix()}\n"
    ).encode("ascii")
    if not 1 <= len(current_body) <= 65536:
        violations.append(f"{CURRENT_RELEASE_BODY}: body size must be 1..65536 bytes")
    if current_body.startswith(b"\xef\xbb\xbf"):
        violations.append(f"{CURRENT_RELEASE_BODY}: UTF-8 BOM is forbidden")
    try:
        current_text = current_body.decode("utf-8")
    except UnicodeDecodeError:
        current_text = ""
        violations.append(f"{CURRENT_RELEASE_BODY}: invalid UTF-8")
    if b"\r" in current_body:
        violations.append(f"{CURRENT_RELEASE_BODY}: only LF newlines are allowed")
    if not current_body.endswith(b"\n") or current_body.endswith(b"\n\n"):
        violations.append(f"{CURRENT_RELEASE_BODY}: exactly one terminal LF is required")
    if any(line.endswith((b" ", b"\t")) for line in current_body.splitlines()):
        violations.append(f"{CURRENT_RELEASE_BODY}: trailing whitespace is forbidden")
    if digest != expected_digest:
        violations.append(f"{CURRENT_RELEASE_BODY}: digest does not match canonical RELEASE_BODY_SHA256")
    if current_checksum != expected_current_checksum:
        violations.append(f"{CURRENT_RELEASE_BODY_CHECKSUM}: bytes do not match body digest and exact path")
    semantic_markers = (
        "v1.13.17", "v1.13.16", "v1.13.15", "inherited",
        "failed publication", "source-only", "no custom assets",
    )
    normalized = " ".join(current_text.lower().split())
    for marker in semantic_markers:
        if marker.lower() not in normalized:
            violations.append(f"{CURRENT_RELEASE_BODY}: missing semantic marker {marker!r}")
    forbidden = (r"\bM17\b", r"\brun[_ -]?id\b", r"certificate (?:is )?successful", r"closure (?:is )?established")
    for pattern in forbidden:
        if re.search(pattern, current_text, re.IGNORECASE):
            violations.append(f"{CURRENT_RELEASE_BODY}: contains forbidden future assertion {pattern!r}")

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
            "v1.13.17",
            "failed-publication predecessor",
            "v1.13.15",
            "v1.13.14",
            "Do not implement v2",
            "phase's `PLAN` or `BUILD` mode",
            "python3 scripts/validate_governance.py",
            "GOTOOLCHAIN=local",
        ),
        Path(".github/copilot-instructions.md"): (
            "v1.13.17 is the active Route C recovery successor",
            "v1.13.14 as immutable historical release state",
            "SQLite-first local productization belongs to v2.x",
        ),
        Path(".github/instructions/ci.instructions.md"): (
            "v1.13.17 Recovery Boundary",
            "v1.13.16 failed-publication disposition",
        ),
        Path("README.md"): (
            "v1.13.17 — Annotated-Tag Certification Recovery",
            "V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED",
            "V2 implementation has not started",
        ),
        Path("SECURITY.md"): (
            "v1.13.17 is the active Route C recovery successor",
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
            "v1.13.17",
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
