#!/usr/bin/env python3
"""Deterministically validate current Coldkeep repository authority."""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
import sys

from release_state_support import (
    Document,
    load_final_source_snapshot,
    load_security_toolchain_progression,
    load_terminal_disposition,
)


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
    Path("docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"),
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
TERMINAL_DISPOSITION_REFERENCE_FILE = Path(
    "docs/release/v1.13/v1.13.17-terminal-disposition-reference.md"
)
SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE = Path(
    "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"
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
BASE_CURRENT_STATE_KEYS = (
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
TERMINAL_MIRROR_STATE_KEYS = (
    "EXTERNAL_TERMINAL_DISPOSITION",
    "TRACKED_TERMINAL_SERIALIZATION",
)
CURRENT_STATE_KEYS = BASE_CURRENT_STATE_KEYS + TERMINAL_MIRROR_STATE_KEYS
CANONICAL_ONLY_STATE_KEYS = (
    "RELEASE_BODY_SHA256",
    "TERMINAL_DISPOSITION_REFERENCE",
    "SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA",
    "SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE",
)
CURRENT_STATE_MIRROR_START = "<!-- coldkeep-current-state:start -->"
CURRENT_STATE_MIRROR_END = "<!-- coldkeep-current-state:end -->"

V18_CANONICAL_CURRENT_STATE_FILE = Path(
    "docs/release/v1.13/v1.13.18-release-state.md"
)
V18_CONTRACT_FILE = Path(
    "docs/release/v1.13/v1.13.18-release-state-validator-contract.md"
)
V18_REFERENCE_FILE = Path(
    "docs/release/v1.13/v1.13.18-final-source-snapshot-reference.md"
)
V18_PHASE_FILE = Path("docs/release/v1.13/v1.13.18-phase-list.md")
V18_RELEASE_BODY = Path("docs/release/v1.13/v1.13.18-release-body.md")
V18_RELEASE_BODY_CHECKSUM = Path(
    "docs/release/v1.13/v1.13.18-release-body.sha256"
)
V18_AUTHORITY_FILES = tuple(
    Path(f"docs/release/v1.13/v1.13.18-{suffix}.md")
    for suffix in (
        "final-source-snapshot-reference",
        "phase-list",
        "release-body",
        "release-gate",
        "release-state-validator-contract",
        "release-state",
        "release-train-reconciliation",
        "scope",
        "source-test-allowlist",
        "validation-checklist",
    )
) + (V18_RELEASE_BODY_CHECKSUM,)
V18_MIRROR_KEYS = (
    "SOURCE_VERSION",
    "RELEASE_ROUTE",
    "CURRENT_PHASE",
    "TRACKED_PUBLICATION_MATERIAL",
    "V1_13_15_STATE",
    "V1_13_16_STATE",
    "V1_13_17_STATE",
    "V1_13_18_STATE",
    "CK-V11316-007",
    "FINDINGS_CONFIRMED",
    "FINDINGS_CLOSED",
    "V1_X_TECHNICAL_CORRECTNESS",
    "V1_X_FULL_CLOSURE",
    "V1_13_18_PUBLICATION",
    "V2_IMPLEMENTATION",
)
V18_CANONICAL_ONLY_KEYS = (
    "RELEASE_BODY_SHA256",
    "FINAL_SOURCE_SNAPSHOT_SCHEMA",
    "FINAL_SOURCE_SNAPSHOT_REFERENCE",
)
V18_ALLOWED_NEXT_PHASES = (3, 4, 5)
V11317_FROZEN_SHA256 = {
    Path("docs/release/v1.13/v1.13.17-phase-list.md"): "0b37a14229792b2edd305cea086c743b2fee5c48494080ae1b67d431968bcb73",
    Path("docs/release/v1.13/v1.13.17-predecessor-disposition.md"): "60ccc892017cc48b9924284f08d97b32b186f169206d5d05ba3dea26fceabbc8",
    Path("docs/release/v1.13/v1.13.17-release-body.md"): "374cc16207ba5569701f38a303a07906aa64f0afd47dcf4f2455fb25434f7302",
    Path("docs/release/v1.13/v1.13.17-release-body.sha256"): "8c313d242a82ed7dd76d6c0b72ae182ec5d30889790894d4defcd3e8e5d5f789",
    Path("docs/release/v1.13/v1.13.17-release-gate.md"): "328c97263c4a68dc542beaabab1dea389c24b8ab17b1a549091baeb3c1e5ae97",
    Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"): "4c2e5f1b1af9d5e535099bdadc3e8f0101791d3a625825b4fc35ad950d3dfdfc",
    Path("docs/release/v1.13/v1.13.17-release-state.md"): "735ed9ea28b0cd8d73c4b83fbf9e32bdacbdabc33e899f904a050c70553c1ec7",
    Path("docs/release/v1.13/v1.13.17-release-train-reconciliation.md"): "44df5d0b5173b14cef8cf5bc85633d395d97a4f156bdc396f755e3849800b36a",
    Path("docs/release/v1.13/v1.13.17-scope.md"): "2f818a1c053a1be89cecd5e45fb49a69664cb21027c3cd4f5033dc8a0b37bb36",
    Path("docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"): "5e44f67721ef919048364b021ade92c32a36cc2bb870cc2a0b7a39aef41cc06f",
    Path("docs/release/v1.13/v1.13.17-source-test-allowlist.md"): "f14becb0c5145ccff801a63cb1fa01407ba3db92dda1772ef4ba9c4762ee27cd",
    Path("docs/release/v1.13/v1.13.17-terminal-disposition-reference.md"): "640460b681215dd645690f15500fb8ca2c08b5bc122d69acdf591d61a7fefc64",
    Path("docs/release/v1.13/v1.13.17-validation-checklist.md"): "c8326f65e18e7e922ddee1ad442240097e6b40ed01e45f982d7e3153dafd1acd",
}


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

    present_keys = set(re.findall(r"(?m)^([A-Za-z0-9_.-]+):", blocks[0]))
    terminal_selected = bool(
        present_keys
        & set(TERMINAL_MIRROR_STATE_KEYS + ("TERMINAL_DISPOSITION_REFERENCE",))
    ) or "NONE_TERMINAL_DISPOSITION_DECLARED" in blocks[0]
    required_mirror = (
        CURRENT_STATE_KEYS if terminal_selected else BASE_CURRENT_STATE_KEYS
    )
    required_canonical = (
        CANONICAL_ONLY_STATE_KEYS
        if terminal_selected
        else ("RELEASE_BODY_SHA256",)
    )
    state, violations = parse_state_lines(
        path,
        blocks[0],
        required_mirror + required_canonical,
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
    }
    for key, value in expected.items():
        if key in state and state[key] != value:
            violations.append(
                f"{path}: canonical {key}={state[key]!r} does not match {value!r}"
            )
    allowed_phases = {
        "2_NEXT", "3_NEXT", "4_NEXT", "5_NEXT", "9_NEXT",
        "NONE_CLOSURE_CANDIDATE", "NONE_TERMINAL_DISPOSITION_DECLARED",
    }
    if state.get("CURRENT_PHASE") not in allowed_phases:
        violations.append(f"{path}: CURRENT_PHASE is unsupported")
    successor_by_phase = {
        "2_NEXT": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        "3_NEXT": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        "4_NEXT": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        "5_NEXT": "READY_PRE_RELEASE",
        "9_NEXT": "PUBLISHED_CLOSURE_PENDING",
        "NONE_CLOSURE_CANDIDATE": "CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT",
        "NONE_TERMINAL_DISPOSITION_DECLARED": "TERMINAL_DISPOSITION_DECLARATION_PRESENT",
    }
    expected_successor = successor_by_phase.get(state.get("CURRENT_PHASE", ""))
    if expected_successor and state.get("V1_13_17_STATE") != expected_successor:
        violations.append(
            f"{path}: V1_13_17_STATE does not match CURRENT_PHASE"
        )
    terminal_values = {
        "V1_X_FULL_CLOSURE": "ESTABLISHED_AT_C_SOURCE_SCOPED_QUALIFIED",
        "EXTERNAL_TERMINAL_DISPOSITION": "ESTABLISHED_AT_C_SOURCE_SCOPED_QUALIFIED",
        "TRACKED_TERMINAL_SERIALIZATION": "DECLARATION_PRESENT_NON_AUTHORIZING",
    }
    if terminal_selected:
        for key, value in terminal_values.items():
            if state.get(key) != value:
                violations.append(f"{path}: terminal {key} is unsupported")
    elif state.get("V1_X_FULL_CLOSURE") != "NOT_ESTABLISHED":
        violations.append(
            f"{path}: canonical V1_X_FULL_CLOSURE={state.get('V1_X_FULL_CLOSURE')!r} "
            "does not match 'NOT_ESTABLISHED'"
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
    allowed = set(required_mirror + required_canonical) | finding_keys
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
    present = set(re.findall(r"(?m)^([A-Za-z0-9_.-]+):", blocks[0]))
    terminal_selected = bool(present & set(TERMINAL_MIRROR_STATE_KEYS)) or (
        "NONE_TERMINAL_DISPOSITION_DECLARED" in blocks[0]
    )
    required = CURRENT_STATE_KEYS if terminal_selected else BASE_CURRENT_STATE_KEYS
    state, violations = parse_state_lines(
        path, blocks[0], required, "current-state mirror"
    )
    extras = sorted(present - set(required))
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
        keys = (
            CURRENT_STATE_KEYS
            if canonical.get("CURRENT_PHASE") == "NONE_TERMINAL_DISPOSITION_DECLARED"
            else BASE_CURRENT_STATE_KEYS
        )
        for key in keys:
            if key in canonical and key in mirror and mirror[key] != canonical[key]:
                violations.append(
                    f"{path}: mirror {key}={mirror[key]!r} does not match "
                    f"canonical {canonical[key]!r}"
                )
    return violations


def _path_material_present(root: Path, relative: Path) -> bool:
    path = root / relative
    return path.exists() or path.is_symlink()


def _historical_terminal_violations(
    state: dict[str, str],
    expectation_present: bool,
    reference_present: bool,
) -> list[str]:
    unexpected = bool(
        expectation_present
        or reference_present
        or state.get("TERMINAL_DISPOSITION_REFERENCE")
    )
    if not unexpected:
        return []
    return [
        f"{CANONICAL_CURRENT_STATE_FILE}: unexpected terminal disposition "
        + "material for historical vocabulary"
    ]


def _selected_terminal_violations(
    root: Path,
    contract_path: Path,
    contract_text: str,
    pointer: str | None,
) -> list[str]:
    contract = Document(root, contract_path.as_posix(), contract_text.splitlines())
    disposition, detail = load_terminal_disposition(root, contract, pointer)
    if detail:
        return [f"{contract_path}: {detail}"]
    if disposition is not None:
        return []
    message = (
        f"{contract_path}: terminal disposition reference invalid: "
        + "expectation block is missing"
    )
    return [message]


def validate_terminal_disposition_contract(
    root: Path, texts: dict[Path, str]
) -> list[str]:
    """Fail closed across old/new vocabulary and terminal material selection."""
    state_text = texts.get(CANONICAL_CURRENT_STATE_FILE)
    contract_path = Path(
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
    )
    contract_text = texts.get(contract_path)
    if state_text is None or contract_text is None:
        return []
    state, _ = canonical_current_state(state_text)
    expectation_present = (
        "coldkeep-terminal-disposition-expectations:" in contract_text
    )
    reference_present = _path_material_present(
        root, TERMINAL_DISPOSITION_REFERENCE_FILE
    )
    if state.get("CURRENT_PHASE") != "NONE_TERMINAL_DISPOSITION_DECLARED":
        return _historical_terminal_violations(
            state, expectation_present, reference_present
        )
    return _selected_terminal_violations(
        root,
        contract_path,
        contract_text,
        state.get("TERMINAL_DISPOSITION_REFERENCE"),
    )


def _progression_expectation_versions(contract_text: str) -> set[int]:
    """Return every selected expectation schema version."""
    return {
        version
        for version in (1, 2, 3, 4, 5)
        if (
            f"coldkeep-v1-security-toolchain-progression-expectations/v{version}"
            in contract_text
        )
    }


def _progression_selection_violation(
    schema: str | None,
    pointer: str | None,
    expectation_versions: set[int],
) -> str | None:
    """Validate the atomic state/contract selection without loading its file."""
    if schema:
        supported = {
            f"coldkeep-v1-security-toolchain-progression-selection/v{version}": version
            for version in (2, 3, 4, 5)
        }
        version = supported.get(schema)
        if version is None:
            return "progression selection schema is unsupported"
        if not pointer or expectation_versions != {version}:
            return (
                f"v{version} progression schema, pointer, expectations, and "
                "reference must be selected together without other-version material"
            )
        return None
    if not pointer or expectation_versions != {1}:
        return (
            "progression pointer, expectations, and reference must be "
            "selected together"
        )
    return None


def _progression_material_selected(
    schema: str | None,
    pointer: str | None,
    expectation_versions: set[int],
    reference_present: bool,
) -> bool:
    return bool(schema or pointer or expectation_versions or reference_present)


def validate_security_progression_contract(
    root: Path, texts: dict[Path, str]
) -> list[str]:
    """Require the progression pointer, expectations, and reference together."""
    state_text = texts.get(CANONICAL_CURRENT_STATE_FILE)
    contract_path = Path(
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
    )
    contract_text = texts.get(contract_path)
    if state_text is None or contract_text is None:
        return []
    state, _ = canonical_current_state(state_text)
    schema = state.get("SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA")
    pointer = state.get("SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE")
    expectation_versions = _progression_expectation_versions(contract_text)
    reference_path = SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE
    reference_present = _path_material_present(root, reference_path)
    selected = _progression_material_selected(
        schema, pointer, expectation_versions, reference_present
    )
    if not selected:
        return []
    selection_violation = _progression_selection_violation(
        schema, pointer, expectation_versions
    )
    if selection_violation:
        return [
            f"{CANONICAL_CURRENT_STATE_FILE}: {selection_violation}"
        ]
    contract = Document(root, contract_path.as_posix(), contract_text.splitlines())
    progression, detail = load_security_toolchain_progression(
        root, contract, pointer
    )
    if detail:
        return [f"{contract_path}: {detail}"]
    if progression is None:
        return [f"{contract_path}: security progression reference is missing"]
    return []


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
        else (
            "NONE_TERMINAL_DISPOSITION_DECLARED"
            if state.get("CURRENT_PHASE") == "NONE_TERMINAL_DISPOSITION_DECLARED"
            else "NONE_CLOSURE_CANDIDATE"
        ) if all_complete
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
    if material != "FROZEN" or current_phase not in {
        "5_NEXT", "9_NEXT", "NONE_CLOSURE_CANDIDATE",
        "NONE_TERMINAL_DISPOSITION_DECLARED",
    }:
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


def _v18_selected(root: Path) -> bool:
    """Return whether the repository declares the isolated v1.13.18 route."""
    version_path = root / "internal/version/version.go"
    state_path = root / V18_CANONICAL_CURRENT_STATE_FILE
    if not version_path.is_file() or version_path.is_symlink():
        return False
    try:
        version_text = version_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    return bool(
        state_path.is_file()
        and not state_path.is_symlink()
        and re.search(r"(?m)^\s*Patch\s*=\s*18\s*$", version_text)
    )


def _v18_canonical_state(
    text: str,
) -> tuple[dict[str, str], list[str]]:
    """Parse the exact v1.13.18 canonical current-state vocabulary."""
    preamble = re.split(r"(?m)^##\s+", text, maxsplit=1)[0]
    blocks = re.findall(r"(?ms)^```text\s*\n(.*?)^```\s*$", preamble)
    if len(blocks) != 1:
        return {}, [
            f"{V18_CANONICAL_CURRENT_STATE_FILE}: canonical preamble requires "
            f"exactly one fenced text block, found {len(blocks)}"
        ]
    required = V18_MIRROR_KEYS + V18_CANONICAL_ONLY_KEYS
    state, violations = parse_state_lines(
        V18_CANONICAL_CURRENT_STATE_FILE,
        blocks[0],
        required,
        "canonical current-state block",
    )
    expected = {
        "SOURCE_VERSION": "1.13.18",
        "RELEASE_ROUTE": "FINAL_V1_SOURCE_SNAPSHOT",
        "TRACKED_PUBLICATION_MATERIAL": "FROZEN",
        "V1_13_15_STATE": "PUBLISHED_STABLE_IMMUTABLE",
        "V1_13_16_STATE": "PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE",
        "V1_13_17_STATE": "PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED",
        "V1_13_18_STATE": "ACTIVE_FINAL_SOURCE_SNAPSHOT_CANDIDATE",
        "CK-V11316-007": "CLOSED_AT_V1.13.16_SOURCE_SCOPE",
        "FINDINGS_CONFIRMED": "15",
        "FINDINGS_CLOSED": "15/15",
        "V1_X_TECHNICAL_CORRECTNESS": "ESTABLISHED",
        "V1_X_FULL_CLOSURE": "ESTABLISHED_AT_M_PROTECTED_MAIN_EFFECTIVE_QUALIFIED",
        "V1_13_18_PUBLICATION": "NOT_PUBLISHED",
        "V2_IMPLEMENTATION": "NOT_STARTED",
        "FINAL_SOURCE_SNAPSHOT_SCHEMA": "coldkeep-v1-final-source-snapshot-selection/v1",
        "FINAL_SOURCE_SNAPSHOT_REFERENCE": V18_REFERENCE_FILE.as_posix(),
    }
    for key, value in expected.items():
        if state.get(key) != value:
            violations.append(
                f"{V18_CANONICAL_CURRENT_STATE_FILE}: canonical {key}="
                f"{state.get(key)!r} does not match {value!r}"
            )
    allowed_phases = {
        f"{number}_NEXT" for number in V18_ALLOWED_NEXT_PHASES
    }
    if state.get("CURRENT_PHASE") not in allowed_phases:
        violations.append(
            f"{V18_CANONICAL_CURRENT_STATE_FILE}: canonical CURRENT_PHASE="
            f"{state.get('CURRENT_PHASE')!r} is outside the closed "
            "3_NEXT/4_NEXT/5_NEXT lifecycle"
        )
    digest = state.get("RELEASE_BODY_SHA256", "")
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        violations.append(
            f"{V18_CANONICAL_CURRENT_STATE_FILE}: FROZEN publication material "
            "requires a lowercase SHA-256"
        )
    finding_keys = {f"CK-V11316-{number:03d}" for number in range(1, 16)}
    present = set(re.findall(r"(?m)^([A-Za-z0-9_.-]+):", blocks[0]))
    allowed = set(required) | finding_keys
    extras = sorted(present - allowed)
    missing_findings = sorted(finding_keys - present)
    if extras:
        violations.append(
            f"{V18_CANONICAL_CURRENT_STATE_FILE}: canonical current-state block "
            f"has extra keys {extras}"
        )
    if missing_findings:
        violations.append(
            f"{V18_CANONICAL_CURRENT_STATE_FILE}: canonical finding rows are "
            f"missing {missing_findings}"
        )
    return state, violations


def _v18_mirror(path: Path, text: str) -> tuple[dict[str, str], list[str]]:
    """Parse one exact v1.13.18 live-state mirror."""
    starts = text.count(CURRENT_STATE_MIRROR_START)
    ends = text.count(CURRENT_STATE_MIRROR_END)
    if starts != 1 or ends != 1:
        return {}, [
            f"{path}: requires exactly one current-state mirror, found "
            f"{starts} start and {ends} end delimiters"
        ]
    body = text.split(CURRENT_STATE_MIRROR_START, 1)[1].split(
        CURRENT_STATE_MIRROR_END, 1
    )[0]
    blocks = re.findall(r"(?ms)^```text\s*\n(.*?)^```\s*$", body)
    if len(blocks) != 1:
        return {}, [
            f"{path}: current-state mirror requires exactly one fenced text block, "
            f"found {len(blocks)}"
        ]
    state, violations = parse_state_lines(
        path, blocks[0], V18_MIRROR_KEYS, "current-state mirror"
    )
    present = set(re.findall(r"(?m)^([A-Za-z0-9_.-]+):", blocks[0]))
    extras = sorted(present - set(V18_MIRROR_KEYS))
    if extras:
        violations.append(f"{path}: current-state mirror has extra keys {extras}")
    return state, violations


def _v18_phase_progression_violations(
    text: str,
    current_phase: str,
) -> list[str]:
    """Require one exact Phase 3, 4, or 5 Next progression."""
    phase_rows = re.findall(
        r"(?ms)^## Phase (\d+)\b.*?^\*\*Status:\*\*\s*(Complete|Next|Not started)\s*$",
        text,
    )
    allowed = {
        f"{number}_NEXT": number for number in V18_ALLOWED_NEXT_PHASES
    }
    expected_next = allowed.get(current_phase)
    expected_rows = (
        [
            (
                str(number),
                "Complete"
                if number < expected_next
                else "Next"
                if number == expected_next
                else "Not started",
            )
            for number in range(10)
        ]
        if expected_next is not None
        else []
    )
    if phase_rows == expected_rows:
        return []
    return [
        f"{V18_PHASE_FILE}: phase progression must exactly match canonical "
        f"CURRENT_PHASE={current_phase!r} within the closed "
        "3_NEXT/4_NEXT/5_NEXT lifecycle"
    ]


def _validate_v11317_frozen(root: Path) -> list[str]:
    """Require the Phase 0/1 historical v1.13.17 byte identities."""
    violations: list[str] = []
    for relative, expected in V11317_FROZEN_SHA256.items():
        path = root / relative
        if not path.is_file() or path.is_symlink():
            violations.append(
                f"{relative}: frozen historical regular file is missing"
            )
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            violations.append(
                f"{relative}: frozen historical SHA-256 {actual} does not match {expected}"
            )
    return violations


def _validate_v18_release_body(
    root: Path, state: dict[str, str]
) -> list[str]:
    """Validate the frozen source-only v1.13.18 release body pair."""
    violations: list[str] = []
    body_path = root / V18_RELEASE_BODY
    checksum_path = root / V18_RELEASE_BODY_CHECKSUM
    for relative, path in (
        (V18_RELEASE_BODY, body_path),
        (V18_RELEASE_BODY_CHECKSUM, checksum_path),
    ):
        if not path.is_file() or path.is_symlink():
            violations.append(f"{relative}: required regular non-symlink file is missing")
    if violations:
        return violations
    body = body_path.read_bytes()
    checksum = checksum_path.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    expected_checksum = (
        f"{digest}  {V18_RELEASE_BODY.as_posix()}\n"
    ).encode("ascii")
    if not 1 <= len(body) <= 65536:
        violations.append(f"{V18_RELEASE_BODY}: body size must be 1..65536 bytes")
    if body.startswith(b"\xef\xbb\xbf") or b"\r" in body or b"\x00" in body:
        violations.append(f"{V18_RELEASE_BODY}: encoding or newline form is invalid")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        text = ""
        violations.append(f"{V18_RELEASE_BODY}: invalid UTF-8")
    if not body.endswith(b"\n") or body.endswith(b"\n\n"):
        violations.append(f"{V18_RELEASE_BODY}: exactly one terminal LF is required")
    if any(line.endswith((b" ", b"\t")) for line in body.splitlines()):
        violations.append(f"{V18_RELEASE_BODY}: trailing whitespace is forbidden")
    if digest != state.get("RELEASE_BODY_SHA256"):
        violations.append(
            f"{V18_RELEASE_BODY}: digest does not match canonical RELEASE_BODY_SHA256"
        )
    if checksum != expected_checksum:
        violations.append(
            f"{V18_RELEASE_BODY_CHECKSUM}: bytes do not match body digest and exact path"
        )
    normalized = " ".join(text.lower().split())
    for marker in (
        "v1.13.18", "v1.13.17", "v1.13.16", "v1.13.15",
        "source-only", "no custom assets", "qualified",
    ):
        if marker not in normalized:
            violations.append(
                f"{V18_RELEASE_BODY}: missing semantic marker {marker!r}"
            )
    for pattern in (
        r"\bR18\s+(?:is|=)", r"\bM18\s+(?:is|=)", r"\bA18\s+(?:is|=)",
        r"publication (?:is )?complete", r"phase 10 (?:is )?safe",
    ):
        if re.search(pattern, text, re.IGNORECASE):
            violations.append(
                f"{V18_RELEASE_BODY}: contains forbidden future assertion {pattern!r}"
            )
    return violations


def validate_v18_contract(root: Path, texts: dict[Path, str]) -> list[str]:
    """Validate the isolated v1.13.18 authority and frozen history."""
    violations: list[str] = []
    loaded: dict[Path, str] = {}
    for relative in V18_AUTHORITY_FILES:
        path = root / relative
        if not path.is_file() or path.is_symlink():
            violations.append(f"{relative}: required regular non-symlink file is missing")
            continue
        if relative.suffix == ".md":
            loaded[relative] = path.read_text(encoding="utf-8")
    state_text = loaded.get(V18_CANONICAL_CURRENT_STATE_FILE)
    if state_text is None:
        return violations
    state, state_violations = _v18_canonical_state(state_text)
    violations.extend(state_violations)
    for relative in CURRENT_STATE_MIRROR_FILES:
        text = texts.get(relative)
        if text is None:
            continue
        mirror, mirror_violations = _v18_mirror(relative, text)
        violations.extend(mirror_violations)
        for key in V18_MIRROR_KEYS:
            if key in state and mirror.get(key) != state[key]:
                violations.append(
                    f"{relative}: mirror {key}={mirror.get(key)!r} does not match "
                    f"canonical {state[key]!r}"
                )
    contract_text = loaded.get(V18_CONTRACT_FILE)
    if contract_text is not None:
        contract = Document(
            root, V18_CONTRACT_FILE.as_posix(), contract_text.splitlines()
        )
        snapshot, detail = load_final_source_snapshot(
            root, contract, state.get("FINAL_SOURCE_SNAPSHOT_REFERENCE")
        )
        if detail:
            violations.append(f"{V18_CONTRACT_FILE}: {detail}")
        elif snapshot is None:
            violations.append(f"{V18_CONTRACT_FILE}: final snapshot reference is missing")
    phase_text = loaded.get(V18_PHASE_FILE, "")
    violations.extend(
        _v18_phase_progression_violations(
            phase_text, state.get("CURRENT_PHASE", "")
        )
    )
    violations.extend(_validate_v18_release_body(root, state))
    violations.extend(_validate_v11317_frozen(root))
    return violations


def validate(root: Path = ROOT) -> list[str]:
    violations = validate_release_body(root)
    v18_selected = _v18_selected(root)
    files = ACTIVE_PROVIDER_FILES + CURRENT_AUTHORITY_FILES + (HISTORICAL_PROVIDER_FILE,)
    texts: dict[Path, str] = {}
    for relative in files:
        path = root / relative
        if not path.is_file() or path.is_symlink():
            if relative == SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE:
                continue
            violations.append(f"{relative}: required regular non-symlink file is missing")
            continue
        texts[relative] = path.read_text(encoding="utf-8")

    for relative in ACTIVE_PROVIDER_FILES + CURRENT_AUTHORITY_FILES:
        if relative in texts:
            violations.extend(active_text_violations(relative, texts[relative]))

    if v18_selected:
        violations.extend(validate_v18_contract(root, texts))
    else:
        violations.extend(validate_current_state_contract(texts))
        violations.extend(validate_terminal_disposition_contract(root, texts))
        violations.extend(validate_security_progression_contract(root, texts))

    marker_contracts = {
        Path("AGENTS.md"): (
            "never lose user data",
            "v1.13.17",
            *(
                ()
                if v18_selected
                else ("failed-publication predecessor", "v1.13.14")
            ),
            "v1.13.15",
            "Do not implement v2",
            "phase's `PLAN` or `BUILD` mode",
            "python3 scripts/validate_governance.py",
            "GOTOOLCHAIN=local",
        ),
        Path(".github/copilot-instructions.md"): (
            *(
                ("v1.13.17",)
                if v18_selected
                else ("v1.13.17 is the active Route C recovery successor",)
            ),
            "v1.13.14 as immutable historical release state",
            "SQLite-first local productization belongs to v2.x",
        ),
        Path(".github/instructions/ci.instructions.md"): (
            "v1.13.17",
            "v1.13.16",
            *(
                ("Go 1.26.9",)
                if v18_selected
                else (
                    "v1.13.17 Recovery Boundary",
                    "v1.13.16 failed-publication disposition",
                )
            ),
        ),
        Path("README.md"): (
            *(
                ("v1.13.17",)
                if v18_selected
                else ("v1.13.17 — Annotated-Tag Certification Recovery",)
            ),
            "V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED",
            "V2 implementation has not started",
        ),
        Path("SECURITY.md"): (
            *(
                ("v1.13.17",)
                if v18_selected
                else ("v1.13.17 is the active Route C recovery successor",)
            ),
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
