#!/usr/bin/env python3
"""Validate Coldkeep's bounded release-state contract without network access."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

from release_state_support import (
    CHANGELOG_HEADING,
    Document,
    InternalError,
    ValidationResult,
    accepted_release_pr,
    artifact_targets,
    candidate_gate_valid,
    changelog_entry_valid,
    current_train_definitions,
    current_train_section,
    development_gate_invalid,
    development_progression_valid,
    exact_tag_matches_head,
    field_from_lines,
    first_invalid_phase,
    git_context_matches,
    github_context,
    main_context,
    parse_phase_states,
    parse_source_version,
    phase_blocks,
    phases_complete,
    previous_closure_detail,
    prior_is_active,
    readme_current_state_valid,
    run_git,
    topology_valid,
    train_definition_invalid,
    train_marker_index,
    tracker_header,
    tracker_values_disagree,
    unlabeled_historical_definitions,
    validate_root,
    version_from_release,
)


VALIDATOR = "coldkeep-release-state"
STATES = ("development", "pre-release", "merged-not-tagged", "released")


def normalized(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def read_document(
    root: Path,
    path: str,
    result: ValidationResult,
    required: bool = True,
) -> Optional[Document]:
    try:
        return Document.load(root, path)
    except FileNotFoundError:
        if required:
            message = (
                f"required release-state input {path} is missing or structurally "
                "ambiguous: file is missing"
            )
            result.add("CKRS019", path, 0, message)
        return None
    except UnicodeDecodeError as exc:
        raise InternalError("parser", f"cannot decode {path}: {exc}") from exc


def heading_versions(doc: Document) -> list[tuple[int, str, str]]:
    values = []
    for index, line in enumerate(doc.lines, 1):
        match = CHANGELOG_HEADING.match(line)
        if match:
            values.append((index, match.group(1), match.group(2).strip()))
    return values


def release_paths(version: str) -> tuple[str, str]:
    major, minor, _ = version.split(".")
    directory = f"docs/release/v{major}.{minor}"
    return directory, f"v{version}"


def active_docs(
    root: Path,
    version: str,
    result: ValidationResult,
) -> dict[str, Optional[Document]]:
    directory, stem = release_paths(version)
    major, minor, _ = version.split(".")
    paths = {
        "version_test": "internal/version/version_test.go",
        "checklist": "PRE_RELEASE_CHECKLIST.md",
        "changelog": "CHANGELOG.md",
        "readme": "README.md",
        "release_readme": f"{directory}/README.md",
        "scope": f"{directory}/{stem}-scope.md",
        "phase_list": f"{directory}/{stem}-phase-list.md",
        "phase_checklist": f"{directory}/{stem}-validation-checklist.md",
        "train": f"{directory}/v{major}.{minor}.x-release-train.md",
        "reconciliation": f"{directory}/{stem}-release-train-reconciliation.md",
        "contract": f"{directory}/{stem}-release-state-validator-contract.md",
    }
    return {key: read_document(root, path, result) for key, path in paths.items()}


def check_ckrs002(version: str, doc: Optional[Document], result: ValidationResult) -> None:
    if not doc:
        return
    matches = re.findall(r'"(\d+\.\d+\.\d+)"', "\n".join(doc.lines))
    if len(matches) != 1 or matches[0] != version:
        found = matches[0] if len(matches) == 1 else "missing or ambiguous"
        result.add("CKRS002", doc.path, 0, f"version test expects {found}; source version is {version}")


def check_ckrs003(version: str, doc: Optional[Document], result: ValidationResult) -> None:
    if not doc:
        return
    matches = [(index, match.group(1)) for index, line in enumerate(doc.lines, 1) if (match := re.match(r'^expected_version="([^"]+)"$', line))]
    if len(matches) != 1 or matches[0][1] != version:
        found = matches[0][1] if len(matches) == 1 else "missing or ambiguous"
        line = matches[0][0] if len(matches) == 1 else 0
        result.add("CKRS003", doc.path, line, f"pre-release expected_version is {found}; source version is {version}")


def check_ckrs004(
    version: str,
    doc: Optional[Document],
    state: Optional[str],
    result: ValidationResult,
) -> Optional[str]:
    if not doc:
        return None
    headings = heading_versions(doc)
    if not headings:
        result.add("CKRS004", doc.path, 0, f"active changelog entry missing for version {version}")
        return None
    line, found, suffix = headings[0]
    if not changelog_entry_valid(found, suffix, version, state):
        result.add("CKRS004", doc.path, line, f"active changelog entry v{found} does not satisfy {state or 'auto'} version {version}")
    return headings[1][1] if len(headings) > 1 else None


def current_section(doc: Optional[Document], heading: str, result: ValidationResult) -> Optional[tuple[int, list[str]]]:
    if not doc:
        return None
    section = doc.section(heading)
    if section is None:
        result.add("CKRS019", doc.path, 0, f"required release-state input {doc.path} is missing or structurally ambiguous: heading {heading} is missing")
    return section


def check_ckrs005(
    version: str,
    doc: Optional[Document],
    state: Optional[str],
    result: ValidationResult,
) -> None:
    section = current_section(doc, "## Current release state", result)
    if not doc or not section:
        return
    start, lines = section
    badge = [line for line in doc.lines if "img.shields.io/badge/status-" in line]
    content = "\n".join(lines)
    if not readme_current_state_valid(version, state, badge, content):
        result.add("CKRS005", doc.path, start, f"root README current release is missing v{version}; expected {version}")


def check_ckrs006(version: str, doc: Optional[Document], state: Optional[str], result: ValidationResult) -> Optional[tuple[int, list[str]]]:
    section = current_section(doc, "## Current Release State", result)
    if not doc or not section:
        return None
    start, lines = section
    text = "\n".join(lines)
    expected_word = "active" if state == "development" else "ready"
    if f"v{version}" not in text or expected_word not in text.lower():
        result.add("CKRS006", doc.path, start, f"release README current state conflicts with {version}")
    return section


def tracker_metadata(
    doc: Optional[Document],
    result: ValidationResult,
) -> Optional[tuple[str, str, str]]:
    if not doc:
        return None
    header = tracker_header(doc)
    release = [(index, value) for index, name, value in header if name == "Release"]
    status = [(index, value) for index, name, value in header if name == "Status"]
    if len(release) != 1 or len(status) != 1:
        result.add("CKRS007", doc.path, 0, f"active tracker metadata is missing or ambiguous in {doc.path}")
        return None
    parsed = version_from_release(release[0][1])
    if not parsed:
        result.add("CKRS007", doc.path, release[0][0], f"active tracker Release is malformed: {release[0][1]}")
        return None
    return parsed[0], parsed[1], status[0][1]


def check_ckrs007(
    version: str,
    docs: dict[str, Optional[Document]],
    state: Optional[str],
    result: ValidationResult,
) -> None:
    values = [tracker_metadata(docs[key], result) for key in ("scope", "phase_list", "phase_checklist")]
    if any(value is None for value in values):
        return
    tracker_values = [value for value in values if value is not None]
    expected_status = "Active" if state == "development" else "Ready for release"
    if tracker_values_disagree(tracker_values, version, expected_status):
        rendered = "; ".join(f"{key}={value}" for key, value in zip(("scope", "phase_list", "phase_checklist"), tracker_values))
        result.add("CKRS007", "docs/release/v1.13", 0, f"active tracker identity/title/status disagree: {rendered}")


def check_ckrs008(version: str, title: Optional[str], doc: Optional[Document], result: ValidationResult) -> None:
    if not doc:
        return
    marker_index = train_marker_index(doc)
    if marker_index is None:
        result.add("CKRS019", doc.path, 0, f"required release-state input {doc.path} is missing or structurally ambiguous: historical boundary is missing")
        return
    current = current_train_definitions(doc, version, marker_index)
    if train_definition_invalid(current, title):
        result.add("CKRS008", doc.path, current[0][0] if current else 0, f"release train has {len(current)} current definitions for {version}")
    for line in unlabeled_historical_definitions(doc, version, marker_index):
        result.add("CKRS008", doc.path, line, f"release train historical proposal for {version} is not labeled Historical proposed")


def check_ckrs009(version: str, doc: Optional[Document], result: ValidationResult) -> Optional[str]:
    if not doc:
        return None
    headings = heading_versions(doc)
    if len(headings) < 2 or headings[0][1] != version:
        return None
    line, previous, suffix = headings[1]
    if "Unreleased" in suffix or not re.search(r"-\s+\d{4}-\d{2}-\d{2}\b", suffix):
        result.add("CKRS009", doc.path, line, f"previous release {previous} is not recorded as released in CHANGELOG.md")
    return previous


def check_ckrs010(root: Path, previous: Optional[str], result: ValidationResult) -> None:
    if not previous:
        return
    directory, stem = release_paths(previous)
    scope_path = f"{directory}/{stem}-scope.md"
    gate_path = f"{directory}/{stem}-release-gate.md"
    train_path = f"{directory}/v{previous.split('.')[0]}.{previous.split('.')[1]}.x-release-train.md"
    scope = read_document(root, scope_path, result, required=False)
    gate = read_document(root, gate_path, result, required=False)
    train = read_document(root, train_path, result, required=False)
    detail = previous_closure_detail(previous, scope, gate, train)
    if detail:
        result.add("CKRS010", gate_path, 0, f"previous release {previous} lacks closed canonical release evidence: {detail}")


def check_ckrs011(previous: Optional[str], readme: Optional[Document], release_readme: Optional[Document], train: Optional[Document], result: ValidationResult) -> None:
    if not previous:
        return
    targets: list[tuple[Optional[Document], Optional[tuple[int, list[str]]]]] = [
        (readme, current_section(readme, "## Current release state", result)),
        (release_readme, current_section(release_readme, "## Current Release State", result)),
    ]
    if train:
        targets.append((train, current_train_section(train)))
    for doc, section in targets:
        if not doc or not section:
            continue
        start, lines = section
        branch = f"release/v{previous}"
        if prior_is_active(previous, branch, lines):
            result.add("CKRS011", doc.path, start, f"current section describes prior release {previous} or branch {branch} as active")


def check_ckrs012(
    phase_numbers: list[int],
    checklist_numbers: list[int],
    result: ValidationResult,
) -> None:
    if not topology_valid(phase_numbers, checklist_numbers):
        result.add(
            "CKRS012",
            "docs/release/v1.13",
            0,
            "phase topology mismatch: phase numbers must be unique, contiguous from 0, ordered, and identical",
        )


def check_ckrs013(
    phase_doc: Document,
    phase_numbers: list[int],
    phase_states: dict[int, tuple[str, int]],
    state: Optional[str],
    result: ValidationResult,
) -> None:
    invalid_phase = first_invalid_phase(phase_numbers, phase_states)
    ordered = [phase_states.get(number, ("missing", 0))[0] for number in phase_numbers]
    if invalid_phase is not None:
        line = phase_states.get(invalid_phase, ("", 0))[1]
        message = f"invalid phase status progression at Phase {invalid_phase}: invalid status"
        result.add("CKRS013", phase_doc.path, line, message)
        return
    if state == "development" and not development_progression_valid(ordered):
        message = "invalid phase status progression at Phase 0: development requires Complete*, one Next, Not started*"
        result.add("CKRS013", phase_doc.path, 0, message)
        return
    candidate_states = ("pre-release", "merged-not-tagged", "released")
    if state in candidate_states and any(value != "Complete" for value in ordered):
        message = f"invalid phase status progression at Phase 0: {state} requires all phases Complete"
        result.add("CKRS013", phase_doc.path, 0, message)


def check_phase_status_parity(
    phase_numbers: list[int],
    checklist_numbers: list[int],
    phase_states: dict[int, tuple[str, int]],
    checklist_states: dict[int, tuple[str, int]],
    checklist_doc: Document,
    result: ValidationResult,
) -> None:
    for number in sorted(set(phase_numbers) & set(checklist_numbers)):
        phase_status = phase_states.get(number, (None, 0))[0]
        checklist_status, line = checklist_states.get(number, (None, 0))
        if phase_status != checklist_status:
            message = f"phase {number} status or current-phase pointer disagrees across active trackers"
            result.add("CKRS014", checklist_doc.path, line, message)


def check_current_phase_pointer(
    docs: dict[str, Optional[Document]],
    phase_numbers: list[int],
    phase_states: dict[int, tuple[str, int]],
    result: ValidationResult,
) -> None:
    next_phase = next(
        (
            number
            for number in phase_numbers
            if phase_states.get(number, (None, 0))[0] == "Next"
        ),
        None,
    )
    if next_phase is None:
        return
    scope_text = "\n".join(docs["scope"].lines if docs["scope"] else [])
    section = current_section(docs["release_readme"], "## Current Release State", result)
    release_text = "\n".join((section or (0, []))[1])
    scope_matches = re.search(rf"Phase {next_phase}.*(?:Next|next)", scope_text)
    release_matches = re.search(rf"Phase {next_phase}.*(?:next|Next)", release_text)
    if not scope_matches or not release_matches:
        message = f"phase {next_phase} status or current-phase pointer disagrees across active trackers"
        result.add("CKRS014", "docs/release/v1.13", 0, message)


def check_ckrs012_014(
    docs: dict[str, Optional[Document]],
    state: Optional[str],
    result: ValidationResult,
) -> list[tuple[int, int, list[str]]]:
    phase_doc, checklist_doc = docs["phase_list"], docs["phase_checklist"]
    if not phase_doc or not checklist_doc:
        return []
    phase_numbers, phase_states = parse_phase_states(phase_doc, "Status")
    checklist_numbers, checklist_states = parse_phase_states(checklist_doc, "Phase status")
    check_ckrs012(phase_numbers, checklist_numbers, result)
    check_ckrs013(phase_doc, phase_numbers, phase_states, state, result)
    check_phase_status_parity(
        phase_numbers,
        checklist_numbers,
        phase_states,
        checklist_states,
        checklist_doc,
        result,
    )
    check_current_phase_pointer(docs, phase_numbers, phase_states, result)
    return phase_blocks(phase_doc)


def check_ckrs015(root: Path, phase_doc: Optional[Document], result: ValidationResult) -> None:
    if not phase_doc:
        return
    for number, start, block in phase_blocks(phase_doc):
        state = field_from_lines(block, "Status")
        if state != "Complete":
            continue
        for target in artifact_targets(root, phase_doc.path, block):
            candidate = root / target
            if target.startswith("../") or not candidate.is_file():
                result.add("CKRS015", phase_doc.path, start + 1, f"completed Phase {number} references missing artifact {target}")


def git_context(root: Path, version: str) -> tuple[str, str, bool, bool, Optional[str]]:
    head = run_git(root, ["rev-parse", "HEAD"]).stdout.strip()
    branch_result = run_git(root, ["symbolic-ref", "--quiet", "--short", "HEAD"], allow_failure=True)
    branch = branch_result.stdout.strip() if branch_result.returncode == 0 else ""
    tag = f"v{version}"
    tag_ref = f"refs/tags/{tag}"
    exists = run_git(root, ["show-ref", "--verify", "--quiet", tag_ref], allow_failure=True).returncode == 0
    annotated = False
    target = None
    if exists:
        annotated = run_git(root, ["cat-file", "-t", tag_ref]).stdout.strip() == "tag"
        if annotated:
            target = run_git(root, ["rev-parse", f"{tag_ref}^{{}}"]).stdout.strip()
    return head, branch, exists, annotated, target


def infer_state(
    root: Path,
    version: str,
    phase_doc: Optional[Document],
) -> tuple[str, tuple[str, str, bool, bool, Optional[str]]]:
    context = git_context(root, version)
    head, branch, exists, annotated, target = context
    env = github_context()
    if exact_tag_matches_head(head, exists, annotated, target):
        return "released", context
    if main_context(branch, env):
        return "merged-not-tagged", context
    release_branch = f"release/v{version}"
    if branch == release_branch or accepted_release_pr(version, env):
        state = "pre-release" if phases_complete(phase_doc) else "development"
        return state, context
    raise InternalError("git-context", "unable to infer release lifecycle from the current Git context")


def check_ckrs016(
    version: str,
    state: str,
    context: tuple[str, str, bool, bool, Optional[str]],
    result: ValidationResult,
) -> None:
    _, branch, _, _, _ = context
    if not git_context_matches(version, state, branch, github_context()):
        result.add("CKRS016", ".git", 0, f"Git context branch={branch or 'detached'} is incompatible with {state} for {version}")


def check_ckrs017(version: str, state: str, context: tuple[str, str, bool, bool, Optional[str]], result: ValidationResult) -> None:
    head, _, exists, annotated, target = context
    if state == "released":
        if not exists or not annotated or target != head:
            detail = "missing annotated tag" if not exists else "tag is lightweight" if not annotated else "peeled target does not equal HEAD"
            result.add("CKRS017", ".git", 0, f"tag v{version} does not satisfy released: {detail}")
    elif exists:
        result.add("CKRS017", ".git", 0, f"tag v{version} does not satisfy {state}: exact tag already exists")


def check_ckrs018(
    root: Path,
    version: str,
    state: str,
    phase_doc: Optional[Document],
    result: ValidationResult,
) -> None:
    directory, stem = release_paths(version)
    path = f"{directory}/{stem}-release-gate.md"
    gate = read_document(root, path, result, required=False)
    complete = phases_complete(phase_doc)
    if state == "development":
        if development_gate_invalid(gate, complete):
            result.add("CKRS018", path, 0, "current release gate is invalid for development: passed gate precedes completed phases")
        return
    if gate is None:
        result.add("CKRS018", path, 0, f"current release gate is invalid for {state}: canonical gate is missing")
        return
    if not candidate_gate_valid(gate, complete):
        result.add("CKRS018", path, 0, f"current release gate is invalid for {state}: passed pre-tag gate and final verdict are required")


def validate(root: Path, requested_state: str) -> ValidationResult:
    result = ValidationResult(None, None)
    version = parse_source_version(root, result)
    result.active_version = version
    if version is None:
        return result
    docs = active_docs(root, version, result)
    if requested_state == "auto":
        state, context = infer_state(root, version, docs["phase_list"])
    else:
        state, context = requested_state, git_context(root, version)
    result.state = state
    check_ckrs002(version, docs["version_test"], result)
    check_ckrs003(version, docs["checklist"], result)
    previous_from_changelog = check_ckrs004(version, docs["changelog"], state, result)
    check_ckrs005(version, docs["readme"], state, result)
    check_ckrs006(version, docs["release_readme"], state, result)
    scope = tracker_metadata(docs["scope"], result)
    check_ckrs007(version, docs, state, result)
    check_ckrs008(version, scope[1] if scope else None, docs["train"], result)
    previous = check_ckrs009(version, docs["changelog"], result) or previous_from_changelog
    check_ckrs010(root, previous, result)
    check_ckrs011(previous, docs["readme"], docs["release_readme"], docs["train"], result)
    check_ckrs012_014(docs, state, result)
    check_ckrs015(root, docs["phase_list"], result)
    check_ckrs016(version, state, context, result)
    check_ckrs017(version, state, context, result)
    check_ckrs018(root, version, state, docs["phase_list"], result)
    return result


def emit(result: ValidationResult, as_json: bool) -> int:
    violations = result.ordered()
    if as_json:
        payload = {
            "status": "ok" if not violations else "error",
            "validator": VALIDATOR,
            "state": result.state,
            "active_version": result.active_version,
            "violations": [item.__dict__ for item in violations],
            "error": None,
        }
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    elif not violations:
        print(f"[release-state] OK state={result.state} active_version={result.active_version} violations=0")
    else:
        for item in violations:
            location = item.path if item.line == 0 else f"{item.path}:{item.line}"
            print(f"[{item.rule}] {location} {item.message}")
        print(f"[release-state] FAILED state={result.state or 'unknown'} active_version={result.active_version or 'unknown'} violations={len(violations)}")
    return 0 if not violations else 1


def emit_error(error: InternalError, as_json: bool) -> int:
    if as_json:
        payload = {"status": "error", "validator": VALIDATOR, "state": None, "active_version": None, "violations": [], "error": {"kind": error.kind, "message": error.message}}
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    else:
        print(f"[release-state] ERROR {error.kind}: {error.message}", file=sys.stderr)
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate Coldkeep release-state documentation and local Git context.")
    parser.add_argument("--json", action="store_true", help="emit one JSON result object")
    parser.add_argument("--repo-root", help="repository root to validate")
    parser.add_argument("--state", choices=("auto", *STATES), default="auto", help="lifecycle state (default: auto)")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        root = validate_root(args.repo_root)
        return emit(validate(root, args.state), args.json)
    except InternalError as error:
        return emit_error(error, args.json)
    except Exception as error:  # defensive boundary; no traceback for callers
        return emit_error(InternalError("internal", str(error)), args.json)


if __name__ == "__main__":
    raise SystemExit(main())
