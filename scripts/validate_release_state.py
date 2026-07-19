#!/usr/bin/env python3
"""Validate Coldkeep's bounded release-state contract without network access."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable, Optional

from release_state_support import (
    Document,
    InternalError,
    ValidationResult,
    active_docs,
    field_from_lines,
    heading_versions,
    normalized,
    parse_phase_states,
    parse_source_version,
    phase_blocks,
    read_document,
    release_paths,
    run_git,
    safe_relative_artifact,
    validate_root,
    version_from_release,
)


VALIDATOR = "coldkeep-release-state"
STATES = ("development", "pre-release", "merged-not-tagged", "released")
METADATA = re.compile(r"^\*\*(Release|Status|Branch|Phase status):\*\*\s*(.*)$")


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


def check_ckrs004(version: str, doc: Optional[Document], state: Optional[str], result: ValidationResult) -> Optional[str]:
    if not doc:
        return None
    headings = heading_versions(doc)
    if not headings:
        result.add("CKRS004", doc.path, 0, f"active changelog entry missing for version {version}")
        return None
    line, found, suffix = headings[0]
    dated = bool(re.search(r"-\s+\d{4}-\d{2}-\d{2}\b", suffix))
    expected_dated = state in ("pre-release", "merged-not-tagged", "released")
    valid = found == version and ((not expected_dated and "Unreleased" in suffix) or (expected_dated and dated and "Unreleased" not in suffix))
    if not valid:
        result.add("CKRS004", doc.path, line, f"active changelog entry v{found} does not satisfy {state or 'auto'} version {version}")
    return headings[1][1] if len(headings) > 1 else None


def current_section(doc: Optional[Document], heading: str, result: ValidationResult) -> Optional[tuple[int, list[str]]]:
    if not doc:
        return None
    section = doc.section(heading)
    if section is None:
        result.add("CKRS019", doc.path, 0, f"required release-state input {doc.path} is missing or structurally ambiguous: heading {heading} is missing")
    return section


def check_ckrs005(version: str, doc: Optional[Document], state: Optional[str], result: ValidationResult) -> None:
    section = current_section(doc, "## Current release state", result)
    if not doc or not section:
        return
    start, lines = section
    badge = [line for line in doc.lines if "img.shields.io/badge/status-" in line]
    content = "\n".join(lines)
    expected_word = "active" if state == "development" else "ready"
    if len(badge) != 1 or f"v{version}" not in badge[0] or f"v{version}" not in content or expected_word not in content.lower():
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


def tracker_metadata(doc: Optional[Document], result: ValidationResult) -> Optional[tuple[str, str, str]]:
    if not doc:
        return None
    header = []
    for index, line in enumerate(doc.lines, 1):
        if line.startswith("## "):
            break
        match = METADATA.match(line)
        if match:
            header.append((index, match.group(1), match.group(2).strip()))
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


def check_ckrs007(version: str, docs: dict[str, Optional[Document]], state: Optional[str], result: ValidationResult) -> None:
    values = [tracker_metadata(docs[key], result) for key in ("scope", "phase_list", "phase_checklist")]
    if any(value is None for value in values):
        return
    tracker_values = [value for value in values if value is not None]
    expected_status = "Active" if state == "development" else "Ready for release"
    if any(value != tracker_values[0] for value in tracker_values) or tracker_values[0][0] != version or tracker_values[0][2] != expected_status:
        rendered = "; ".join(f"{key}={value}" for key, value in zip(("scope", "phase_list", "phase_checklist"), tracker_values))
        result.add("CKRS007", "docs/release/v1.13", 0, f"active tracker identity/title/status disagree: {rendered}")


def check_ckrs008(version: str, title: Optional[str], doc: Optional[Document], result: ValidationResult) -> None:
    if not doc:
        return
    marker = "## Historical proposed continuation and final disposition"
    marker_index = next((index for index, line in enumerate(doc.lines) if line == marker), None)
    if marker_index is None:
        result.add("CKRS019", doc.path, 0, f"required release-state input {doc.path} is missing or structurally ambiguous: historical boundary is missing")
        return
    pattern = re.compile(rf"^### `v{re.escape(version)}\s+—\s+(.+)`$")
    current = [(index + 1, match.group(1)) for index, line in enumerate(doc.lines[:marker_index]) if (match := pattern.match(line))]
    if len(current) != 1 or (title and current and current[0][1] != title):
        result.add("CKRS008", doc.path, current[0][0] if current else 0, f"release train has {len(current)} current definitions for {version}")
    for index, line in enumerate(doc.lines[marker_index + 1 :], marker_index + 2):
        if re.match(rf"^### `v{re.escape(version)}\s+—", line) and "Historical proposed" not in line:
            result.add("CKRS008", doc.path, index, f"release train historical proposal for {version} is not labeled Historical proposed")


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
    detail = None
    if scope is None:
        detail = "scope is missing"
    elif "Released and operationally closed" not in [value for _, value in scope.metadata("Status")]:
        detail = "scope is not operationally closed"
    elif gate is None:
        detail = "canonical gate is missing"
    elif train is None:
        detail = "release train is missing"
    elif not re.search(rf"^### `v{re.escape(previous)}\s+—.*?`$[\s\S]*?^\*\*Status:\*\* Released and operationally closed$", "\n".join(train.lines), re.M):
        detail = "release train is not operationally closed"
    elif "Passed and released" not in [value for _, value in gate.metadata("Status")]:
        detail = "gate is not passed and released"
    else:
        verdict = gate.section("## Final verdict")
        if verdict is None or not "\n".join(verdict[1]).strip() or re.search(r"\bpending\b", "\n".join(verdict[1]), re.I):
            detail = "gate verdict is missing or pending"
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
        marker = "## Historical proposed continuation and final disposition"
        marker_index = next((index for index, line in enumerate(train.lines) if line == marker), len(train.lines))
        targets.append((train, (1, train.lines[:marker_index])))
    for doc, section in targets:
        if not doc or not section:
            continue
        start, lines = section
        text = "\n".join(lines)
        branch = f"release/v{previous}"
        if re.search(rf"v{re.escape(previous)}[^\n]*(?:is\s+)?active", text, re.I) or re.search(rf"{re.escape(branch)}[^\n]*(?:is\s+)?active", text, re.I):
            result.add("CKRS011", doc.path, start, f"current section describes prior release {previous} or branch {branch} as active")


def check_ckrs012_014(docs: dict[str, Optional[Document]], state: Optional[str], result: ValidationResult) -> list[tuple[int, int, list[str]]]:
    phase_doc, checklist_doc = docs["phase_list"], docs["phase_checklist"]
    if not phase_doc or not checklist_doc:
        return []
    phase_numbers, phase_states = parse_phase_states(phase_doc, "Status")
    checklist_numbers, checklist_states = parse_phase_states(checklist_doc, "Phase status")
    expected_numbers = list(range(len(phase_numbers)))
    if phase_numbers != expected_numbers or len(set(phase_numbers)) != len(phase_numbers) or checklist_numbers != expected_numbers or len(set(checklist_numbers)) != len(checklist_numbers):
        result.add("CKRS012", "docs/release/v1.13", 0, "phase topology mismatch: phase numbers must be unique, contiguous from 0, ordered, and identical")
    allowed = {"Complete", "Next", "Not started"}
    ordered = [phase_states.get(number, ("missing", 0))[0] for number in phase_numbers]
    if any(value not in allowed for value in ordered):
        first = next(number for number in phase_numbers if phase_states.get(number, ("missing", 0))[0] not in allowed)
        result.add("CKRS013", phase_doc.path, phase_states.get(first, ("", 0))[1], f"invalid phase status progression at Phase {first}: invalid status")
    elif state == "development":
        next_positions = [index for index, value in enumerate(ordered) if value == "Next"]
        valid = len(next_positions) == 1 and all(value == "Complete" for value in ordered[: next_positions[0]]) and all(value == "Not started" for value in ordered[next_positions[0] + 1 :])
        if not valid:
            result.add("CKRS013", phase_doc.path, 0, "invalid phase status progression at Phase 0: development requires Complete*, one Next, Not started*")
    elif state in ("pre-release", "merged-not-tagged", "released") and any(value != "Complete" for value in ordered):
        result.add("CKRS013", phase_doc.path, 0, f"invalid phase status progression at Phase 0: {state} requires all phases Complete")
    for number in sorted(set(phase_numbers) & set(checklist_numbers)):
        if phase_states.get(number, (None, 0))[0] != checklist_states.get(number, (None, 0))[0]:
            line = checklist_states.get(number, (None, 0))[1]
            result.add("CKRS014", checklist_doc.path, line, f"phase {number} status or current-phase pointer disagrees across active trackers")
    next_phase = next((number for number in phase_numbers if phase_states.get(number, (None, 0))[0] == "Next"), None)
    if next_phase is not None:
        scope_text = "\n".join(docs["scope"].lines if docs["scope"] else [])
        release_text = "\n".join((current_section(docs["release_readme"], "## Current Release State", result) or (0, []))[1])
        if not re.search(rf"Phase {next_phase}.*(?:Next|next)", scope_text) or not re.search(rf"Phase {next_phase}.*(?:next|Next)", release_text):
            result.add("CKRS014", "docs/release/v1.13", 0, f"phase {next_phase} status or current-phase pointer disagrees across active trackers")
    return phase_blocks(phase_doc)


def artifact_targets(root: Path, containing: str, lines: Iterable[str]) -> list[str]:
    raw: list[str] = []
    for line in lines:
        raw.extend(re.findall(r"\[[^\]]+\]\(([^)]+\.md(?:#[^)]+)?)\)", line))
        raw.extend(re.findall(r"`([^`\n]+\.md(?:#[^`\n]+)?)`", line))
    targets = []
    for value in raw:
        value = value.split("#", 1)[0]
        if not value or re.match(r"https?://", value) or value.startswith("/"):
            continue
        targets.append(safe_relative_artifact(root, containing, value))
    return sorted(set(targets))


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


def infer_state(root: Path, version: str, phase_doc: Optional[Document]) -> tuple[str, tuple[str, str, bool, bool, Optional[str]]]:
    context = git_context(root, version)
    head, branch, exists, annotated, target = context
    env = {key: os.environ.get(key, "") for key in ("GITHUB_EVENT_NAME", "GITHUB_REF", "GITHUB_REF_NAME", "GITHUB_HEAD_REF")}
    if exists and annotated and target == head:
        return "released", context
    if branch == "main" or env["GITHUB_REF"] == "refs/heads/main":
        return "merged-not-tagged", context
    release_branch = f"release/v{version}"
    accepted_pr = env["GITHUB_EVENT_NAME"] == "pull_request" and env["GITHUB_REF"].startswith("refs/pull/") and env["GITHUB_HEAD_REF"] == release_branch
    if branch == release_branch or accepted_pr:
        numbers, states = parse_phase_states(phase_doc, "Status") if phase_doc else ([], {})
        complete = bool(numbers) and all(states.get(number, (None, 0))[0] == "Complete" for number in numbers)
        return ("pre-release" if complete else "development"), context
    raise InternalError("git-context", "unable to infer release lifecycle from the current Git context")


def check_ckrs016(version: str, state: str, context: tuple[str, str, bool, bool, Optional[str]], result: ValidationResult) -> None:
    _, branch, _, _, _ = context
    env = {key: os.environ.get(key, "") for key in ("GITHUB_EVENT_NAME", "GITHUB_REF", "GITHUB_HEAD_REF")}
    release_branch = f"release/v{version}"
    accepted_pr = env["GITHUB_EVENT_NAME"] == "pull_request" and env["GITHUB_REF"].startswith("refs/pull/") and env["GITHUB_HEAD_REF"] == release_branch
    valid = (state in ("development", "pre-release") and (branch == release_branch or accepted_pr)) or (state == "merged-not-tagged" and (branch == "main" or env["GITHUB_REF"] == "refs/heads/main")) or state == "released"
    if not valid:
        result.add("CKRS016", ".git", 0, f"Git context branch={branch or 'detached'} is incompatible with {state} for {version}")


def check_ckrs017(version: str, state: str, context: tuple[str, str, bool, bool, Optional[str]], result: ValidationResult) -> None:
    head, _, exists, annotated, target = context
    if state == "released":
        if not exists or not annotated or target != head:
            detail = "missing annotated tag" if not exists else "tag is lightweight" if not annotated else "peeled target does not equal HEAD"
            result.add("CKRS017", ".git", 0, f"tag v{version} does not satisfy released: {detail}")
    elif exists:
        result.add("CKRS017", ".git", 0, f"tag v{version} does not satisfy {state}: exact tag already exists")


def check_ckrs018(root: Path, version: str, state: str, phase_doc: Optional[Document], result: ValidationResult) -> None:
    directory, stem = release_paths(version)
    path = f"{directory}/{stem}-release-gate.md"
    gate = read_document(root, path, result, required=False)
    complete = phase_doc is not None and all(field_from_lines(block, "Status") == "Complete" for _, _, block in phase_blocks(phase_doc))
    if state == "development":
        if gate and "Passed" in "\n".join(value for _, value in gate.metadata("Status")) and not complete:
            result.add("CKRS018", path, 0, "current release gate is invalid for development: passed gate precedes completed phases")
        return
    if gate is None:
        result.add("CKRS018", path, 0, f"current release gate is invalid for {state}: canonical gate is missing")
        return
    status = [value for _, value in gate.metadata("Status")]
    verdict = gate.section("## Final verdict")
    if not complete or not any("Passed" in value for value in status) or verdict is None:
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
