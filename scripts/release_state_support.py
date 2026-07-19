"""Private support primitives for the Coldkeep release-state validator."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Iterable, Optional


SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
CHANGELOG_HEADING = re.compile(r"^## (?:v|\[)(\d+\.\d+\.\d+)(?:\]|\b)(.*)$")
PHASE_HEADING = re.compile(r"^## Phase (\d+)\b")
METADATA = re.compile(r"^\*\*(Release|Status|Branch|Phase status):\*\*\s*(.*)$")
ProcessResult = subprocess.CompletedProcess[str]


@dataclass(frozen=True)
class Violation:
    """One deterministic release-state contract violation."""

    rule: str
    path: str
    line: int
    message: str


@dataclass
class ValidationResult:
    """Accumulated validator state and violations."""

    state: Optional[str]
    active_version: Optional[str]
    violations: list[Violation] = field(default_factory=list)

    def add(self, rule: str, path: str, line: int, message: str) -> None:
        self.violations.append(Violation(rule, path, line, message))

    def ordered(self) -> list[Violation]:
        return sorted(
            self.violations,
            key=lambda item: (item.rule, item.path, item.line, item.message),
        )


class InternalError(Exception):
    """A deterministic validator failure outside the CKRS rule catalogue."""

    def __init__(self, kind: str, message: str) -> None:
        """Create an internal error with a stable machine-readable kind."""
        super().__init__(message)
        self.kind = kind
        self.message = message


@dataclass
class Document:
    """A UTF-8 repository document with bounded Markdown helpers."""

    root: Path
    path: str
    lines: list[str]

    @classmethod
    def load(cls, root: Path, path: str) -> "Document":
        target = root / path
        if not target.is_file():
            raise FileNotFoundError(path)
        return cls(root, path, target.read_text(encoding="utf-8").splitlines())

    def section(self, heading: str) -> Optional[tuple[int, list[str]]]:
        marker = next(
            (index for index, line in enumerate(self.lines) if line == heading),
            None,
        )
        if marker is None:
            return None
        level = len(heading) - len(heading.lstrip("#"))
        end = next(
            (
                index
                for index in range(marker + 1, len(self.lines))
                if heading_closes_section(self.lines[index], level)
            ),
            len(self.lines),
        )
        return marker + 1, self.lines[marker:end]

    def metadata(self, name: str) -> list[tuple[int, str]]:
        values = []
        for index, line in enumerate(self.lines, 1):
            match = METADATA.match(line)
            if match and match.group(1) == name:
                values.append((index, match.group(2).strip()))
        return values


def heading_closes_section(line: str, level: int) -> bool:
    """Return whether a Markdown heading closes a bounded section."""
    if not line.startswith("#"):
        return False
    candidate_level = len(line) - len(line.lstrip("#"))
    return candidate_level <= level and line.startswith("#" * candidate_level + " ")


def normalized(path: Path, root: Path) -> str:
    """Normalize a path to a repository-relative POSIX representation."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def resolved_executable(name: str) -> str:
    """Resolve an executable to an absolute path or fail deterministically."""
    executable = shutil.which(name)
    if executable is None:
        raise InternalError("git", f"unable to locate executable: {name}")
    return str(Path(executable).resolve())


def run_process(
    argv: list[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    check: bool = False,
) -> ProcessResult:
    """Run reviewed list-form argv without shell interpretation."""
    return subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
        shell=False,
    )


def run_git(
    root: Path,
    args: list[str],
    allow_failure: bool = False,
) -> ProcessResult:
    """Run a read-only Git inspection command with stable error handling."""
    try:
        completed = run_process(
            [resolved_executable("git"), "-C", str(root), *args],
            check=False,
        )
    except OSError as exc:
        raise InternalError("git", f"unable to execute git: {exc}") from exc
    if completed.returncode and not allow_failure:
        detail = (
            completed.stderr.strip()
            or completed.stdout.strip()
            or "git command failed"
        )
        raise InternalError("git", detail)
    return completed


def validate_root(value: Optional[str]) -> Path:
    """Validate repository anchors and return the canonical worktree root."""
    root = Path(value).resolve() if value else Path(__file__).resolve().parents[1]
    anchors_exist = (
        root.is_dir()
        and (root / ".git").exists()
        and (root / "internal/version/version.go").is_file()
        and (root / "CHANGELOG.md").is_file()
    )
    if not anchors_exist:
        raise InternalError(
            "repository-layout",
            "repository root is missing required anchors",
        )
    top = run_git(root, ["rev-parse", "--show-toplevel"]).stdout.strip()
    if Path(top).resolve() != root:
        raise InternalError(
            "repository-layout",
            "repository root is not the Git worktree root",
        )
    return root


def read_document(
    root: Path,
    path: str,
    result: ValidationResult,
    required: bool = True,
) -> Optional[Document]:
    """Read a release document and map missing inputs to CKRS019."""
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


def parse_source_version(root: Path, result: ValidationResult) -> Optional[str]:
    """Parse the three authoritative Go version constants."""
    path = "internal/version/version.go"
    text = (root / path).read_text(encoding="utf-8")
    parts: list[str] = []
    for name in ("Major", "Minor", "Patch"):
        matches = re.findall(rf"(?m)^\s*{name}\s*=\s*([^\s/]+)", text)
        valid = (
            len(matches) == 1
            and re.fullmatch(r"\d+", matches[0]) is not None
            and int(matches[0]) >= 0
        )
        if not valid:
            result.add(
                "CKRS001",
                path,
                0,
                "source version must contain exactly one integer Major, Minor, and Patch declaration",
            )
            return None
        parts.append(matches[0])
    version = ".".join(parts)
    if not SEMVER.fullmatch(version):
        result.add(
            "CKRS001",
            path,
            0,
            "source version must contain exactly one integer Major, Minor, and Patch declaration",
        )
        return None
    return version


def version_from_release(value: str) -> Optional[tuple[str, str]]:
    """Parse version and title from a release metadata field."""
    match = re.search(r"`?(v\d+\.\d+\.\d+)\s+—\s+([^`]+?)`?$", value)
    if not match:
        return None
    return match.group(1)[1:], match.group(2).strip()


def heading_versions(doc: Document) -> list[tuple[int, str, str]]:
    """Return recognized changelog version headings."""
    values = []
    for index, line in enumerate(doc.lines, 1):
        match = CHANGELOG_HEADING.match(line)
        if match:
            values.append((index, match.group(1), match.group(2).strip()))
    return values


def phase_blocks(doc: Document) -> list[tuple[int, int, list[str]]]:
    """Split a tracker into Phase N blocks."""
    starts = [
        (index, int(match.group(1)))
        for index, line in enumerate(doc.lines)
        if (match := PHASE_HEADING.match(line))
    ]
    blocks = []
    for offset, (start, number) in enumerate(starts):
        end = starts[offset + 1][0] if offset + 1 < len(starts) else len(doc.lines)
        blocks.append((number, start, doc.lines[start:end]))
    return blocks


def field_from_lines(lines: Iterable[str], name: str) -> Optional[str]:
    """Read one anchored metadata field from bounded lines."""
    pattern = re.compile(rf"^\*\*{re.escape(name)}:\*\*\s*(.+)$")
    values = [
        match.group(1).strip()
        for line in lines
        if (match := pattern.match(line))
    ]
    return values[0] if len(values) == 1 else None


def parse_phase_states(
    doc: Document,
    metadata_field: str,
) -> tuple[list[int], dict[int, tuple[str, int]]]:
    """Read phase numbers and one aggregate status per phase."""
    numbers: list[int] = []
    states: dict[int, tuple[str, int]] = {}
    for number, start, block in phase_blocks(doc):
        numbers.append(number)
        values = []
        pattern = re.compile(
            rf"^\*\*{re.escape(metadata_field)}:\*\*\s*(.+)$",
        )
        for relative, line in enumerate(block, 1):
            match = pattern.match(line)
            if match:
                values.append((match.group(1).strip(), start + relative + 1))
        if len(values) == 1:
            states[number] = values[0]
    return numbers, states


def release_paths(version: str) -> tuple[str, str]:
    """Return the release directory and versioned filename stem."""
    major, minor, _ = version.split(".")
    directory = f"docs/release/v{major}.{minor}"
    return directory, f"v{version}"


def active_docs(
    root: Path,
    version: str,
    result: ValidationResult,
) -> dict[str, Optional[Document]]:
    """Load the bounded current release-state document set."""
    directory, stem = release_paths(version)
    major, minor, _ = version.split(".")
    return {
        "version_test": read_document(root, "internal/version/version_test.go", result),
        "checklist": read_document(root, "PRE_RELEASE_CHECKLIST.md", result),
        "changelog": read_document(root, "CHANGELOG.md", result),
        "readme": read_document(root, "README.md", result),
        "release_readme": read_document(root, f"{directory}/README.md", result),
        "scope": read_document(root, f"{directory}/{stem}-scope.md", result),
        "phase_list": read_document(
            root,
            f"{directory}/{stem}-phase-list.md",
            result,
        ),
        "phase_checklist": read_document(
            root,
            f"{directory}/{stem}-validation-checklist.md",
            result,
        ),
        "train": read_document(
            root,
            f"{directory}/v{major}.{minor}.x-release-train.md",
            result,
        ),
        "reconciliation": read_document(
            root,
            f"{directory}/{stem}-release-train-reconciliation.md",
            result,
        ),
        "contract": read_document(
            root,
            f"{directory}/{stem}-release-state-validator-contract.md",
            result,
        ),
    }


def safe_relative_artifact(root: Path, containing: str, value: str) -> str:
    """Resolve one relative Markdown artifact without escaping the root."""
    first = PurePosixPath(value).parts[0] if PurePosixPath(value).parts else ""
    root_names = {item.name for item in root.iterdir()}
    candidate = root / value if first in root_names else (root / containing).parent / value
    try:
        return candidate.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return "../" + value
