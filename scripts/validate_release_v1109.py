#!/usr/bin/env python3
"""v1.10.9 Phase 8 pre-PR validation script.

Checks that all required Phase 8 release evidence files exist and contain
the required phrases. Exits 0 on success, 1 on failure.

Usage:
    python3 scripts/validate_release_v1109.py
"""

import os
import sys

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
RELEASE_DIR = os.path.join(REPO_ROOT, "docs", "release", "v1.10")

ERRORS = []


def _path(*parts):
    return os.path.join(RELEASE_DIR, *parts)


def require_file(rel_path):
    full = _path(rel_path)
    if not os.path.isfile(full):
        ERRORS.append(f"missing file: docs/release/v1.10/{rel_path}")
        return False
    return True


def require_phrase(rel_path, phrase):
    full = _path(rel_path)
    if not os.path.isfile(full):
        # Missing file already reported
        return
    with open(full, encoding="utf-8") as fh:
        content = fh.read()
    if phrase not in content:
        ERRORS.append(
            f"docs/release/v1.10/{rel_path}: missing required phrase: {phrase!r}"
        )


def require_csv_row(rel_path, row_id):
    full = _path(rel_path)
    if not os.path.isfile(full):
        return
    with open(full, encoding="utf-8") as fh:
        content = fh.read()
    if row_id not in content:
        ERRORS.append(
            f"docs/release/v1.10/{rel_path}: missing required CSV row: {row_id!r}"
        )


# ---------------------------------------------------------------------------
# File existence checks
# ---------------------------------------------------------------------------

REQUIRED_FILES = [
    "v1.10.9-scope.md",
    "v1.10.9-fault-model.md",
    "v1.10.9-phase-status.md",
    "v1.10.9-checklist.md",
    "v1.10.9-test-inventory.csv",
    "v1.10.9-phase0-baseline.md",
    "v1.10.9-phase1-scripted-fault-fs.md",
    "v1.10.9-phase2-restore-fault-injection.md",
    "v1.10.9-phase3-container-fault-injection.md",
    "v1.10.9-phase4-recovery-quarantine-fault-injection.md",
    "v1.10.9-phase5-gc-delete-failure.md",
    "v1.10.9-phase6-no-silent-corruption-verification.md",
    "v1.10.9-local-validation-results.md",
    "v1.10.9-phase7-local-validation.md",
    "v1.10.9-pr-summary.md",
    "v1.10.9-release-notes.md",
    "v1.10.9-phase8-pr-ci-codacy-merge-release.md",
    "v1.10.9-final-release-gate.md",
]

for f in REQUIRED_FILES:
    require_file(f)

# ---------------------------------------------------------------------------
# Phase 8 doc content checks
# ---------------------------------------------------------------------------

# PR summary
require_phrase("v1.10.9-pr-summary.md", "Filesystem Fault Injection Phase 1")
require_phrase("v1.10.9-pr-summary.md", "deterministic, test-only filesystem fault injection")
require_phrase("v1.10.9-pr-summary.md", "Core Invariant")
require_phrase("v1.10.9-pr-summary.md", "Scripted fault filesystem helper")
require_phrase("v1.10.9-pr-summary.md", "Restore fault scenarios")
require_phrase("v1.10.9-pr-summary.md", "Container write/seal fault scenarios")
require_phrase("v1.10.9-pr-summary.md", "Recovery / quarantine fault scenarios")
require_phrase("v1.10.9-pr-summary.md", "GC delete failure scenario")
require_phrase("v1.10.9-pr-summary.md", "No-silent-corruption matrix")
require_phrase("v1.10.9-pr-summary.md", "Explicit non-goals")

# Release notes
require_phrase("v1.10.9-release-notes.md", "Coldkeep v1.10.9")
require_phrase("v1.10.9-release-notes.md", "Filesystem Fault Injection Phase 1")
require_phrase("v1.10.9-release-notes.md", "No migration required")
require_phrase("v1.10.9-release-notes.md", "does not expose production fault injection")

# Phase 8 tracking doc
require_phrase("v1.10.9-phase8-pr-ci-codacy-merge-release.md", "Status: In Progress")
require_phrase("v1.10.9-phase8-pr-ci-codacy-merge-release.md", "PR URL: TBD")
require_phrase("v1.10.9-phase8-pr-ci-codacy-merge-release.md", "CI status: TBD")
require_phrase("v1.10.9-phase8-pr-ci-codacy-merge-release.md", "Codacy status: TBD")
require_phrase("v1.10.9-phase8-pr-ci-codacy-merge-release.md", "Merge commit SHA: TBD")
require_phrase("v1.10.9-phase8-pr-ci-codacy-merge-release.md", "GitHub release URL: TBD")

# Final release gate
require_phrase("v1.10.9-final-release-gate.md", "Status: Pending")
require_phrase("v1.10.9-final-release-gate.md", "PR opened")
require_phrase("v1.10.9-final-release-gate.md", "CI passed")
require_phrase("v1.10.9-final-release-gate.md", "Codacy reviewed")
require_phrase("v1.10.9-final-release-gate.md", "Tag `v1.10.9` pushed")
require_phrase("v1.10.9-final-release-gate.md", "GitHub release published")

# Phase status
require_phrase("v1.10.9-phase-status.md", "Status: Phase 8 in progress")
require_phrase("v1.10.9-phase-status.md", "Phase 8 Findings Summary")

# ---------------------------------------------------------------------------
# CSV row checks
# ---------------------------------------------------------------------------

CSV = "v1.10.9-test-inventory.csv"

REQUIRED_ROWS = [
    "CK-1109-P0-BASELINE",
    "CK-1109-P1-SCRIPTED-FAULT-FS",
    "CK-1109-P2-RESTORE-FAULTS",
    "CK-1109-P3-CONTAINER-FAULTS",
    "CK-1109-P4-RECOVERY-QUARANTINE-FAULTS",
    "CK-1109-P5-GC-DELETE-FAULTS",
    "CK-1109-P6-NO-SILENT-CORRUPTION-MATRIX",
    "CK-1109-P7-LOCAL-VALIDATION",
    "CK-1109-P8-PR-CI-CODACY-MERGE-RELEASE",
]

for row_id in REQUIRED_ROWS:
    require_csv_row(CSV, row_id)

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

if ERRORS:
    for err in ERRORS:
        print(f"[validate_release_v1109] ERROR: {err}", file=sys.stderr)
    sys.exit(1)

print("v1.10.9 Phase 8 pre-PR validation OK")
