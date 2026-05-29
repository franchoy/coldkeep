#!/usr/bin/env python3
"""v1.10.12 Phase 3 validation script.

Checks that all required Phase 3 release evidence files exist and contain
the required phrases and CSV rows. Exits 0 on success, 1 on failure.

Usage:
    python3 scripts/validate_phase3.py
"""

import csv
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
        return
    with open(full, encoding="utf-8") as fh:
        content = fh.read()
    if phrase not in content:
        ERRORS.append(
            f"docs/release/v1.10/{rel_path}: missing required phrase: {phrase!r}"
        )


def require_csv_column_value(rel_path, column, value):
    full = _path(rel_path)
    if not os.path.isfile(full):
        return
    with open(full, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    found = any(row.get(column, "").strip() == value for row in rows)
    if not found:
        ERRORS.append(
            f"docs/release/v1.10/{rel_path}: no row with {column}={value!r}"
        )


# ---------------------------------------------------------------------------
# File existence checks
# ---------------------------------------------------------------------------

REQUIRED_FILES = [
    "v1.10.12-db-fs-storage-access-inventory.csv",
    "v1.10.12-db-fs-storage-access-inventory.md",
    "v1.10.12-phase3-db-fs-storage-access-inventory.md",
    "v1.10.12-phase-status.md",
    "v1.10.12-checklist.md",
]

for f in REQUIRED_FILES:
    require_file(f)

# ---------------------------------------------------------------------------
# Inventory CSV checks
# ---------------------------------------------------------------------------

CSV = "v1.10.12-db-fs-storage-access-inventory.csv"

# At least one critical risk surface
require_csv_column_value(CSV, "risk_surface", "critical")

# Required future boundary types
require_csv_column_value(CSV, "future_boundary", "operation_contract_candidate")
require_csv_column_value(CSV, "future_boundary", "catalog_boundary_candidate")
require_csv_column_value(CSV, "future_boundary", "filesystem_boundary_candidate")

# Spot-check key row IDs
full_csv = _path(CSV)
if os.path.isfile(full_csv):
    with open(full_csv, encoding="utf-8") as fh:
        content = fh.read()
    for row_id in ["DBFS-11012-001", "DBFS-11012-002", "DBFS-11012-019",
                   "DBFS-11012-024", "DBFS-11012-031"]:
        if row_id not in content:
            ERRORS.append(f"docs/release/v1.10/{CSV}: missing row {row_id!r}")

# ---------------------------------------------------------------------------
# Inventory markdown checks
# ---------------------------------------------------------------------------

MD = "v1.10.12-db-fs-storage-access-inventory.md"

for phrase in [
    "Direct DB / Filesystem / Storage-Context Access Inventory",
    "Core Invariant",
    "Inputs Inspected",
    "Summary",
    "Risk Summary",
    "Critical Access Surfaces",
    "Future Boundary Candidates",
    "Relationship to Phase 2",
    "Release Boundary",
    "Handoff",
]:
    require_phrase(MD, phrase)

# ---------------------------------------------------------------------------
# Phase 3 evidence doc checks
# ---------------------------------------------------------------------------

P3 = "v1.10.12-phase3-db-fs-storage-access-inventory.md"

for phrase in [
    "Status: Complete",
    "Phase 3",
    "Result",
    "Scope Confirmation",
    "Phase 4 \u2014 Operation Contract Candidate Inventory",
]:
    require_phrase(P3, phrase)

# ---------------------------------------------------------------------------
# Phase status checks
# ---------------------------------------------------------------------------

PS = "v1.10.12-phase-status.md"

for phrase in [
    "Status: Phase 3 complete",
    "| 3 | Direct DB / Filesystem / Storage-Context Access Inventory | Complete |",
    "Phase 4 \u2014 Operation Contract Candidate Inventory",
    "Phase 3 Findings Summary",
]:
    require_phrase(PS, phrase)

# ---------------------------------------------------------------------------
# Checklist checks
# ---------------------------------------------------------------------------

CL = "v1.10.12-checklist.md"

for phrase in [
    "- [x] Phase 3 \u2014 Direct DB / Filesystem / Storage-Context Access Inventory",
    "Phase 3 Gates",
    "Direct DB access inspected",
    "Filesystem access inspected",
    "Future catalog boundary candidates identified",
    "Future filesystem boundary candidates identified",
    "No engine extraction started",
    "No catalog abstraction started",
]:
    require_phrase(CL, phrase)

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

if ERRORS:
    for err in ERRORS:
        print(f"[validate_phase3] ERROR: {err}", file=sys.stderr)
    sys.exit(1)

print("v1.10.12 Phase 3 validation OK")
