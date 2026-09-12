#!/usr/bin/env python3
"""Validate mandatory go test -json execution evidence for frozen profiles."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROFILES = {
    "integration-correctness-plain": {
        "package": "github.com/franchoy/coldkeep/tests/integration",
        "tests": (
            "TestRoundTripStoreRestore",
            "TestRemoveWithSharedChunksRefCount",
            "TestStartupRecoveryResyncsPreexistingQuarantinedOrphanConflictState",
            "TestSimulationMatchesRealSizeMetrics",
            "TestVerifySystemFullRejectsNonContiguousPackedBlockOffsets",
            "TestVerifySystemDeepCLIRejectsTwoPreexistingPackedPhysicalCorruptionsAtPreflight",
        ),
    },
    "integration-correctness-aes-gcm": {
        "package": "github.com/franchoy/coldkeep/tests/integration",
        "tests": ("TestRoundTripStoreRestore",),
    },
    "ck014-internal-verify": {
        "package": "github.com/franchoy/coldkeep/internal/verify",
        "tests": (
            "TestVerifySystemDeepPreflightFailureDoesNotInvokeDownstreamReader",
            "TestVerifySystemDeepCollectsTwoInjectedDownstreamPhysicalFaults",
            "TestVerifySystemDeepInjectedDownstreamReaderCleanPipelinePasses",
            "TestVerifySystemDeepRejectsNilDownstreamReaderAfterPreflight",
        ),
    },
}


def load_events(path: Path) -> tuple[list[dict[str, object]], list[str]]:
    events: list[dict[str, object]] = []
    failures: list[str] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if not raw_line.strip():
                    continue
                try:
                    event = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    failures.append(f"malformed JSON line {line_number}: {exc}")
                    continue
                if not isinstance(event, dict):
                    failures.append(f"JSON line {line_number} is not an object")
                    continue
                events.append(event)
    except OSError as exc:
        failures.append(f"cannot read {path}: {exc}")
    if not events:
        failures.append("JSON evidence file contained no events")
    return events, failures


def validate_events(profile_name: str, events: list[dict[str, object]]) -> list[str]:
    profile = PROFILES.get(profile_name)
    if profile is None:
        return [f"unknown required-test profile: {profile_name}"]

    expected_package = str(profile["package"])
    failures: list[str] = []
    package_events = [
        (index, event)
        for index, event in enumerate(events)
        if event.get("Package") == expected_package
    ]
    package_level = [
        (index, event)
        for index, event in package_events
        if not event.get("Test")
    ]
    starts = [index for index, event in package_level if event.get("Action") == "start"]
    terminals = [
        (index, str(event.get("Action")))
        for index, event in package_level
        if event.get("Action") in {"pass", "fail", "skip"}
    ]
    if not starts:
        failures.append(f"expected package start missing: package={expected_package}")
    if len(terminals) != 1 or terminals[0][1] != "pass":
        failures.append(
            f"expected package did not complete with one pass: package={expected_package} terminals={terminals}"
        )
    elif starts and starts[0] >= terminals[0][0]:
        failures.append(f"expected package pass preceded its start: package={expected_package}")

    for test_name in profile["tests"]:
        matching = [
            (index, event)
            for index, event in package_events
            if event.get("Test") == test_name
        ]
        runs = [index for index, event in matching if event.get("Action") == "run"]
        test_terminals = [
            (index, str(event.get("Action")))
            for index, event in matching
            if event.get("Action") in {"pass", "fail", "skip"}
        ]
        if len(runs) != 1:
            failures.append(
                f"required run count mismatch: package={expected_package} test={test_name} runs={len(runs)}"
            )
        if len(test_terminals) != 1 or test_terminals[0][1] != "pass":
            failures.append(
                f"required terminal pass missing or contradictory: package={expected_package} test={test_name} terminals={test_terminals}"
            )
        elif len(runs) == 1 and runs[0] >= test_terminals[0][0]:
            failures.append(
                f"required pass did not follow run: package={expected_package} test={test_name}"
            )

    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, choices=tuple(PROFILES))
    parser.add_argument("--events", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    events, failures = load_events(args.events)
    if not failures:
        failures.extend(validate_events(args.profile, events))
    if failures:
        print("required execution-proof failure:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"required execution-proof pass: profile={args.profile}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
