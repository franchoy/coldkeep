#!/usr/bin/env python3

"""
Compare Phase 8 GC benchmark results between 1 MiB and 2 MiB block-size candidates.

The primary comparison metric is retained_dead_bytes_due_to_packed_blocks:
2 MiB blocks are expected to retain more unreclaimable dead space because
each packed block contains more chunks, meaning a partially-live block wastes
more bytes than a smaller block in the same scenario.

Usage:
  scripts/compare_phase8_gc_results.py <result_1m.json> <result_2m.json> [options]

Options:
  --output-json    emit JSON comparison result instead of human text
"""

import json
import sys


def usage() -> None:
    print(
        "Usage: scripts/compare_phase8_gc_results.py <result_1m.json> <result_2m.json> [--output-json]",
        file=sys.stderr,
    )


def load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract(doc: dict) -> dict:
    data = (doc.get("data") or {})
    sim = data.get("gc_simulation") or {}
    removal = data.get("removal") or {}
    restore_val = data.get("restore_validation") or {}
    return {
        "block_mb": int(data.get("block_target_size_mb", 0)),
        "dataset": str(data.get("dataset", "")),
        "run_id": str(data.get("run_id", "")),
        "logically_reclaimable_bytes": int(sim.get("logically_reclaimable_bytes", 0)),
        "physically_reclaimable_bytes": int(sim.get("physically_reclaimable_bytes", 0)),
        "retained_dead_bytes": int(sim.get("retained_dead_bytes_due_to_packed_blocks", 0)),
        "packed_blocks_dead": int(sim.get("packed_blocks_dead", 0)),
        "fully_reclaimable_containers": int(sim.get("fully_reclaimable_containers", 0)),
        "removed_count": int(removal.get("removed_count", 0)),
        "kept_count": int(removal.get("kept_count", 0)),
        "restore_ok": bool(restore_val.get("no_hash_mismatch", False)),
    }


def main() -> int:
    if len(sys.argv) < 3:
        usage()
        return 2

    result_a = sys.argv[1]
    result_b = sys.argv[2]
    emit_json = "--output-json" in sys.argv[3:]

    m_a = extract(load(result_a))
    m_b = extract(load(result_b))

    by_block = {m_a["block_mb"]: m_a, m_b["block_mb"]: m_b}
    if 1 not in by_block or 2 not in by_block:
        raise SystemExit("comparison requires one 1 MiB result and one 2 MiB result")

    one = by_block[1]
    two = by_block[2]

    failures = []
    annotations = []

    if not one["restore_ok"]:
        failures.append("1 MiB: restore validation failed")
    if not two["restore_ok"]:
        failures.append("2 MiB: restore validation failed")

    retained_delta = two["retained_dead_bytes"] - one["retained_dead_bytes"]

    if retained_delta > 0:
        annotations.append(
            f"2 MiB retains more dead space: +{retained_delta} bytes "
            f"(1MiB={one['retained_dead_bytes']} 2MiB={two['retained_dead_bytes']})"
        )
    elif retained_delta < 0:
        annotations.append(
            f"1 MiB retains more dead space unexpectedly: {abs(retained_delta)} bytes "
            f"(1MiB={one['retained_dead_bytes']} 2MiB={two['retained_dead_bytes']})"
        )
    else:
        annotations.append(
            f"retained_dead_bytes identical (both {one['retained_dead_bytes']} bytes)"
        )

    # Hint for decision: mark the block size that is more favorable for storage efficiency
    if two["retained_dead_bytes"] > one["retained_dead_bytes"]:
        decision_hint = (
            "1 MiB is more space-efficient: retains fewer dead bytes after GC. "
            "If throughput difference is small, 1 MiB is preferred."
        )
    elif two["retained_dead_bytes"] < one["retained_dead_bytes"]:
        decision_hint = (
            "2 MiB retains fewer dead bytes after GC (unexpected; investigate dataset composition)."
        )
    else:
        decision_hint = "Both block sizes retain equal dead bytes after GC."

    out = {
        "status": "ok" if not failures else "failed",
        "comparison": {
            "one_mib": {
                "logically_reclaimable_bytes": one["logically_reclaimable_bytes"],
                "physically_reclaimable_bytes": one["physically_reclaimable_bytes"],
                "retained_dead_bytes_due_to_packed_blocks": one["retained_dead_bytes"],
                "packed_blocks_dead": one["packed_blocks_dead"],
                "fully_reclaimable_containers": one["fully_reclaimable_containers"],
            },
            "two_mib": {
                "logically_reclaimable_bytes": two["logically_reclaimable_bytes"],
                "physically_reclaimable_bytes": two["physically_reclaimable_bytes"],
                "retained_dead_bytes_due_to_packed_blocks": two["retained_dead_bytes"],
                "packed_blocks_dead": two["packed_blocks_dead"],
                "fully_reclaimable_containers": two["fully_reclaimable_containers"],
            },
            "delta": {
                "retained_dead_bytes": retained_delta,
            },
        },
        "decision_hint": decision_hint,
        "annotations": annotations,
        "failures": failures,
    }

    if emit_json:
        print(json.dumps(out, indent=2, sort_keys=True))
    else:
        print(f"GC Phase 8 Comparison — 1 MiB vs 2 MiB")
        print(f"  1 MiB retained_dead_bytes_due_to_packed_blocks : {one['retained_dead_bytes']}")
        print(f"  2 MiB retained_dead_bytes_due_to_packed_blocks : {two['retained_dead_bytes']}")
        print(f"  delta (2MiB - 1MiB)                           : {retained_delta:+d}")
        print()
        print(f"  1 MiB physically_reclaimable_bytes             : {one['physically_reclaimable_bytes']}")
        print(f"  2 MiB physically_reclaimable_bytes             : {two['physically_reclaimable_bytes']}")
        print()
        for ann in annotations:
            print(f"  NOTE: {ann}")
        print()
        print(f"  DECISION: {decision_hint}")
        if failures:
            print()
            for f in failures:
                print(f"  FAIL: {f}")

    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
