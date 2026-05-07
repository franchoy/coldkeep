#!/usr/bin/env python3

import json
import math
import sys


def usage() -> None:
    print(
        "Usage: scripts/compare_phase8_dedup_results.py <result_1m.json> <result_2m.json> [--max-delta-ratio <r>]",
        file=sys.stderr,
    )


def load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_metrics(doc: dict) -> dict:
    data = (doc.get("data") or {})
    dedup = data.get("dedup") or {}
    counts = data.get("counts") or {}
    restore_validation = data.get("restore_validation") or []

    return {
        "block_mb": int(data.get("block_target_size_mb", 0)),
        "dataset": str(data.get("dataset", "")),
        "run_id": str(data.get("run_id", "")),
        "chunk_incremental_ratio": float(dedup.get("chunk_incremental_ratio", 0.0)),
        "block_incremental_ratio": float(dedup.get("block_incremental_ratio", 0.0)),
        "new_chunks_v2": int(counts.get("new_chunks_v2", 0)),
        "new_blocks_v2": int(counts.get("new_blocks_v2", 0)),
        "chunks_after_v1": int(counts.get("chunks_after_v1", 0)),
        "blocks_after_v1": int(counts.get("blocks_after_v1", 0)),
        "v1_source_tree_hash": str((restore_validation[0].get("source_tree_hash") if len(restore_validation) > 0 else "")),
        "v1_restored_tree_hash": str((restore_validation[0].get("restored_tree_hash") if len(restore_validation) > 0 else "")),
        "v2_source_tree_hash": str((restore_validation[1].get("source_tree_hash") if len(restore_validation) > 1 else "")),
        "v2_restored_tree_hash": str((restore_validation[1].get("restored_tree_hash") if len(restore_validation) > 1 else "")),
    }


def main() -> int:
    if len(sys.argv) < 3:
        usage()
        return 2

    result_a = sys.argv[1]
    result_b = sys.argv[2]

    max_delta_ratio = 0.10
    if len(sys.argv) > 3:
        if len(sys.argv) != 5 or sys.argv[3] != "--max-delta-ratio":
            usage()
            return 2
        max_delta_ratio = float(sys.argv[4])
        if max_delta_ratio < 0:
            raise SystemExit("--max-delta-ratio must be >= 0")

    m_a = extract_metrics(load(result_a))
    m_b = extract_metrics(load(result_b))

    by_block = {m_a["block_mb"]: m_a, m_b["block_mb"]: m_b}
    if 1 not in by_block or 2 not in by_block:
        raise SystemExit("comparison requires one 1 MiB result and one 2 MiB result")

    one = by_block[1]
    two = by_block[2]

    failures = []
    warnings = []

    for label in ("v1", "v2"):
        src_key = f"{label}_source_tree_hash"
        dst_key = f"{label}_restored_tree_hash"
        if one[src_key] == "" or two[src_key] == "":
            failures.append(f"missing {label} source tree hash")
            continue
        if one[src_key] != two[src_key]:
            failures.append(
                f"chunk identity invariant failed proxy ({label} source tree hash differs across block sizes): "
                f"1MiB={one[src_key]} 2MiB={two[src_key]}"
            )
        if one[dst_key] != one[src_key]:
            failures.append(f"1MiB {label} restored tree hash does not match source")
        if two[dst_key] != two[src_key]:
            failures.append(f"2MiB {label} restored tree hash does not match source")

    chunk_delta = abs(one["chunk_incremental_ratio"] - two["chunk_incremental_ratio"])
    block_delta = abs(one["block_incremental_ratio"] - two["block_incremental_ratio"])

    if chunk_delta > max_delta_ratio:
        warnings.append(
            f"dedup chunk incremental ratio differs significantly: delta={chunk_delta:.6f} > {max_delta_ratio:.6f}"
        )
    if block_delta > max_delta_ratio:
        warnings.append(
            f"dedup block incremental ratio differs significantly: delta={block_delta:.6f} > {max_delta_ratio:.6f}"
        )

    out = {
        "status": "ok" if not failures else "failed",
        "comparison": {
            "max_delta_ratio": max_delta_ratio,
            "one_mib": {
                "chunk_incremental_ratio": one["chunk_incremental_ratio"],
                "block_incremental_ratio": one["block_incremental_ratio"],
                "new_chunks_v2": one["new_chunks_v2"],
                "new_blocks_v2": one["new_blocks_v2"],
            },
            "two_mib": {
                "chunk_incremental_ratio": two["chunk_incremental_ratio"],
                "block_incremental_ratio": two["block_incremental_ratio"],
                "new_chunks_v2": two["new_chunks_v2"],
                "new_blocks_v2": two["new_blocks_v2"],
            },
            "delta": {
                "chunk_incremental_ratio": chunk_delta,
                "block_incremental_ratio": block_delta,
            },
        },
        "warnings": warnings,
        "failures": failures,
        "investigate": len(warnings) > 0,
    }

    print(json.dumps(out, indent=2, sort_keys=True))

    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
