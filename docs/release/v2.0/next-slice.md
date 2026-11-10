# Phase 3C2 handoff

Phase 3C1 is the currently adopted bounded implementation unit; see its
[result](phase3c1-result.md) for measured qualification and the exact source
checkpoint. [CI follow-up acceptance](ci-acceptance-bridge.md) closes the stale
CI handoff at `3aa9cae7b15037e6f51c22311e2b2ee0c0178b86`. All preceding result
files remain unchanged point-in-time evidence.

Parent action 2.04: ACTIVE. Phase 3C: ACTIVE. Phase 3C2: OUTSTANDING.
Implementation: ACTIVE. Product gates: INCOMPLETE. Keep PR #129 draft,
`release/v2.0` as the development branch, main and stable v1.13.19 unchanged.

C2 must use the exact generation IDs, source/binding provenance and raw names in
[generation-model.md](generation-model.md). It owns bounded typed indexed reads,
keyset pagination and DTO round trips, followed by integrated action closure.
It must not browse by loading Inventory.Catalog and slicing or by OFFSET.
Proposed measured ceilings: 100 returned/101 fetched rows, 4-KiB cursor and
1-MiB encoded response, with finite caller context. Bind each cursor to exact
repository/source/generation/parent, query version/filter/order, fixed watermark
and final unique key. Refuse cross-scope and stale cursors. Test delayed commit
below a watermark; a sequence alone is not commit order. Measure EXPLAIN
(ANALYZE, BUFFERS), rows, response bytes and allocation at increasing sizes,
including equal keys, insertion between pages, cancellation and maximum names.
No API authorization, cursor secret or capture/publication receipt is invented.

Only a later adopted C2 BUILD may add integrated authority transition. Preserve
3A/3B owning reports, require owning C1/C2 results, and add negative tests proving
that old reports or declarations cannot clear 2.04. Current structural support
can clear only C1; parent 3C and C2 remain outstanding obligations.

All schema20 ordinary writers, GC, repair and deletion remain fenced. Generation
reference preservation proves recovery while reclamation is fenced; it does not
prove schema20 live-GC survival. The later publication owner must integrate the
common reachability predicate before activation. All IMPORT_READY targets,
including v4/malformed/future receipts, remain recovery-only. Capture, durable
jobs, product generation restore and UI remain with later owners.

Carry the Linux/PostgreSQL16 session-bound physical profile and exact prior
profiles; no Windows/native-byte runtime, physical-media authenticity, hardware
independence, real reboot/unmount or cross-filesystem transfer is newly qualified.
Retain source-bound reuse and genuine skips from their owning evidence.

Recommended C2 settings: GPT-6 Astra / Extra High for the read-consistency and
integration proof. Record requested and observable actual settings separately;
use unknown when actual settings are unavailable. C2 requires its own adoption.
