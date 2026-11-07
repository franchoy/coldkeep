# Next bounded slice: 2.01 baseline fixtures → 2.03 passive recovery

Recommended Codex model: **GPT-6 Astra**, reasoning **Extra High**. Rationale:
passive construction, lease ownership and recovery-cut closure cross storage
and catalog boundaries and need careful failure analysis. Record actual model
and effort only if observable. Execution scope: local development branch,
disposable fixtures/server/storage, focused implementation and checks, local
commits; no publication or user-repository migration. This is a future handoff,
not authority to implement it in Phase 0.

## Inputs and baseline

Read active instructions, current.json, the adopted charter and original full
2.01/2.03 specifications plus shared C02/C10 and review F03–F05/F10. Bind actual
launch HEAD and preserve unrelated work. Inspect `internal/application/session.go`,
`internal/db`/`db.ConnectDB` schema bootstrap, `internal/storage` startup recovery,
`internal/coordination`, catalog reachability and existing test fixture builders.
Do not assume application.Open or OpenLocalStorage is passive.

First complete representative baseline fixtures with v1.13.19 reader identity
D19 `d3581602aee2599547564322fe083baae1d52063`, pinned Go/dependencies and
schema 17. Reuse existing fixture builders/readers and historical regression
cases; record a small manifest of expected bytes, metadata, supported names,
reader versions and separately retained test keys. Cover plaintext/AES-GCM,
legacy/packed, none/zstd compression, empty files, multiple historical versions,
snapshots, metadata, interrupted/inconsistent states and distinct native names.
Distinguish inherited catalog history from fresh capture generations. Preserve
unreadable fixtures as failing evidence rather than regenerating them.

## Input/output/failure contract

Input: explicit existing backend/catalog, validated payload root, legacy lease,
separate new export destination with capacity, trusted compatible database
backup tools, reader material and independent key custody. Resolve one immutable
configuration tuple before constructing adapters. No ambient fallback or split
root between lease, writer and maintenance.

Passive inspect/open-existing output: version/capability/layout inventory and
compatibility verdict, with no database/root creation, schema migration, startup
repair or repository identity writes. Missing database/root, future schema,
wrong root, missing key or inconsistent graph fails explicitly without fallback.
Close partial resources before releasing ownership. Restore rehearsals use a
copied/imported catalog because restore can mutate pin records.

Quiesced recovery output: consistent **full catalog** and complete referenced
closure (whole physical containers where needed), copied bytes rather than live
hard links; versioned bounded manifest with provenance, catalog/payload/package
versions, schema/reader/capability/key requirements, object sizes/checksums and
completion marker. Pre-identity v1 uses package-local legacy provenance; it does
not gain schema/IDs merely to satisfy the manifest. Retain obtainable reader
material and document PostgreSQL tool/ownership mapping. Trusted operator SQL
backup is distinct from untrusted browser uploads. SQLite, if included, needs a
qualified backup/closed-DB procedure that accounts for WAL; it is not a product
backend-default selection.

Failure: missing objects, bad digests, unsupported versions, interrupted copy,
space exhaustion or lost exclusion leaves incomplete output that normal import
refuses. If exclusion is released, prove the same catalog/object cut before
resume or start a new generation. Preserve inconsistent source as forensic
material; repair is a separate operation on a copy, never silently part of
inspection. Same-device export establishes portability, not independent failure
domain protection. No source originals are deleted.

## Required exit evidence before schema additions

1. Baseline reader restores every selected historical fixture to expected bytes
   and supported metadata; complete reader/schema/failure-limit inventory.
2. Missing-root/database and future-schema inspection causes zero source writes;
   passive inspection leaves schema and payload/control inventory unchanged.
3. Interrupt export around metadata, payload copy and finalization; incomplete
   markers and corrupt/missing members fail closed, with bounded resource use.
4. Make original catalog/storage/installation unavailable. Import only completed
   material into a fresh disposable target, supply keys separately, verify graph
   closure and restore selected history with exact expected hashes.
5. Run later live GC on the disposable original and show completed copied export
   still restores. Recovered activation must not revive old jobs, admission tokens
   or control credentials; required epoch handling is owned by 2.07/2.15.

Only after this recovery evidence may later actions add schema. Keep accepted
intent, committed history, completeness, verification, independent protection
and installed restore outcomes separate in the result.

After baseline fixtures are fixed, 2.02 can prove a minimal packaged daemon with
actual embedded UI, authenticated internal history/restore journey, explicit
frontend-then-Go clean-source build and Linux x86-64 CGO/SQLite native feasibility.
Record linked runtime and failures; no support claim follows from compilation.
PostgreSQL Docker remains the first product path. This packaging work does not
wait for Windows/macOS resources and does not authorize unrelated upgrades.
