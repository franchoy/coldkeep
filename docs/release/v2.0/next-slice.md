# Next bounded slice: 2.03 passive open and recovery-cut inventory

Recommended model: **GPT-6 Astra**, effort **Extra High**. Scope: a separate local
BUILD run on disposable repositories, focused implementation/tests and local
commits; no publication, user migration or schema additions. Rationale: opening
mode, root/key/backend identity, ownership and complete reachability must agree
before an exporter can safely reuse them. Record actual settings only if
observable; no account/global configuration change is implied.

## Inputs

Bind the completed 2.01 candidate and preserve unrelated work. Read the original
full 2.03 specification, shared C02/C10 and adopted F03–F05/F09/F10, the
[baseline ledger](baseline-compatibility.md), [product contract](product-contract.md)
and [fixed fixture oracle](../../../testdata/product-fixtures/README.md).
Use exact D19 reader/schema/key references and copied valid/refusal fixtures.
Inspect application.Open, db.ConnectDB/EnsureSchema, ParseStorageContext,
OpenLocalStorage, the repository lease and all reachability queries. Existing
constructors and successful restore are not passive-compatibility proof.

## First-unit output and completion criteria

1. Define explicit inspect/open-existing inputs and ownership lifetime, distinct
   from create, migration and corrective owned-runtime modes. Resolve one
   immutable catalog/backend, payload/control-root, key and profile tuple.
2. Acquire the established legacy lease before applicable repository access;
   retain ownership through all opened resources, closing them before release.
   Preserve `.coldkeep-control/repository.lock`; no new persistent identity or
   catalog schema is introduced to make inspection work.
3. Produce a bounded capability/version/layout verdict and a quiesced full-catalog
   recovery-cut inventory: all referenced recipes, blocks/whole containers,
   required reader/key material and incompatible/missing members. Account for
   physical_file/snapshot_file roots and existing pins; do not infer full format
   compatibility from a successful restore alone.
4. Prove on disposable data that missing root/database, future schema/format,
   wrong root, missing key and inconsistent graph refuse without creation,
   migration, repair, replacement or fallback. Compare source schema, catalog
   meaning and payload/control inventories before/after. Preserve failures.
5. Prove lifetime/cancellation/partial-open cleanup and maintenance exclusion;
   no hidden second root or ambient backend may be used. Record limits and
   expected versus observed fixture outcomes without rewriting the oracle.

This first unit ends at passive opening and the recovery-cut inventory. It
**does not complete 2.03**. Package staging/publication, constrained import and
original-installation-loss restoration are subsequent bounded 2.03 work. All
must pass before any schema additions or adoption claims.

## Remaining integrated 2.03 contract (subsequent unit)

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

## Later 2.03 exit evidence required before schema additions

1. Carry forward the complete 2.01 reader/schema/failure-limit inventory and
   recorded valid-restore versus expected-refusal fixture outcomes.
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

As a separate bounded run after baseline fixtures are fixed, 2.02 can prove a
minimal packaged daemon with actual embedded UI, authenticated internal
history/restore journey, explicit
frontend-then-Go clean-source build and Linux x86-64 CGO/SQLite native feasibility.
Record linked runtime and failures; no support claim follows from compilation.
PostgreSQL Docker remains the first product path. This packaging work does not
wait for Windows/macOS resources and does not authorize unrelated upgrades.
