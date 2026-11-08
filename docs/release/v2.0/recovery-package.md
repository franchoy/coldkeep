# Recovery package v1 and operator contract — Phase 1C

This internal Linux API proves a byte-complete generation for the selected
local PostgreSQL 16 / manual-postgresql-docker profile. It does not implement
product import, reconstruct files, prove key/reader custody, authenticate SQL,
or establish independent recoverability. Parent 2.03 remains ACTIVE.

## Operator sequence

1. Stop legacy writers and maintenance. Retain the usable encryption key and
   compatible Coldkeep reader separately; record their custody externally.
   Never repair a missing lock while an owner could still be running.
2. For an **existing** unprepared payload root only, call
   `coordination.PrepareExisting(ctx, payloadRoot)`. This explicitly creates
   missing `.coldkeep-control`/`repository.lock`, using the existing process
   registry/native lock domain. It refuses missing roots and unsafe artifacts,
   preserves an existing lock inode/bytes and owner metadata, and does not open
   the catalog or change payloads. Repeated preparation is safe; contention
   refuses. Preparation is its own mutation, before passive fingerprints.
3. Supply `recoveryexport.Config`: explicit local host/socket, port, database,
   user, transport, payload root, password/key resolvers and key reference.
   It retains the accepted [passive contract](passive-inventory.md). The role
   needs full visibility/SELECT on all catalog tables **and sequences**. Runtime
   never grants privileges. Export further restricts user/database to ASCII
   `[A-Za-z_][A-Za-z0-9_]{0,62}`; DSN/URI interpretations are refused.
4. Call `recoveryexport.Export(ctx, config, ExportOptions{...})` with
   `DefaultLimits()`, `DefaultExportLimits()`, absolute explicitly qualified
   `Tools{PGDump, PGRestore}` paths, and a fresh absolute destination directory
   name whose parent already exists. No general startup or preparation occurs.
   Source and destination must be canonically disjoint; sibling directories are
   permitted. Existing destination material is never replaced. Paths through
   parent symlinks are resolved before directory anchoring; aliases into the
   source refuse. Select genuinely separate storage when that is needed.
5. On success, call `Check(ctx, destination, DefaultExportLimits())` to reconcile
   the independent generation. `ByteComplete` covers exact declared package
   integrity only. It opens no source, obtains no key and executes no SQL.
   A later importer must admit restored catalog semantics against members before
   activation. Keep the entire completed directory together and immutable.
6. On any error, inspect `ExportResult.Published`. If false, leave the generation
   incomplete, or explicitly remove only the known operation-owned directory
   after its operation has returned and closed all resources. Never reuse it.
   Restart with a new destination and fresh cut. If Published is true, preserve
   the directory and run Check: publication may have succeeded despite a lost
   acknowledgement or later sync/close failure. Do not delete a valid generation
   to manufacture a clean retry. After process interruption, Check likewise
   reconciles the final marker; absence or failure means incomplete.

No standalone product CLI is added. Internal callers use the APIs above; the
reproducible acceptance adapter is `scripts/run_recovery_export_tests.sh`.
Point `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` exclusively at a disposable
PostgreSQL 16 server with database/role creation privileges for fixture setup.
Set `COLDKEEP_PHASE1B_TEST=1`, `COLDKEEP_PHASE1C_TEST=1`,
`COLDKEEP_PG_DUMP=/absolute/16/bin/pg_dump` and the matched `COLDKEEP_PG_RESTORE`.
Put installed Go 1.26.10 on PATH and pass one **absent** evidence directory.
The runner performs setup, production export/check, test-only metadata restore
and cleanup. It exits nonzero for Go failure, skip or missing required event.
It never targets a user catalog. Credentials are environment inputs, not command
arguments or evidence. Do not save runtime database/password/key artifacts.

## Independent version domains and flat layout

Application release **2.0.0-dev.0**, catalog schema **17**, accepted payload
encoding majors **0/1**, and package version **1** are separate domains. This
unit changes no schema, migration, dependency or stored payload encoding.

| Member | Meaning |
| --- | --- |
| `catalog.dump` | Full single-process PostgreSQL custom archive, supported header 1.15.0. All 19 tables, ordinary data, sequence state and required schema objects. |
| `object-000001.bin` … | Independent regular-file whole-object bytes in inventory order, including retired extents/unused regions. Original catalog filenames are manifest data, never destination paths. |
| `manifest.json` | Strict versioned metadata, exact members/lengths/SHA-256, container IDs/original names/payload majors, full-catalog counts, reader/key requirements and exclusions. |
| `COMPLETE` | Atomic final publication. Version, generation and SHA-256 of the exact manifest bytes. |

The random 128-bit generation is package-local. Snapshot ID and canonical-root
SHA-256 are observations of a pre-identity legacy source, not persistent
repository/key identities or reusable authority. Each export opens a fresh cut;
an old inventory/manifest/snapshot cannot authorize later copying. Key references
are labels; no raw keys/passwords are packaged. Key custody, reader custody and
destination independence are **unknown** unless independently demonstrated.
The manifest carries the compatible reader requirement. Operators retain that
reader/toolchain and separately usable keys; same-disk copies do not establish
a separate failure domain. Uncataloged source files are explicitly excluded.

## Snapshot, tools and restoration prerequisites

One owned read-only repeatable-read transaction validates catalog/graph/whole
objects and exports `pg_export_snapshot()`. `pg_dump -Fc --snapshot=...` imports
that snapshot while its owner stays live through dumping and copying. All
catalog-listed containers are included, independent of current-file reachability.
The lease excludes participating writers/maintenance, which also stabilizes
non-MVCC sequence state. Export never calls source nextval/setval. The snapshot
regression's deliberate external commit changes ordinary table data only.

Qualified clients must both report the same PostgreSQL **16.x** version; server
must be PostgreSQL 16. Client paths and actual version/server/image identities
belong in execution receipts; actual versions and encoding/locale also appear in
the manifest. Missing/incompatible tools refuse without fallback. The database
must use libc locale and default tablespace. Extra schemas, extensions beyond
plpgsql, row policies/RLS, custom tablespaces, foreign servers, large objects,
event triggers, publications/subscriptions/security labels, and public SECURITY
DEFINER or non-SQL/plpgsql functions refuse. The existing catalog validator also
refuses unknown table/column requirements. These are deliberate bounded profile
restrictions, not silently discarded dependencies. Backups remain **trusted
operator executable material**; checksums do not make untrusted SQL safe.

Subscription refusal applies only to the selected database, including disabled
subscriptions. It reads their publicly visible database identity without reading
connection strings or requiring additional privileges. Subscriptions belonging
to another database in the same PostgreSQL cluster do not block this export.

Children receive structured arguments and an explicit environment, with no
inherited PG/service/passfile/options target selection. Passwords go in an
operation-owned mode-0600 escaped passfile inside a private mode-0700 temporary
directory outside both trees, removed after the child joins. The existing
temporary parent is canonicalized and checked before any creation. A parent
equal to or inside either protected tree refuses; a common ancestor with
separate existing source and generation directories remains supported.
Creation, passfile writing and removal use the validated directory anchor.
Host/port/user/name are explicit. Read-only and row_security=off are enforced. No retries select a
new cut. stdout/archive, stderr and archive-inspection output are bounded;
nonzero exit, malformed archive or I/O errors prevent publication.

The metadata round-trip test creates a fresh database separately, then loads it
as a **non-superuser** with matched pg_restore, `--no-owner --no-acl
--exit-on-error`. Ownership/ACL remapping is a **restore** policy; a custom dump
retains source ownership metadata. No `--create`, `--clean`, trigger disabling,
schema initialization or migration is used to repair the archive. This test
compares every table, sequence value/definition, constraints, indexes, functions
and triggers, then exercises next-ID/FK behavior in the destination only. It is
not the constrained product importer or an installation-loss restore proof.

## Lifetimes and publication

Source readers, pg_dump and os/exec pipe-copy workers finish before cut cleanup.
Cancellation keeps database/sql automatic rollback active; actual rollback,
owned physical-driver close and tracked socket termination join before lease
release, including errors. No marker is eligible inside the cut callback.
A late source/namespace/lease/cleanup failure leaves the generation incomplete.

Members are exclusive mode-0600 files in a fresh mode-0700 directory. Copies
are streamed, compared with this cut, closed/synced and reread from destination.
After successful source cleanup and lease release, the staged package is checked
again. A synced `complete.pending` is atomically renamed to `COMPLETE` using
Linux `RENAME_NOREPLACE`; this is the sole publication point. Cancellation
observed beforehand refuses. A later acknowledgement/sync/close error can leave
a valid completed generation and is reported as Published=true/Complete=false.
Directory syncs support the selected local filesystem path; tests of process
interruption do not qualify arbitrary hardware power-loss durability.

## Hard bounds and refusal behavior

Callers may lower ceilings, not raise them. Default export/check limits:

| Resource | Ceiling |
| --- | --- |
| Archive / manifest | 64 MiB / 8 MiB |
| Total reserved output | 4 GiB + 72 MiB + 4 KiB (marker reserve included) |
| Archive plus payload members | 20,001; flat generated namespace |
| Destination path / JSON string / JSON key | 4,096 / 4,096 / 128 bytes |
| JSON nesting / keys per object | 12 / 64; duplicates and unknown fields refuse |
| Child stderr / tool-version stdout | 64 KiB / 4 KiB |
| Archive inspection SQL output | 512 MiB, discarded without execution |
| Copy buffer / parallel copies / active archive child | 64 KiB / 1 / 1 |
| Operation duration | 2 minutes; cancellation joins actual cleanup |

Manifest member and excluded-name counts are checked during token traversal,
before typed slice allocation. The caller's member cap and the independent
20,000-name exclusion cap remain separate. Traversal observes cancellation;
case-folded aliases cannot bypass count limits or name the same field twice.

Retained inventory limits include 20,000 rows/directory entries, 16 MiB catalog,
8 MiB decoded block, 1 GiB whole object, 4 GiB total objects and two minutes.
Integer subtraction bounds totals before writes. Capacity reserves payload
bytes plus bounded archive/manifest/marker space; ENOSPC/short writes still
refuse if free space changes. No allocation of maximal fixtures is necessary to
exercise smaller configured limits. Regular local filesystem operations remain
subject to the host kernel/filesystem: context checks do not preempt a wedged
kernel I/O syscall. No hard real-time or hostile-writer fencing claim is made.

Both source-copy handles and destination members must be regular, non-symlink,
single-link files; hardlinked legacy sources are deliberately unsupported.
Checker requires the exact member set, validates names/counts/lengths/digests,
rejects aliased/nonregular/duplicate/traversal/extra material and strictly binds
marker/manifest/generation. It checks archive framing/integrity, not SQL
semantics. Package-local checksums provide integrity, not authenticity. No
cross-run resume, automatic cleanup of arbitrary paths or live pins are used.
