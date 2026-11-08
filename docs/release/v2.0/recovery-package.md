# Recovery package v1 and operator contract — Phase 1D

The Linux/manual PostgreSQL 16 profile supports byte-complete export, constrained
fresh-target import and the existing schema-17 snapshot reader. Package integrity,
semantic import readiness, demonstrated file recovery, key/reader custody and
physical failure-domain confirmation are separate facts. Product gates remain
INCOMPLETE. See the [current charter](README.md) for conditional local 2.03 authority.

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
   Import admits restored catalog semantics against these members before
   publishing its separate ready receipt. Keep the entire completed directory together and immutable.
6. On any error, inspect `ExportResult.Published`. If false, leave the generation
   incomplete, or explicitly remove only the known operation-owned directory
   after its operation has returned and closed all resources. Never reuse it.
   Restart with a new destination and fresh cut. If Published is true, preserve
   the directory and run Check: publication may have succeeded despite a lost
   acknowledgement or later sync/close failure. Do not delete a valid generation
   to manufacture a clean retry. After process interruption, Check likewise
   reconciles the final marker; absence or failure means incomplete.

The export APIs above remain internal. The narrow Linux `coldkeep-recovery`
operator adapter below exposes check/import/restore. The acceptance runner is `scripts/run_recovery_export_tests.sh`.
Point `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` exclusively at a disposable
PostgreSQL 16 server with database/role creation privileges for fixture setup.
Set `COLDKEEP_PHASE1B_TEST=1`, `COLDKEEP_PHASE1C_TEST=1`,
`COLDKEEP_PG_DUMP=/absolute/16/bin/pg_dump` and the matched `COLDKEEP_PG_RESTORE`.
Put installed Go 1.26.10 on PATH and pass one **absent** evidence directory.
Also set an explicit `COLDKEEP_RECOVERY_IMAGE` PostgreSQL 16 image digest already
available to Docker. The runner builds a separately staged static reader and
performs production export/import, isolated file restoration, live-GC recovery
and owned fixture cleanup. Its six-minute test ceiling covers the added rehearsals. It exits nonzero for Go failure, skip or missing required event.
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
and triggers, then exercises next-ID/FK behavior in the destination only. This retained test remains a metadata-only regression; Phase 1D adds separate
production-import and isolated-file-restoration tests.

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

Readiness receipts have a separate shared **32 MiB + 64 KiB** byte ceiling and
**20,000 objects**. The unchanged inventory diagnostic ceiling already contains
the complete JSON object array within 32 MiB; 64 KiB covers the other fields.
At most two 4,096-byte strings (target root and host) expand sixfold under JSON
escaping; database/role are at most 63 bytes, key reference 128, and remaining
labels/digests/integers are fixed or bounded. The row/directory and 20,001-member
archive-plus-object limits independently bound the array. The producer validates
this exact reader contract before staging readiness. The anchored regular-file
reader checks actual bytes through EOF, then traverses tokens before typed object
allocation, enforcing alias-aware counts, duplicate/unknown-field, depth (12),
string (4,096), key (128) and trailing-input refusal. Operator config remains
separately limited to 1 MiB. No supported package capacity or format changes.

The caller-lowered inventory duration starts at the shared owned inspection entry,
including import's destination admission. Its nested deadline never extends an
earlier parent/global deadline. It limits work permission; actual rollback,
physical-driver/socket and child/credential cleanup must still join while outer
reservation/root ownership is held. A deadline is not a hard wall-clock promise
that cleanup or a wedged kernel syscall has completed.

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


## Constrained import and actual recovery

Build the narrow adapter in the reviewed checkout with pinned Go 1.26.10:

```bash
GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=0 go build -o /separate/tools/coldkeep-recovery ./cmd/coldkeep-recovery
```

Stage the executable and PostgreSQL 16 tools independently, recording executable
SHA-256 and tool patch versions. Static Linux adapter compilation is a rehearsal
convenience, not action 2.02 distribution qualification. Keep keys in separate
private custody: this reader supports **32-byte AES-256 keys**, encoded as exactly
64 hex characters in a mode-0600 file (no newline). Passive inspection's other
AES lengths are not admitted by this importer. Missing/wrong keys, mismatched
aliases and unsupported reader requirements refuse before readiness.

Provision a dedicated database separately as administrator, using `template0`,
matching the manifest's encoding, libc collation/ctype and default tablespace.
Create an ordinary login owner without superuser, replication, bypass-RLS,
createdb/createrole or any role memberships. No role/database settings or existing
user objects are allowed. Stop all other clients. Reservations exclude cooperating
imports, including same database/different roots; they do not fence an external
administrator. The importer never creates or drops the database, grants access,
initializes a schema, migrates, repairs, overwrites or resumes a target.

The destination JSON uses only explicit destination fields. It is not source Config:

```json
{
  "host": "127.0.0.1",
  "port": 5432,
  "database": "recovery_target",
  "user": "recovery_owner",
  "payload_root": "/recovery/new-payload",
  "password_file": "/separate/custody/password",
  "key_file": "/separate/custody/key.hex",
  "key_reference": "the-manifest-key-alias",
  "pg_dump": "/usr/lib/postgresql/16/bin/pg_dump",
  "pg_restore": "/usr/lib/postgresql/16/bin/pg_restore"
}
```

For plaintext, use empty key file/reference strings. Password files contain the
exact password with no newline and mode 0600. Keep credential parents outside
package/payload trees. Target/output parents must exist, be canonical and be
operator-controlled; target and output leaves must be absent. The importer owns
only its newly created root. Do not stage credentials or working files inside
the immutable generation. Do not supply ambient PG/libpq settings or COLDKEEP_KEY.

Verify `check` output and obtain generation/manifest digest through the trusted
operator channel. Checksums are not authentication. Explicitly consent to the
**executable trusted backup** associated with those identities:

```bash
env -i PATH=/usr/bin:/bin /separate/tools/coldkeep-recovery check -package /recovery/generation
env -i PATH=/usr/bin:/bin /separate/tools/coldkeep-recovery import \
  -config /separate/custody/destination.json -package /recovery/generation \
  -generation GENERATION_HEX -manifest-sha256 MANIFEST_SHA256 \
  -trusted-operator-backup -exclusive-target -database-capacity-bytes 805306368
```

The library equivalent is `recoveryexport.Import(ctx, Destination{...},
ImportOptions{...})`. This distinct destination type grants writable restore
intent without weakening passive Config. The ordinary SQL restore runs exactly
one `pg_restore --no-owner --no-acl --exit-on-error --single-transaction` child,
with explicit target scalars and a minimal writable environment. No clean/create,
parallel jobs, superuser option, disabled triggers or row filtering is used.

Import verifies the completed package before SQL, retains a directory anchor,
copies and hashes each consumed regular single-link member, checks private archive
bytes again, and streams/discards at most 512 MiB expanded SQL before restoring.
This inspection is format/size validation, never SQL sanitization. Existing
manifest/count/object/total limits remain unchanged; known copy bytes plus 16 MiB
reserve must fit the target filesystem. The operator supplies a dedicated database
capacity budget of at least 64 MiB; it does not predict heap/index/temp/WAL demand
from compressed size. The rehearsal uses a finite 768 MiB PostgreSQL tmpfs and
512 MiB reader-container memory. Actual exhausted-storage connection refusal and
copy/close/fsync/child failures leave no ready handoff. These observed limits are
not a general hard SQL memory/WAL sandbox.

Catalog filenames are validated before copying to the final payload namespace.
The full passive validation core runs under continuous destination ownership,
without reacquiring its lease. It checks schema, counts, graph, exact whole-object
IDs/names/lengths/digests/formats and derived key requirements, including snapshot,
pin-only, repair and retired references. It does not reuse the old server snapshot
ID or invent persistent repository/key IDs. Original root observation remains
provenance; the new root and database OID are separate local observations.

All active data sessions, rollback/driver/socket joins, children, credentials,
copy durability and semantic admission finish before atomic publication of
`.coldkeep-control/IMPORT_READY`. The database reservation and root lease remain
held through publication, then close before successful return. The receipt binds
the generation/digest, observed target, admitted mapping, profile and reader/key
requirements. It is a logical handoff, **not** an atomic PostgreSQL/filesystem
transaction or user-repository adoption. Immutable package custody/independence
fields remain unknown.

On failure, `Published=false` means no usable handoff, even if metadata committed
in the private target. Preserve diagnostics; retry with a new database/root.
`Published=true, Ready=false` means published-or-uncertain: preserve the target,
check its receipt/mapping/catalog and reconcile under exclusive ownership before
using it. Never delete a possibly usable target automatically. A caller may remove
only the exact operation-created root after resources close; database destruction
requires its own operator ownership/authorization. Do not delete package input.

Restore the selected snapshot into a fresh output, using the imported explicit
DB/root and separately staged key/reader:

```bash
env -i PATH=/usr/bin:/bin timeout --kill-after=5s 120s \
  /separate/tools/coldkeep-recovery restore \
  -config /separate/custody/destination.json \
  -snapshot SELECTED_SNAPSHOT_ID -output /recovery/new-output
```

The adapter browses actual snapshot entries and calls the existing snapshot/storage
reader with `StorageContext{DB, ContainerDir}` and no writer/startup recovery.
The role is writable because restore pins/unpins chunks. Success reports balanced
pins, selected IDs/paths and final-file reread SHA-256/mode/mtime. Reader cancellation
is checked between files by the unchanged snapshot wrapper; `timeout` bounds the
owned child during a file. Abrupt termination can leave pins and requires operator
reconciliation of that disposable target; it is not successful balanced cleanup.

The rehearsal mounts only package (read-only), independently staged custody/tools
(read-only) and fresh destination/output. Its network namespace contains a separate
PostgreSQL server. Negative checks show original catalog, root, checkout and key
source unavailable. It restores 3 plaintext and 8 mixed/encrypted historical files,
including older content, zero-current snapshot history, empty file, distinct case/
Unicode names, packed/zstd and legacy data, with mode/mtime checked separately.
A second source fixture removes current roots normally, executes snapshot deletion
and real live GC, then freshly imports the same checksum-identical generation into
a new isolated destination and restores 8 files. Pin/repair/retired closure is a
separate four-object test and is not made artificially reclaimable by GC.

The fixed 23-case product oracle and both known limits remain unchanged: literal
Unix backslash normalization and the by-ID reader's future catalog block-metadata
gap. Import's full semantic admission refuses the latter before readiness. Same-host
container isolation demonstrates recovery behavior; physical disk/host-loss
independence remains **unknown** until separately confirmed by the operator.

PostgreSQL's [pg_restore trust and transaction contract](https://www.postgresql.org/docs/16/app-pgrestore.html)
and [template0 provisioning](https://www.postgresql.org/docs/16/sql-createdatabase.html)
remain the governing upstream boundaries.

## Phase 3A profile extension

[Identity adoption](identity-adoption.md) adds package-v2/schema18/receipt-v2 with
all six identity/provenance tables, exact constraints and unbound lineage. The
existing v1/schema17/receipt-v1 contract above remains unchanged. The new operator
validates either exact tuple before restore pins; mixed profiles refuse. Existing
v1 ready receipts remain readable on unchanged targets, and any retained ready
receipt blocks adoption. Phase 3B physical binding is still required for ordinary
schema18 mutations.
