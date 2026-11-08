# Phase 1B passive inventory contract

`internal/recoveryexport.Inspect(ctx, config, limits, consume)` establishes one
owned cut and calls a synchronous consumer with a diagnostic inventory. It
returns `Complete: true` only after consumption and successful resource closure.
No live handles or producer goroutines are returned. Retaining inventory JSON
never retains a live cut: a later exporter must acquire and revalidate anew.
There is no CLI export command, copied package, completion marker or import.

## Explicit input and lifetime

Supply backend `postgres`, profile `manual-postgresql-docker`, one existing
payload root, explicit PostgreSQL host/port/database/user, transport
`local-plaintext`, a scoped password resolver, and optional key resolver plus
nonsecret key reference. Resolvers run once before adapter construction; key
bytes are copied privately and cleared on exit. No DSN/password/key bytes enter
reports. No global environment is changed. Libpq `PG*` / `PQGO_DEBUG` ambient
settings are refused during resolution; later environment changes cannot select
another connection, key or root. There is no database/backend/root fallback.
The bounded transport is loopback TCP or a local Unix socket with explicit
plaintext transport, tested on loopback Docker PostgreSQL 16. No remote/TLS,
other PostgreSQL major or full product support qualification is claimed here.

The canonical root is the existing legacy coordination identity. Acquire the
same process registry and `.coldkeep-control/repository.lock` native exclusive
lock, opening the existing artifact without creation. Missing root/control/lock
refuses; unsafe control/lock/owner paths refuse. Existing owner metadata bytes
are untouched. The native lock artifact persists after release. Namespace
substitution is checked before success and release. This is cooperative local
exclusion from participating writers/maintenance, not arbitrary external SQL,
filesystem modification, distributed fencing, or proof that a future daemon
has drained its own internal workers.

Open the PostgreSQL connection only after acquiring ownership. Use a single
`REPEATABLE READ READ ONLY` transaction; all schema and catalog queries use it.
The transaction and lease remain held throughout consumption. Every cursor,
container and root handle closes; the transaction rolls back; the database
closes; then the lease releases. Failure/cancellation/consumer error follows the
same ordering. Consumers must cooperate with cancellation and return only after
their own synchronous consumption ends. PostgreSQL snapshot semantics are from
[SET TRANSACTION](https://www.postgresql.org/docs/16/sql-set-transaction.html).

The passive path does not call ConnectDB/EnsureSchema, runtime constructors,
recovery/repair, writers, or restore pin/unpin. Payload handles are read-only,
opened beneath `os.Root`; container validation uses the existing historical
header validator on the opened handle. A configuration pointing at a byte-
identical copied legacy root cannot be distinguished from the original by a
persistent installation/key identity that v1 never stored. Only the supplied
configuration, observed catalog, canonical root and checked objects are bound.

## Closure and verification

Enumerate all schema-17 tables, including physical files, snapshots/path/member
rows, logical recipes, chunks/pins, blocks, packed refs, repository config,
repair attempts/containers/blocks/chunks and retired packed/legacy extents.
Unknown table/column/type layouts and non-singleton/future schemas refuse.
Validate roots, references, recipe order and size, counters, lifecycle state,
format/codec requirements and packed encoded membership. Snapshot-only and
zero-current logical files remain included. Pin-only chunks and unreferenced
catalog containers remain included. Empty files create no physical objects.

Copy units in the diagnostic are **whole catalog-listed containers**, with IDs,
filenames, size, greatest required extent, header major and whole-object SHA-256.
The inventory includes every catalog relationship. A packed chunk's legacy
`blocks` companion row is compatibility metadata: compressed companions are
not additional direct physical extents. Require its referenced container to
agree, preserve the row, and validate the actual packed extent. Direct legacy
and retired legacy extents are checked independently. Terminal published repair
journals and retired entries contribute their own complete physical closure.

Checks cover container headers/CRC/sizes, required extents, whole-object digests,
packed physical/compressed/logical hashes where present, authenticated AES-GCM,
bounded zstd decode, actual CKBL headers/layout/membership, and active chunk
hashes. Legacy direct chunks are decrypted and checked against chunk hashes;
retired legacy extents have size/codec/authentication checks and the containing
object digest, with no invented historical chunk digest. No reconstructed-file
hash verification occurs. The old restore-future-block-metadata oracle stays
unchanged; new admission rejects that catalog requirement independently of
valid encoded v1 bytes, and also rejects actual future encoded headers.

Unreferenced complete logical recipes/containers are included. Top-level
uncataloged entries are listed as excluded observations, without traversal,
repair or pruning; unreferenced container bytes are included with whole objects.
PROCESSING/ABORTED logical/chunk rows, sealing/quarantined containers, non-
PUBLISHED repair state, missing/dangling members or unexplained packed entries
refuse as inconsistent/incomplete. This conservative unit does not rehabilitate
interrupted state. Missing keys, unsupported requirements, corruption and
capacity failures remain distinct. It never advises replacing a missing key.

## Bounds and output

Defaults are hard ceilings; callers can lower them. Maximum: 20,000 catalog
rows total; 16 MiB encoded catalog metadata; 4,096 bytes per text/bytea field;
512 schema columns; 8 MiB stored or decoded block; 256 MiB cumulative decoded
payload work; 1 GiB per object; 4 GiB total objects; 20,000 directory entries;
32 MiB serialized diagnostic; 2 minutes context deadline. There is one failure
rather than an unbounded error list. Queries use a remaining-row SQL LIMIT plus
one overflow sentinel, and server-side field guards before JSON serialization.
No full-table client load followed by pagination. Maps/sorts are bounded by the
shared row/metadata budget; one block is decoded at a time, whole-object hashing
uses 64 KiB reads, zstd has one decoder worker and bounded output/window memory.
Queries additionally have 10-second statement and 1-second lock timeouts.

`Inventory.WriteDiagnostic(writer)` streams one bounded row at a time. Its
caller owns an explicit destination **outside the inspected source**, using a
disposable report location for this unit. It emits no package completion marker.
Partial diagnostics, limit failures and cancelled operations are incomplete.
No source sidecar or spool is created. Filesystem access times and PostgreSQL
internal telemetry are outside the no-Coldkeep-source-mutation claim.

SQLite's existing fixed fixtures remain exercised; no passive SQLite adapter,
`.db` copying or immutable/WAL shortcut is introduced. Supported SQLite backup
handling is still a decision in the remaining action 2.03 work.
