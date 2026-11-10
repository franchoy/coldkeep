# Phase 3C1 generation and recovery contract

The owner adopted the Phase 3C1 BUILD on 10 November 2026. This contract fixes
the implementation choices before DDL. It is not a qualification receipt.
Parent 2.04 and implementation remain ACTIVE; product gates INCOMPLETE;
Phase 3C2 (measured indexed reads, pagination and integrated closure) OUTSTANDING.
Requested model/effort: GPT-6 Astra / Extra High. Actual model variant and effort
are unknown to the implementation agent; neither is inferred from that request.

The selected Linux/PostgreSQL16 maintenance profile adds exact schema20,
package4, IMPORT_READY v4 and a distinct generation-model-v1 recovery reader.
Profiles 17/18/19 and SQLite17 retain their prior contracts. Ordinary mutation,
GC, repair, capture, publication and imported-target activation remain fenced.

## Records and integrity

`db/generation_postgres.sql` is the exact additive DDL. `schema_upgrade_operation`
records the one 19-to-20 transition with an `upgrade_` 128-bit identity, request
and plan digests, repository/root, unchanged binding revision, and verified
pre19 recovery generation/manifest. It does not create a binding revision.

`generation_policy` has a `policy_` 128-bit revision ID, repository/configured
source, canonical descriptor bytes and SHA-256. `capture_generation` has a
`generation_` 128-bit ID, repository, configured source, immutable source-binding
revision, same-source policy and pinned descriptor digest, nullable start/end nanoseconds, scan state
(`unknown`, `pending`, `partial`, `enumerated`), outcome (`unknown`, `incomplete`,
`failed`) and a necessarily absent publication receipt. Fixture enumeration
is not a successful capture. No production insertion command is provided.

`generation_root` holds optional mode/mtime for the source root independently
of relative members. `generation_entry` identifies a member by generation and
`entry_` ID, nullable parent, raw BYTEA component, kind (`file`, `directory`),
outcome (`stored`, `excluded`, `missing`, `unreadable`, `unstable`, `unsupported`),
optional logical-file reference and mode/mtime. Only a stored file has a logical
file; its completed recipe must validate. A stored directory has no recipe.
Directory metadata never uses an empty file surrogate. Unknown metadata is NULL.

Exact NULLS NOT DISTINCT sibling uniqueness covers root-level names. A composite
self FK requires a directory parent in the same generation. Full admission
rejects cycles, paths over 3072 raw bytes including separators and depth over
128. Each component is 1–255 bytes, excluding NUL, slash, dot and dot-dot.
There is no full-path B-tree key. Component keys must be measured at their maximum
on the pinned server. Opaque identities and all FK constraints preserve source
ownership; the synthetic legacy source and snapshot mappings remain unchanged.

Policy revisions are insert-only through the private record contract: a repeated
ID cannot replace bytes or ownership. No ordinary write facade is enabled.
A composite FK pins each generation to its policy digest, so a canonical policy
rewrite plus a changed digest also refuses while referenced. The SQL digest
constraint and semantic admission validate canonical bytes;
privileged SQL is outside the cooperative owner contract. No historical meaning
is inferred from current source configuration. Existing physical-file ref_count
and current-chunk live_ref_count are unchanged by generation references.

## Exact wire grammar

A selector is exactly `{"kind":"posix-bytes-base64","components":[...]}`.
The array has 1–128 canonical padded standard-base64 strings, each decoding to
one admitted component. Re-encoding must be identical. Case, whitespace, Unicode
normalization, backslash/newline, literal U+FFFD and invalid native UTF-8 bytes
remain distinct. Display strings never select entries. Selector input is capped
at 8192 JSON bytes before parsing; unknown/duplicate fields, malformed UTF-8,
unpaired surrogate escapes and noncanonical base64 refuse.

A policy descriptor has exactly these fields in this canonical order:
`version` (integer 1), `intent` (`capture` or `archival-import`), `scope`
(`source-root` or `selected`), `selection_version` (integer 1), `rules` (ordered
array, at most 32). Each rule has exactly `action` (`include` or `exclude`) and
`selection` (the selector above). `source-root` allows zero rules; `selected`
requires at least one include rule. Rules have no executable expressions.
Canonical bytes are Go JSON encoding of this fixed typed structure, without
whitespace. Admission requires byte equality with that encoding, max 4096 bytes.
All selectors separately obey their component/path bounds. Input bytes and array
counts are checked before constructing typed policy data. Credentials and other
unknown fields refuse. Evaluation belongs to action 2.10.

## Upgrade and recovery

Explicit `upgrade-preview`, `upgrade-apply`, `upgrade-reconcile` require exact
schema19, repository/root/binding revision and an `upgrade_` operation ID.
Apply binds the reviewed digest and a fresh independently checked checkpoint.
Fresh Apply creates that package itself under owned Export; it does not accept
a caller-supplied checkpoint. Export dumps its owned database snapshot, and Check
independently verifies package member bytes and profile. The installed intent
pins generation and exact manifest SHA-256, including every member digest, to
the current catalog/prior plan. Retry verifies these pins; stale same-repository
package substitution with an intact intent refuses. Current catalog/plan are
rechecked before and within the DDL transaction. Check does not independently
restore and semantically reread catalog.dump. Rewriting both a package and its
owned intent does not confer supported authority; privileged local tampering
is outside this cooperative-owner protocol.
The existing installation/root/database ownership lifetime covers checkpoint
export, physical SQL/driver/child/credential cleanup, transaction, postvalidation
and intent cleanup. Marker and descriptor bytes are preserved unchanged.

A `coldkeep-schema-upgrade-intent/v1` record occupies the already reserved
`binding-intent.v1.json` slot and its `.pending` staging name. Untouched pre3C
tools must refuse conflicting mutation/readiness while either exists. Exact new
operator retry validates the same checkpoint/request and reconciles staged or
installed intent; foreign/malformed material is never overwritten. Unknown commit
outcomes require same-request reconciliation. Before-commit faults preserve19.
No in-place downgrade or installation switch is implemented.

Recovery inventories include all five new tables and generation-only recipes,
within the unchanged 20000-row/16-MiB aggregate bounds and 4096-byte variable
field guard. Encoded bytea/JSON expansion counts separately. Named columns and
exact relation-bound constraints use bounded server-side admission before
deparsing. RLS/filtered views and all future profiles refuse. Recovery exports
carry portable history, never physical authority. All IMPORT_READY files,
including malformed/future receipts, prevent upgrade and binding maintenance.

The result must separately qualify plaintext/encrypted round trips, isolated
historical restore and generation-only recipe recovery while reclamation stays
fenced. Phase 3C2 owns public indexed/keyset reads and integrated action closure;
this unit cannot clear action 2.04 or any product-release gate.

## Supplied-handle writer fence

Untouched D19 does not check the schema version when an engine receives a
caller-supplied database handle. Schema20 therefore installs a fixed SQL fence:
BEFORE STATEMENT INSERT/UPDATE/DELETE/TRUNCATE on every legacy, identity,
binding and upgrade table, plus UPDATE/DELETE/TRUNCATE on policy/generation
revision tables. It runs before row defaults, sequence allocation and payload
writes. All noninternal triggers are admitted by an exact inventory, including
the four retained timestamp triggers; both function bodies use bounded exact
admission. Missing, disabled, extra or changed triggers/functions refuse.

The recovery operator obtains a dedicated single-connection pool only after
owned full receipt/catalog/payload validation and database-OID confirmation.
It joins automatic rollback, physical driver close and cancellation sockets before releasing the lease. A session marker permits only
chunk pin_count +1/-1 with all other supplied row values unchanged. Connection
replacement is forbidden by the owned physical-session adapter. Ordinary constructors and old
writers never receive it. This is a cooperative compatibility fence against
untouched ordinary code, not a security boundary against a database owner or
hostile SQL. Generation-only recovery uses the same owned reader seam privately.
