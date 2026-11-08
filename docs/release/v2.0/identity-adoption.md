# Phase 3A identity and recovery contract

This is the bounded Linux/PostgreSQL16 `manual-postgresql-docker` development
profile. Parent 2.04 remains ACTIVE. Ordinary schema17 behavior and the SQLite
proof remain supported; SQLite identity adoption is refused. Phase 3B physical
binding and Phase 3C capture membership are not implemented.

## Identity and provenance

`repo_`, `root_`, `device_`, `source_` and `adopt_` followed by exactly 32 lowercase
hexadecimal digits encode type plus 128 CSPRNG bits. PostgreSQL checks canonical
encoding, uniqueness, singleton identity cardinality and foreign keys. IDs are
names, never credentials, lock tokens or evidence of physical possession.

Six additive tables are specified in `db/adoption_postgres.sql`. Repository and
root identify the existing logical history and single logical payload root.
The device records the adopter, explicitly not the historical capturer. The
source is a disabled, unbound, synthetic unknown legacy scope; it does not become
a live tracked source. Every existing snapshot gets an explicit legacy mapping.
No capture generation, key-custody identity, job service or source path binding
is manufactured. Snapshot IDs, file history and payload recipes remain unchanged.

The completed operation binds the exact request/plan, resulting IDs, 17-to-18
transition and fresh pre-adoption recovery generation/manifest digest. All
records and authoritative catalog_version change in one transaction. No automatic
downgrade exists. Rollback restores the pre17 generation into a fresh owned target;
that generation cannot contain identities which did not exist when it was made.

## Operator flow

Use `coldkeep-recovery adopt-preview -config /private/source.json` with the same
explicit credential/key/tool configuration as the recovery operator. The source
must already have a prepared canonical payload root and exact supported schema17.
Preview performs bounded full-catalog/payload inspection without adoption. Retain
its `plan_sha256`; it binds catalog rows, payload identities and source selection.

Generate an `adopt_` request with 16 random bytes, for example Python's
`secrets.token_hex(16)`. Apply explicitly:

```text
coldkeep-recovery adopt-apply -config /private/source.json \
  -expected-schema 17 -operation-id adopt_<32-random-lowercase-hex-digits> \
  -plan-sha256 <reviewed-preview-digest> -checkpoint /independent/new-generation
```

The operator must possess required keys and exact PostgreSQL16 clients. The
checkpoint destination must be fresh, separate and capacity bounded. Credentials
and keys remain outside exported package bytes. No ambient database target is
accepted. Repeating the same committed request/plan returns the same lineage;
a conflicting request refuses. Before-commit failure leaves schema17; an uncertain
commit response must be reconciled using the same request, not assumed rolled back.
Keep the generation after every later error. This is not installation switching.

One existing canonical-root lease spans inspection, owned export, actual SQL/
driver/child/credential cleanup, independent completion checking, writable
migration, post-validation and final cleanup. A database-scoped reservation also
serializes adopters using distinct root paths. Existing standalone Export still
publishes only after its own cleanup and lease release. A transient PostgreSQL
snapshot ID is not the checkpoint. Arbitrary external SQL writers or cross-host
owners remain outside the cooperative local profile.

Any retained `.coldkeep-control/IMPORT_READY` refuses adoption before mutation.
In particular, merely replacing a v1 receipt would not revoke authority already
read by an old operator. This implementation does not attempt that revocation.
The receipt restriction is deliberate; it is not permission to delete a receipt
and adopt while a reader may still be active.

## Compatibility

| Catalog | Ordinary mutation | Recovery package | Exact reader capability | Ready receipt |
| --- | --- | --- | --- | --- |
| 17 PostgreSQL | Existing behavior | v1, 19 tables | Existing schema17 reader string | v1, no repository ID |
| 17 SQLite | Existing qualified bounds | PostgreSQL export is unsupported | Existing SQLite proof | Not an adoption route |
| 18 PostgreSQL | Refused: identity binding required | v2, 25 tables | schema18 identity-lineage-v1/unbound | v2, repository ID |
| Other/malformed | Refused | Refused | Refused | Refused |

The development application version remains 2.0.0-dev.0 and does not establish
capability. The preserved D19 and accepted pre3A production writers reject 18 at
schema admission even with auto-bootstrap. New ordinary PostgreSQL connection,
startup recovery and engine construction also reject 18 before mutation. The
fixture daemon retains its explicit schema17 boundary. Low-level SQL and payload
primitives remain caller-owned internals, not independent admission APIs.

Schema18 inspection/import requires all 25 named tables, supported named column
types, all identity NOT NULL/PK/UNIQUE/FK/CHECK definitions, exact capability and
a complete valid lineage graph. Column physical order is immaterial. Existing
row, complete-byte, aggregate, receipt, object and deadline ceilings are unchanged;
identity rows and snapshot mappings consume the same catalog budget. Unsupported
or falsely labeled profiles cannot publish readiness. A checked file listing
alone never proves SQL semantics.

The new recovery operator accepts old schema17/v1 receipts on their unchanged
valid target. It validates the current full catalog/payload and receipt tuple
under ownership using joined passive sessions before temporary restore pins.
Schema18 imports preserve logical IDs and migration lineage while all bindings
remain unbound. General capture, remove, configuration changes, GC and repair
remain unavailable. Isolated recovery tests must independently reread fixed
plaintext/encrypted historical content and metadata and compare lineage exactly.
A copied ID and a successful restore do not prove current volume identity or
physical failure-domain independence.

## Qualification boundary

The blocking recovery runner retains all 244 accepted named requirements and
adds Phase 3A requirements, builds byte-verified untouched D19/accepted writers
and the accepted recovery operator, and refuses missing/skipped events or a
nonzero Go result. Required cases cover actual adoption/operator use, conflicting
and repeated requests, distinct-root process contention, rollback and physical
close barriers, child and credential joins, profile/constraint/bounds refusals,
old receipt continuity and isolated pre17/post18 historical recovery. Deterministic
transaction faults do not claim power-loss durability.

Technical references: [PostgreSQL16 snapshot lifetime](https://www.postgresql.org/docs/16/functions-admin.html),
[transaction rollback](https://www.postgresql.org/docs/16/sql-rollback.html) and
[constraint definitions](https://www.postgresql.org/docs/16/sql-altertable.html).
