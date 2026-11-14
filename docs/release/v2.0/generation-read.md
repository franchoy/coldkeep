# Bounded generation-model reads

`generation.Reader.Read` accepts five fixed version-1 requests: source-scoped
generations, generation detail, parent-scoped children, exact entry and selector
resolution. These are unpublished generation-model records with actual scan and
outcome values. Nullable publication remains absent. Directory, zero-byte file
and missing/excluded/unreadable/unstable/unsupported outcomes remain distinct.
No HTTP/UI, capture, product restore or ordinary writer is introduced.

`WithGenerationReadView` owns installed Linux/PostgreSQL16 schema20 admission,
the existing canonical-root lease, installation/control handles and physical
binding evidence. A separate non-data database-alias reservation is retained.
All methods use the same physical data connection and REPEATABLE READ READ ONLY
transaction. Opening performs the existing bounded full catalog/graph/payload
admission, including key checks and inventory serialization: O(total admitted
catalog/payload), within 20,000 rows/16 MiB and the existing two-minute ceiling.
Opening time and retained memory are measured separately from page work.
Disabled/unavailable input sources retain catalog history; current input paths
are not a prerequisite for browsing installed repository metadata.

Methods reject simultaneous use. Callback return closes admission, cancels and
joins admitted work before SQL cleanup and ownership release. Escaped readers
refuse without SQL. Descriptor/marker/control substitutions and pending intents
refuse before outer success. Rollback, cancellation transport and physical close
must join; cleanup errors leave the operation incomplete. The trusted callback
must cooperate with cancellation; a callback ignoring its context cannot be
forcibly stopped, so the duration ceiling is not a hard cleanup-return promise.

Generation order is opaque ID order with explicit `COLLATE "C"`, never capture
or commit order. Children use raw BYTEA components and the existing exact
generation/parent/component unique index. SQL includes the fixed parent in its
ordering so root/null-parent pages can use that index. Requests have no optional
filters or alternate sorts. Pages issue parameterized SQL, never slice Inventory
or use OFFSET. Auxiliary identity/watermark/selector data queries are measured too. Each
successful Read also makes two inherited ownership-reservation checks on the
separate non-data connection. Their time/allocation is included; saved EXPLAIN
plans cover data queries only. Global pg_locks work is not an indexed page-work
claim.
Server CASE guards cap variable fields before driver transfer.

Defaults are 50 returned members, hard maximum 100 and limit+1 sentinel at most
101. Optional request `response_limit` defaults to 1 MiB and may only lower
that ceiling. A response, including a single item, exceeding it returns explicit
`ErrResponseLimit` with no successful response. Cursor/request/response ceilings are 4 KiB/16 KiB/1 MiB. Existing raw-name
limits remain 255-byte component, 3,072-byte path and 128 components; policies
remain 4,096 bytes/32 rules. Canonical base64 preserves native POSIX bytes;
display text is not identity. Signed int64 timestamps/revisions/logical IDs use
canonical decimal JSON strings, including values above 2^53. Null stays distinct
from zero. Strict decoders reject invalid Unicode, duplicate/unknown or aliased
keys, malformed base64, traversal, noncanonical integers, oversized raw input,
array counts and trailing input. Real pinned Node JSON consumers qualify this.

Cursors bind version/query, view identity, complete scope, fixed order, captured
upper key and last unique key/entry identity. The owned snapshot validates the
upper bound and last identity. Returned cursors enumerate one stable set despite
late commits below/above the bound. Cursors expire with the view; they are neither
authorization nor publication receipts. With no issued-cursor registry, arbitrary
well-formed edits of a last key are not all detectable; no such claim is made.

`PrepareGenerationReadIndex` explicitly prepares only the nonunique B-tree
`coldkeep_generation_source_read_v1` on repository/source/generation IDs, all
COLLATE C/text_ops/ASC NULLS LAST. It validates bounded catalog metadata, exact
ownership and absence of intents; creates and verifies a fresh owned recovery
checkpoint before first DDL; holds appropriate locks in one finite owned
transaction; validates the result and unchanged logical catalog/payload/binding.
No deparser, IF NOT EXISTS shortcut or unexpected-index rebuild is used.
Exact-present retries validate/reconcile without another checkpoint. After an
unknown commit outcome, absence requires a new checkpoint destination; conflicts
refuse. No persistent operation-history format is invented.

This index is optional physical capability. Schema20/package4/IMPORT_READY v4
and profiles17–19/SQLite17 stay unchanged. Browse reports preparation-required
when absent; passive reads never create it. Imported targets remain recovery-only
and refuse preparation. Untouched accepted C1 and current operators qualify
indexed/unindexed plaintext/encrypted recovery; all nine previous staged roles
remain, with the accepted C1 reader as the tenth required role.

The qualification matrix retains every accepted 722 recovery and 49 passive
requirement plus their package events, and adds C2/fixture cases. Ordinary
planner EXPLAIN ANALYZE/BUFFERS records first/middle/final pages at roughly
100/1,000/5,000 selected/distractor records inside the existing envelope. Rows,
filters, loops, buffers, sort work, response bytes, allocations and timing are
observations for this profile, not million-row capacity or universal latency
claims. Maximum names are tested separately.

Integrated local 2.04 completion requires the preserved predecessor reports,
accepted C1 bridge, owning C2 result and source-bound local qualification record.
The validator checks structure, exact predecessor hashes, required scopes and
input binding; it does not execute or certify those runtime results. Independent
review and both actual-head hosted events are additional acceptance conditions.
Implementation stays ACTIVE, product gates INCOMPLETE and later actions remain
outstanding. Ordinary writers/GC/repair/deletion/import activation remain fenced.
No Windows native-byte runtime, broader native delivery, physical-media
authenticity, failure-domain independence, reboot/unmount, cross-filesystem
transfer or schema20 live-GC survival is established here.
