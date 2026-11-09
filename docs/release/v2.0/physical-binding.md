# Phase 3B physical binding contract

Phase 3B BUILD is COMPLETE, awaiting independent review, under
`coldkeep_phase3b_codex_2026-11-09.md`; see [qualification](phase3b-result.md).
Parent 2.04 and implementation remain ACTIVE; product gates remain INCOMPLETE.
This contract precedes implementation. Qualification and actual limits belong
in the result; this document is not a qualification receipt.

The Linux/PostgreSQL16 maintenance facade in `internal/recoveryexport` reuses
the existing owned cut, export, passive connection cleanup and database
reservation. Ordinary writers remain limited to schema17. Schema18 keeps its
exact identity-lineage-v1/unbound meaning and all 53 constraints. Schema19 adds
six tables: binding_operation, root_binding_revision, root_binding_head,
configured_source, source_binding_revision and source_binding_head. The reader
capability is schema19 physical-binding-v1/recovery-only; package3/receipt3
are distinct from application and control-record versions.

Every operation has a unique bind_ ID, canonical request/plan digest and fresh
independently checked pre-operation export. Every committed operation advances
the root mapping revision by exactly one, including source changes, so the
descriptor/marker identify the complete configuration cut. Root revisions keep
the installation/incarnation, exact path, evidence and operation; the head has
a composite FK to its own revision. Configured sources use distinct source_
IDs and bounded unique slots 1–64, retain separate revision/head rows, and are
always capture-disabled. Disable retains identity and history. Source changes
require a future fresh scan unless same-object continuity is proven. Legacy
synthetic source/adoption/snapshot provenance and payload recipes are unchanged.
PK/UNIQUE/FK/CHECK/NOT NULL and the portable graph validator enforce these rules.
New constraints use the accepted relation-bound two-stage metadata envelope;
4096 bytes per constraint and the exact 94-constraint count bound aggregate
input to 385024 bytes. Schema18 retains 53 and 217088 bytes respectively.

The selected evidence class is linux-local-unique-mount-session-v1: opened-handle device,
inode, filesystem type and Linux STATX_MNT_ID_UNIQUE, plus boot ID and mount
namespace inode. Linux 6.8 or later with the returned UNIQUE mask is required;
missing unique-mount evidence refuses instead of downgrading. Ordinary mount
IDs and namespace inodes were observed to be reused during Docker recreation.
The admission allowlist recognizes ext4, XFS, tmpfs and overlayfs. This run
observed ext4 and tmpfs; XFS/overlayfs were not separately qualified. This is session-bound
consistency evidence, not hardware attestation or clone resistance. A reboot,
namespace change or mount recreation requires explicit reconnect. Same-object
offline relocation preserves root/control/native-lock objects. Reconnect may
change session observations but must preserve those objects and pass full
catalog/payload closure; it advances incarnation. Distinct old/new roots and
copied controls refuse. Cross-filesystem transfer is outside this profile.

Paths use canonical absolute native bytes encoded as base64, with a separate
display string where needed. No trimming, case folding or Unicode replacement
selects authority. A smaller 3072-byte native-path ceiling ensures base64 fits
the existing 4096-byte catalog-field envelope. Components `.` and `..` and
noncanonical syntax refuse before mutation; existing aliases resolve through
the established canonical-root lock domain. Catalog paths remain historical
provenance and are not required to exist during recovery.

Root marker `.coldkeep-control/root-marker.v1.json` (16 KiB), private
`installation.v1.json` (64 KiB), and `binding-intent.v1.json` (128 KiB) are
strict versioned JSON, regular single-link private owner files, accessed through
verified directory handles. Duplicate/unknown/trailing/deep/oversized records
refuse before typed allocation. Publication uses exclusive private staging,
file sync, atomic rename and parent sync. The repository lock is never replaced.
There is one pending intent per installation, one root and at most 64 sources;
all revisions consume unchanged catalog budgets and lower caller deadlines.

The explicit installation directory must preexist, be owned/private, outside
root/source/checkpoint/credential locations, and have a distinct observed mount
from the payload. A same-mount sibling refuses before any child creation.
This proves mount-availability separation only; hardware independence is unknown.
Status reads bounded installation information without credentials or catalog
access, reports pending reconciliation (including prepared staging) or root
mismatch, and never initializes.
An optional checked status also inspects the explicit catalog/key tuple.

Lock order is installation-directory flock, existing canonical-root lease,
then database-scoped advisory reservation. All remain held through checkpoint,
transaction, postcommit publication, cancellation, SQL driver/child/credential
cleanup and physical close. The database reservation closes before root lease;
the installation reservation closes last. Any observed root/control/lock loss
is terminal for that lease. No handle revival or mid-operation reacquisition.

Preview reads and hashes the explicit request, full catalog/payload inventory,
old mapping and current evidence. Apply reacquires and revalidates that plan.
For each operation: check locations/tuple and absence of any IMPORT_READY;
export and independently check a fresh checkpoint; sync prepared intent;
compare-and-swap schema/head and transactionally record operation/revisions;
join SQL cleanup; publish marker then descriptor; recheck durable tuple; remove
intent and sync its parent. Completion requires all cleanup to succeed.

| Crash cut | Explicit same-request reconciliation |
| --- | --- |
| Before durable intent, without a staging residue | Prior catalog remains; new preview/apply may retry with a fresh checkpoint |
| Invalid or partial prepared staging | Prior catalog/control tuple is unchanged; unexplained staging refuses automatic mutation |
| Intent present, no committed operation | Verify checkpoint and prior plan, execute same operation |
| Commit response uncertain | Query exact operation/plan; never infer rollback |
| Committed, marker or descriptor missing/stale | Matching durable intent plus actual catalog permit publication |
| Files published, intent retained | Verify exact tuple and finish same request |
| Intent removed | Same operation is idempotent only with complete matching files/catalog |

Ordinary startup cannot reconstruct a missing descriptor. Conflicting requests,
stale revisions, missing intent after incomplete publication and unexplained
control files refuse. Failure after commit reports reconciliation-required.
Process-crash qualification makes no general power-loss guarantee.

Portable schema19 export/import includes complete lineage and referenced payloads;
it excludes active descriptor/intent/lease authority. Import is recovery-only and
uses v3 IMPORT_READY. Any present receipt, including malformed/future artifacts,
refuses all binding and source maintenance. Validated recovery-only restoration
continues without original DB/root/source/descriptor. Activation remains deferred.
Phase 3C membership/exact entry names/policy/indexed lookup remains separate.


Operator commands use the existing private recovery config. `binding-status
-installation-directory /private/install` needs no credentials or catalog.
`binding-check -config /private/config.json -installation-directory ...` adds
catalog/key validation. `binding-source-status` also takes `-source-id`.
The config can use `payload_root_base64` instead of `payload_root` for native
bytes outside valid UTF-8; supplying both refuses. The explicit database role
is the credential lookup reference; the raw password and private password-file
location remain solely in operator custody.

`binding-preview` takes `-binding-action bind|relocate|rebind|source-register|
source-rebind|source-disable`, `-expected-schema`, `-expected-revision`,
`-operation-id bind_<32 lowercase hex>`, and `-installation-directory`.
Source operations additionally take `-source-id`, `-expected-source-revision`
and (except disable) `-source-path-base64`. Generate fresh IDs with 16 CSPRNG
bytes; configured sources use the source_ prefix. Review the complete returned
old/new mapping and plan digest. `binding-apply` and `binding-reconcile` take
the same arguments plus `-plan-sha256` and a fresh `-checkpoint` destination.
Both revalidate; neither turns a preview into durable authority.

A crash after checkpoint export but before durable intent preserves the old
catalog and the orphan checkpoint. Retry the same operation/plan with a fresh
explicit checkpoint destination; no existing generation is overwritten. Once
intent is durable, reconcile with its exact checkpoint destination. Missing or
conflicting intent cannot reconstruct authority from a copied database.

The aggregate blocking test timeout is ten minutes to include the added binding
families; individual operation/inventory/export ceilings are unchanged. The
unique-mount observation follows the [Linux statx contract](https://kernel.googlesource.com/pub/scm/docs/man-pages/man-pages/+/master/man/man2/statx.2).
