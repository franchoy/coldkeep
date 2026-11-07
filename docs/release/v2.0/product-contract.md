# Action 2.01 local product contract

This freezes the intended manual profile and its result vocabulary. It does
not implement capture, scheduling, a daemon, API, recovery export or new schema.
Inspected capabilities and limits are in [the baseline ledger](baseline-compatibility.md).

## Operating profiles and release checklist

| Profile / capability | Status after 2.01 | Owning evidence still required |
|---|---|---|
| Manual local PostgreSQL Docker, Linux x86-64 Docker Engine | Selected first v2.0.0 target at 2.17; not qualified | Actions 2.01–2.13, 2.15, 2.17. One owner/host/repository per daemon and local payload root; embedded UI, authenticated local API and CLI share Go operations. |
| Ubuntu 24.04 / ext4 reference | Candidate reference choices | Validate actual mount, lifecycle, restoration and durability behavior before support claims. |
| Linux native | Early feasibility selected, delivery unqualified | Separate 2.02 investigation; later native packaging/qualification. Compilation is not support. |
| SQLite engine/catalog | Retained existing compatibility; portable fixture backend | Not the first product default. Live SQLite GC is refused; full product lifecycle qualification remains 2.14. |
| SQLite Docker and backend conversion | Deferred, unselected prerequisites | 2.14/2.16; preserve each existing repository's backend and encryption. |
| Scheduling and policy retention | Deferred for first manual release | 2.19/2.20 and later execution scopes; first manual profile is keep-all. |
| Windows/macOS native packages, desktop shell, OS launch integration | Unqualified, independently selected later | 2.22–2.26; optional shell at 2.29. Lack of signing/build resources does not block PostgreSQL Docker. |
| Network shares, multiple hosts/roots, agents, hosted service | Outside selected local scope | Separate topology, fencing and later-series qualification. |
| Filesystem snapshots, rich ACL/xattr/link metadata, content search, key rotation | Optional/deferred | 2.31–2.34 and their capability-specific evidence; none silently becomes a first-release promise. |

First stable acceptance still requires preserving source originals, exact
history, verification, restore, independent recovery export and supported
v1.13.19 adoption. Ordinary files and directories, including empty directories,
belong to the product promise. No hosted account, subscription or telemetry
dependency is selected. Docker mount roles, authenticated browser credentials,
bounded ingestion and common reachability remain the charter's F03–F14 contracts.
Prepared binaries/images must contain UI assets; source builds run frontend
then Go. None of these future acceptance boxes is checked by fixture success.

## Capture, archival import and selection

Recurring capture is a new source generation observed over a declared interval,
with its selection-policy revision, per-entry outcomes and published membership.
One-off archival import intentionally brings selected existing files/history
into the repository without creating an implied recurring schedule. Both share
content-storage primitives, preserve originals and retain explicit origin.
An inherited catalog snapshot is labeled inherited history: it is not evidence
of a fresh source scan or a point-in-time application-consistent snapshot.

Selection names the source root/binding, included paths, exclusions and traversal
policy. Initially follow no symlinks and exclude symlinks/special files with
visible unsupported/excluded outcomes. Ordinary directories have metadata and
membership without content recipes. Empty directories are distinct from empty
files. Do not silently follow aliases, fold case, normalize native names or
replace unsupported representations. Exact-name transport belongs to later
capture/restore adapters, with explicit mapping/refusal at incapable destinations.

An exclusion means outside the selected scope, not a deletion. Unavailable,
unreadable and unstable sources/entries are recorded distinctly; an unavailable
root must never become an authoritative empty capture. Removal of a configured
source disables future work but deletes neither originals nor historical
captures. Explicit history expiry and reference-safe GC are separate later
operations; keep-all is the first manual policy. There is no bidirectional
synchronization, original-file deletion authority or peer-to-peer storage.

Capture is best-effort per-file observation unless a separately qualified
snapshot adapter establishes stronger consistency. Record changed-during-read,
unreadable, excluded and unsupported entries without silently substituting data.
Only the later protected publication protocol may advance a committed generation;
a mutable current-file list or a successful `StoreFolder` is not that protocol.

## Result glossary and evidence dimensions

| Dimension | Meaning and evidence | Does not imply |
|---|---|---|
| Accepted intent | Validated request durably recorded with identity/retry/operation parameters at the later job boundary | Work started, data committed or capture complete. No durable job API exists yet. |
| Committed publication | Declared catalog/membership publication boundary completed under the recorded failure model; identity/digest/receipt bind the generation | Atomic filesystem snapshot, complete scope, verification, independent copy or power-loss durability. |
| Capture completeness | Selected membership reconciled with the declared policy; explicit counts/outcomes for included, excluded, failed, unsupported and unstable entries | Exclusions are deletions, or unavailable source means empty. Full/partial legacy snapshot selection is a different attribute. |
| Verification coverage | Named method/level, scope, time and checked hashes/graph/metadata, plus precise failures | Every byte or every generation was checked, a fresh read occurred, or verification runs automatically. |
| Independent protection | A completed usable copy plus reader/key requirements and evidence of a separately confirmed failure domain | A catalog dump, same-disk export, hard link or planned destination provides disk-loss protection. |
| Restored content | Selected recipes reconstructed with expected hashes; installation result identifies destination and visible bytes | Correct metadata, application consistency, a final-file reread or a complete repository recovery. |
| Achieved consistency / fidelity | Explicit per-file or stronger capture boundary and actually applied metadata capabilities | Atomic application/database backup or fidelity for unsupported ACLs/names/links. |

Keep restoration stages separate: reconstruct and verify content; atomically
install under the destination safety contract; apply metadata; optionally
perform and record a separate post-write reread. A metadata error after
installation reports visible correct bytes plus failed/partial metadata. It
must not be flattened into either total success or “nothing restored.” The
fixture harness records its own post-write hash read and mode/mtime checks;
that does not add those steps to the production engine.

| Worked example | Intent / commitment | Completeness | Verification | Independent protection | Restore / consistency |
|---|---|---|---|---|---|
| Generation publishes nine files; tenth selected file was unreadable | Accepted and committed under its declared publication rule | Incomplete, nine included and one failure; exclusions reported separately | Only explicitly checked bytes/graph | Not established | No restore performed; per-file observation only |
| Generation commits all selected entries; verification job has not run | Accepted and committed | Complete for that recorded policy/interval | Unverified; commit is not verification | Not established | No restore proof |
| v1 snapshot is adopted as inherited catalog history | Adoption intent may commit a mapping; original snapshot identity retained | Fresh-capture completeness is not applicable/unknown; legacy selection scope retained | Only the named inherited recipes checked | Depends on an independently held recovery set | Selected historical bytes may restore; no fresh scan or new capture implied |
| Full quiesced export completes on the same disk | Export publication complete, assuming later 2.03 checks passed | Complete package closure | Catalog/object checks explicitly recorded | **No disk-loss independence** | Portable candidate only until isolated import/restoration is demonstrated |
| Restore installs verified bytes, then chmod fails | Restore operation partially achieved; bytes visible | Requested selection and per-file outcomes recorded | Reconstructed content passed; post-write reread only if separately done | Unrelated to this operation | Content installed, metadata failed; no unconditional healthy/success flag |

## Action 2.01 acceptance and downstream owners

The [fixture manifest](../../../testdata/product-fixtures/manifest.json) declares
valid restores, refusals and known baseline limits separately. Valid files must
match fixed historical hashes and selected metadata. Invalid/incomplete,
unsupported and key-unavailable fixtures must produce the declared refusal at
the appropriate existing boundary without repair/replacement or unintended
source mutation. Unexpected failures remain evidence to investigate; known
limits cannot be relabeled as successful lossless capture.

2.03 receives this ledger/oracle for passive opening and a quiesced inventory,
then separately completes package publication/import and original-installation-
loss restoration before schema additions. 2.02 packaging/native feasibility is
a separate available branch after these fixtures. 2.04/2.10–2.12 own identity,
exact names, bounded ingestion, generation membership and restore fidelity.
2.15 owns adoption/upgrade proofs. None is implemented by this contract.
