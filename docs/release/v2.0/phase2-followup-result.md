# Local action 2.02: COMPLETE — internal delivery and Linux feasibility proof

Date: 8 November 2026. Mode: BUILD. Owning correction:
`coldkeep_phase2_followup_codex_2026-11-08.md`.
Implementation: ACTIVE. Product gates: INCOMPLETE.
Accepted local-profile 2.03: COMPLETE, unchanged. Corrected 2.02 awaits review.
Action 2.04 identities and explicit migration fences remains the next planned
product action and has not started. No push, merge, publication, release,
global configuration change or real-installation switch occurred.

P1 restore destination: PASS.
P2 TCP classification: PASS.
P3 complete-value byte bounds: PASS.
Fresh integrated qualification: PASS.

## Identity and input verification

Requested model: GPT-6 Astra. Requested effort: Extra High.
Actual model: unknown. Actual effort: unknown. The execution interface did not
expose either setting; the request is not an observation. No setting was changed
and no subagent was used.

Submitted base: `eb19d18da9307f61ea3a6f775565ec21a153f0d1`, tree
`4f6197ff35fd065c3432fdb7f19ee6a5f1b3e40f`. Its original worktree was clean;
all 1,733 submitted source hashes matched before the correction worktree was
created. The implementation checkpoint is
`553967e53e8dcc2853b5dba0c73f6104e92e3d71`, tree
`bfe1b019d67fdbf6cae638b2ed8e60820cefaf09`, on
`release/v2.0-phase2-followup` at
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase2-followup`.
All 27 qualifying commands ran with that clean identity. The final result commit
changes only current-authority/result documents; the exact final HEAD/tree,
commits and path delta are bound in accompanying `final-identity.json`, outside
the committed report to avoid a self-referential commit identity. Both patch
forms are independently replayed to that final tree in `patch-replay.json`.

Launch ZIP SHA-256:
`b592a6245d75a7852aeed6378fb2c05c3ffcade6387406d9f363fbc62b839843`.
All 50 checksums covering its other members passed. The original external
Phase 2 evidence ZIP was read at its recorded path: 1,501,905 bytes,
SHA-256 `eaafa155cd20824cdb333ec2b06d1afcd1c0edcc10b1cdced6499b7edb2b76ff`.
All 298 manifest entries and the 299-member coverage passed. ZIP paths were
checked before extraction. Its report, identity and both patches exactly matched
the launch copies. The submitted patches were not reapplied to the checkout.
Input verification and source verification are separate recorded checks.

The original `phase2-result.md` is unchanged, SHA-256
`b96fd8f311e6d482c53f1e40f081d980678fc6f1173965adcc5279820528a1fa`.
The launch review/instruction/identity and their hashes accompany the evidence;
the previous full archive is referenced by verified digest, not nested again.

## Submitted-source counterexamples and corrected behavior

An owned detached probe worktree started at the exact submitted HEAD. Only two
test overlays were added. Every one of its 1,733 tracked source hashes was checked
unchanged before probing and again before removal. The first run used the supplied
proposals without interface adaptations. The retained overlay adds explicit
catalog/inode observations and SQLite version/encoding facts. Both overlay versions,
raw commands and qualifying failures remain available; setup errors are not used
as finding evidence.

| Scenario | Expected | Submitted observation | Corrected observation |
|---|---|---|---|
| P1 payload root plus ASCII trailing space | Configuration refusal before lease/catalog/engine | Open succeeded; actual old-writer restore completed inside the source root with bytes `tiny`; configured output remained empty | `fixture_configuration_refused` during Config.validate; payload/control names, modes, inodes and content, catalog bytes/inode and configured output preserved |
| P1 trailing nonbreaking space | Same refusal | Same source-root mutation and empty configured output | Same early refusal and preservation |
| P1 ordinary sibling and internal-space output | Actual retained restore, unchanged source payload/control | Both original proposal controls passed | Both passed; independently reread SHA-256 `8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c`, mode 0640, mtime 1577934245 |
| P2 actual Alpine Wget against reachable 200 | Reachability detected | TCP accept; exit 0; submitted loop rejected confinement | Connect-only helper reports connected; confinement rejects |
| P2 actual Alpine Wget against reachable 403/404/stall | Reachability detected despite HTTP failure | Independently recorded accepts; each exit 1; unchanged submitted loop incorrectly recorded refused=true | All connected controls reject confinement; stalled control returns without awaiting the response |
| P3 65,541-byte NUL-containing label | Bounds refusal before engine reads | Pinned Go driver: SQL LENGTH=4, complete bytes=65541; envelope and Open passed; actual SnapshotShow returned all 65541 bytes | `fixture_limit_exceeded` from envelope/Open before Verify/SnapshotShow; refused label reread exactly unchanged |

P1 changes only adapter admission: a restore root whose existing engine
TrimSpace interpretation differs is refused, never trimmed or redirected. Existing
canonicality, directory/empty-output and separation checks remain. Engine restore,
storage containment, coordination, historical fixture bytes and oracle are unchanged.

P3 replaces every existing LENGTH text guard with OCTET_LENGTH: logical original
name, snapshot path, chunk hash, snapshot ID, snapshot label and container filename.
Budgets and schema stay unchanged. Scalar checks still precede allocation-producing
engine reads. On the pinned SQLite 3.54.1 / mattn 1.14.53 path, UTF-8 database
encoding is required before those guards. PostgreSQL requires both server and
client UTF8 encoding. Other encodings refuse without conversion or repair.
The pinned SQLite driver uses sqlite3_column_text/bytes and the direct-column
octet_length implementation uses stored-size metadata; source excerpts/digests
and runtime observations are retained in `driver-byte-semantics.json`.

Fresh controls cover exact-limit and one-over ASCII, exact-byte multibyte,
multibyte one-over below the old character bound, SQLite NUL oversize, empty
values and nullable labels. The existing schema's empty-path refusal is explicitly
preserved. SQLite UTF-16LE/BE witnesses show 1,024 storage bytes becoming 1,536 Go
UTF-8 bytes; both now refuse before catalog history queries. PostgreSQL scalar
cases run against its actual copied old-writer fixture, roll back each mutation,
and retain ordinary browser history/restoration. Non-UTF8 client encoding refuses;
UTF8 is restored before further qualification. No impossible PostgreSQL NUL value
is manufactured. Bounded empty and NUL labels also round-trip through SQLite history.

## TCP namespace, controls and actual exposure observations

The private helper is built with Go 1.26.10, CGO_ENABLED=0, `-trimpath
-buildvcs=false`, then mounted read-only in the existing pinned Alpine proof
image. It performs one numeric TCP connection: no DNS, proxy, redirect, HTTP
success predicate or retry. The 1,000 ms connect deadline is separate from
the 10-second Docker/subprocess startup deadline. Connected is decisive before
close; explicit refusal, connect timeout and route errors remain distinct.
Unexpected local errors, missing helpers/containers, malformed/missing/duplicate
or oversized output, wrong targets and outer timeouts invalidate proof.

Helper source SHA-256:
`6ee9aa4833d64f4bddf50671f11bfaac50b6d879a7c6fe7c8e77c7071b09535c`.
Helper binary SHA-256:
`66d37b5b080a079a4a5f4e59432213a3b1429b096781e951b9f8dc8eadd3e3df`.
Clean proof image:
`sha256:6c8a9494bd9a4b6230ec0491c15f3cd00ccba9f81639545371b11e8fdfd34737`.
The exact owned namespace/container inspection and commands are in
`integrated/docker/tcp.json` and `tcp-*.json`.

One isolated sibling-network container namespace runs the helper for all controls
and all real sibling targets. Owned ready listeners independently log accepts.
Reachable 200, 403, 404 and stalled controls report connected and fail confinement.
The stalled control took about 0.051 seconds including Docker exec. The closed
port reports Linux ECONNREFUSED (111). Actual missing-helper and missing-container
executions are rejected; missing/malformed result controls are rejected. An actual
mismatched Host request receives 403 and is independently logged by the listener.
All nine control names and required target results are enforced by the same
parser/campaign checker used by the integrated runner. Mutation tests additionally
reject duplicate/truncated output, wrong stage/target, unsupported socket errors,
missing accepts/controls/targets and nonzero/outer-timeout execution.

Fresh selected sibling targets: the published port 34637 on 192.168.1.141,
192.168.122.1, 172.20.0.1, 172.19.0.1, 172.17.0.1, 172.21.0.1, 172.18.0.1 and
172.22.0.1 each explicitly refused. Direct daemon target 172.22.0.3:18766
had a bounded connect timeout: no establishment within the declared interval,
not an explicit refusal. Separate host socket probes to those eight published
targets and [::1]:34637 all explicitly refused. No external physical LAN peer
or routed IPv6 network was available or claimed qualified.

The full fresh Docker proof retained CLI/daemon image builds, private disposable
PostgreSQL, packaged browser restores and hash/metadata rereads, non-root mounts,
resource limits, missing-bind refusal, browser-close ownership, second-owner
refusal, shutdown and CLI history. Unique per-run image tags prevent overwriting
the pre-existing local proof image; their cleanup is recorded. Image bases and
dependency pins did not change.

## Fresh qualification and explicit reuse

Fresh: 85/85 required Go test/subtest run/pass pairs and all 15 critical outputs,
with no skips; 38/38 PostgreSQL cases and all three outputs, with no skips;
all nine TCP controls; native and Docker browser journeys restoring nonempty,
empty and encrypted history with six independent hash/mode/mtime observations;
real CGO LocalWriter store/close/reopen/restore; missing/stale assets, independent
CLI installation and named no-CGO SQLite refusal. All 24 original Go requirements
and ten original critical outputs remain mandatory.

Fresh current oracle: all unchanged 23 cases matched (12 restores, nine refusals,
two known baseline limits). Fresh Python: 561 tests passed, plus 27 focused authority
tests. Build, vet, golangci-lint 2.9.0, dependency tidy diff, formatting, shell checks,
matrix, row-writer/source guards, CI audit/mutations and current/historical validators
passed. Hosted CI was not run. Structural validators do not certify runtime behavior.

Observed environment: Ubuntu 24.04.5 LTS, Linux x86-64 kernel 7.0.0-42, ext4,
GCC 13.3.0, glibc 2.39, Go 1.26.10 with GOTOOLCHAIN=local and GOWORK=off,
Node 24.22.2, npm 11.19.1, Docker 29.9.2 and Compose 5.6.1. Tool executable hashes,
native linkage, compiler flags and actual browser version are retained.
Prepared native binary SHA-256:
`8cf48617a2f454966751ae44875fa01e69599e84ccc673e7bede864c39a8a489`.
Its clean manifest binds the checkpoint, unchanged frontend lock/input hashes and
asset digest `5dd6f2163bd7802217b7da03a8e7947c4bbb4279d6f529b7cd79425dd57700dd`.
Frontend contract, lint/typecheck and build ran fresh in that prepared build.

Reused, explicitly not fresh: submitted recovery race evidence (418 passes,
all 244 required events, no skips); broad affected SQLite evidence (609 passes
and three existing unqualified skips); retained storage/history evidence (35
passes, no skips); and exact historical D19 23-case evidence. `reuse-ledger.json`
records original source identity, exact command/status and raw-log digests, the
verified external archive, 628 unchanged relevant source/dependency/runner hashes,
and all six historical D19 artifact equalities. The three broad-suite skips remain
`TestRestoreNativeDarwinTempPathWithoutEvalSymlinks`,
`TestStoreInterleavingRetryClaimSignalsCASBoundary` and
`TestStoreFolderWorkersFourWithConcurrentGCPlanNoCorruption`; none is a pass.
No shared implementation/dependency change invalidated this authorized reuse.

## Initial failures, preservation and cleanup

The original P1/P3 proposal run failed causally as expected and its ordinary
destination/label controls passed. The enriched test-only overlay additionally
assumed byte-identical SQLite catalog files after a valid restore; that assertion
was too strong because the unchanged engine performs temporary pin bookkeeping.
It was removed only from valid-restore controls. Refusal still requires exact
catalog bytes/inode, and valid restore still requires unchanged source payload/
control state and fixed content/metadata. An initial scalar test attempted an
empty snapshot path: the existing schema rejected it. The corrected test asserts
that preserved refusal. Both initial failures remain in raw logs. No production
expectation or budget was weakened. Development passes are not substituted for
the final clean-checkpoint runs.

All 33 predecessor registrations/checkouts (including nine absent registrations),
130 prior refs, accepted reports, 849 frozen historical files and pre-existing
containers/networks/image identities/tags are preserved. Frozen aggregate:
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
No schema, engine/storage/catalog/snapshot/coordination/recovery implementation,
payload/receipt format, fixture/oracle expectation, Go/frontend pin, image base
or CLI behavior changed. Full changed-path explanations accompany the final
source scope ledger.

Only owned disposable probe state, fixture copies, credentials, containers,
networks and per-run image tags were removed. Browser/daemon children were joined;
disposable PostgreSQL was destroyed with its roles/sessions. The owned probe
worktree was removed after retaining overlays and rechecking all submitted hashes.
Generated follow-up binaries, embedded assets, frontend build output and
node_modules were removed after identity/linkage capture. Accepted predecessor
tools/browser caches remain untouched. `cleanup.json`, fresh preservation checks
and patch replay cleanup bind the final observations. No keys, credentials,
fixture databases/payloads, binaries, browser caches or node_modules ship in the ZIP.

This is the selected internal Linux proof. Native distribution, clean-machine
support, SQLite lifecycle/defaults/live GC, production lifecycle/authentication,
durable jobs, other platforms, hosted CI, external LAN/routed IPv6, physical
failure-domain independence and product/release gates remain unqualified.

## Artifacts and next action

Evidence directory:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108`.
It contains `final-identity.json`, standalone `phase2-followup-result.md`,
`review.patch`, `ordered.patch`, `patch-replay.json`, source/validation/reuse and
cleanup ledgers, and `coldkeep-phase2-followup-evidence.zip` with complete
`SHA256SUMS` and an external `.sha256` file. Standalone and archived report,
identity and patches are checked byte-for-byte.

Review the corrected result and artifacts. The [next handoff](next-slice.md)
proposes 2.04 identities and explicit migration fences under a separate owning
instruction after review; it does not begin that action. Recommended execution
remains GPT-6 Astra / Extra High with observable settings recorded separately.

## Exact clean-checkpoint commands and raw logs

All commands below ran from the follow-up worktree at the checkpoint above,
with GOTOOLCHAIN=local, GOWORK=off and CGO_ENABLED=1 (the helper and no-CGO
experiments explicitly override CGO). Every exit status was 0. Raw logs are
under the evidence directory. `commands.jsonl` also preserves development and
expected failing counterexamples; `validation-ledger.json` binds this clean set.

| Check / raw log | Exact command | Raw log SHA-256 |
|---|---|---|
| `logs/qualifying-authority.log` | `python3 scripts/validate_release_state.py --state auto --json` | `7adb734d49d1eb72db5d2892b236e358bb324c025364d7a20850fc8cf593164a` |
| `logs/qualifying-build.log` | `go build ./...` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `logs/qualifying-ci-audit.log` | `bash scripts/audit_ci_enforcement.sh --local-only` | `560c20a9e133022a5758cc8ae9bee1d12270086fd72954d66e4db9ff416218e2` |
| `logs/qualifying-ci-mutations.log` | `go test -count=1 -v ./scripts -run 'TestAuditCIEnforcementRequires(Phase2Proof\|RecoveryCopy\|ProductFixtures)'` | `6fffd3352ec9bded2ddc838dcc8e2bb9dca7a5e9b49300a7b57c10e69ff0d944` |
| `logs/qualifying-cleanup.log` | `python3 /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/cleanup-owned.py` | `d39760dc7c0cb448bcd52b5657642d2d5206407e0fad93b56809c68ba3598dcf` |
| `logs/qualifying-environment.log` | `python3 /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/environment.py` | `bcd8f5b6e5c5f5f5cbafcf5aae8a9539efac6ecb93311e9277a2d8ccffc5d722` |
| `logs/qualifying-focused-authority.log` | `python3 -m unittest discover -s scripts -p test_v2_authority.py -v` | `17a44e3f4e79711c55947202e7f651c47c96024ce19b27fb429355279c5851c7` |
| `logs/qualifying-format.log` | `python3 /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/format-check.py` | `d6f51a60f8cac5cba30f2bf701f0f2632fb6b8fda1a6479e01c924bfa45b7ffa` |
| `logs/qualifying-governance.log` | `python3 scripts/validate_governance.py` | `42e7d014616f1f5af381f1ea546c743319a256d9d6c7c7b1517edb51adf0d7c1` |
| `logs/qualifying-historical-authority.log` | `python3 scripts/validate_release_state.py --authority historical-v1 --repo-root /home/franchoy/vscode/coldkeep/coldkeep --state auto --json` | `03b5cbba3b36f358fbe8f0361785f9e8d7cb68e847b6e5382ef62ea564e1111d` |
| `logs/qualifying-historical-governance.log` | `python3 scripts/validate_governance.py --authority historical-v1 --repo-root /home/franchoy/vscode/coldkeep/coldkeep` | `42e7d014616f1f5af381f1ea546c743319a256d9d6c7c7b1517edb51adf0d7c1` |
| `logs/qualifying-integrated.log` | `python3 scripts/run_phase2_proof.py --output /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/integrated` | `a47ea0ffc917ae85fa567c40948a50142b6f56569491db3053a7e13a7aba861b` |
| `logs/qualifying-linkage.log` | `python3 /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/linkage-check.py` | `90162c37fb531c45836851f66fdc7e65271913ad39f879791aa260413be51c01` |
| `logs/qualifying-lint.log` | `/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1b-20261108/tools/golangci-lint run ./...` | `e92606b0bf483111dff0a120c315ea165821348f31365020e2468a0059095c47` |
| `logs/qualifying-matrix.log` | `bash scripts/validate_validation_matrix.sh` | `12fc8152f4febe35d9e7774ed6e62ead4e2ec059b2147a823e022b9ec95dc48d` |
| `logs/qualifying-preservation.log` | `python3 /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/preservation.py checkpoint` | `182ac92edef1230f73964f2715c46b78470e57b524754f78a5415d938d6e1229` |
| `logs/qualifying-product.log` | `python3 scripts/run_product_fixtures.py --reader current --output /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/product-current` | `ca0d3f6f5acf9bbbed27f3b4e8b2924f0ab606581ca1e0e1a27f17937ffa93c8` |
| `logs/qualifying-proof-mutations.log` | `python3 -m unittest discover -s scripts -p test_phase2_runner.py -v` | `742e17e06ee98e413c0115f01d32536094895ed0ecf5c06e0bcf3adc65b1ea00` |
| `logs/qualifying-python.log` | `python3 -m unittest discover -s scripts -p 'test_*.py' -v` | `6c536af49f98e7b6af2198ecd726252077791fd9d45ddd6721d3d2646b8206d1` |
| `logs/qualifying-recovery-runner.log` | `python3 -m unittest discover -s scripts -p test_recovery_runner.py -v` | `0b4ac98b2c1c0baffd0a88521678787acd16e96b1e534038fedbe95d743e4efa` |
| `logs/qualifying-reuse-boundary.log` | `python3 /home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-followup-20261108/reuse.py` | `e4fb7c43955f42b16ec6c2575dfaa1cfbecef3ff7127c959edd632c1f99c23d7` |
| `logs/qualifying-row-writers.log` | `bash scripts/check_versioned_row_writers.sh` | `811fcc7840be0bb094138e05b679a8a5851bc45ea447e5d1dd41a7dc559aacef` |
| `logs/qualifying-shellcheck.log` | `shellcheck scripts/audit_ci_enforcement.sh scripts/run_recovery_export_tests.sh` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `logs/qualifying-smart-quotes.log` | `bash scripts/check_smart_quotes.sh` | `3ab4541ef6970b483669b9358cbb509822162de8a32b93788cf8083cc3f89a2c` |
| `logs/qualifying-tcp-mutations.log` | `python3 -m unittest discover -s scripts -p test_proof_tcp.py -v` | `200330d943b920fef1c5a1bfba9a6ddb5bf58178a941af031d7eca63a6c59db0` |
| `logs/qualifying-tidy.log` | `go mod tidy -diff` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `logs/qualifying-vet.log` | `go vet ./...` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
