# Local action 2.02: COMPLETE — internal delivery and Linux feasibility proof

Date: 8 November 2026. Mode: BUILD. Owning instruction:
`coldkeep_phase2_codex_2026-11-08.md`, applied within the user's explicit bounded
execution request. All three selected packages and applicable integrated criteria
passed. Conditional local completion is authorized; no closure-only run is needed.

Product gates: INCOMPLETE. Implementation: ACTIVE. Application: 2.0.0-dev.0.
Module: github.com/franchoy/coldkeep/v2. Latest stable: 1.13.19.
First product profile: manual-postgresql-docker. Accepted 2.01 and local-profile
2.03 COMPLETE reports remain unchanged. This result grants no product, native,
SQLite lifecycle/default, durable-job or production-authentication readiness.
Action 2.04 was not begun. Nothing was pushed, published, deployed, tagged or merged.

Requested model/effort: GPT-6 Astra / Extra High. Actual model: unknown. Actual
reasoning effort: unknown. Neither is observable through the execution interface;
no account/global setting was changed and no subagents were used.

## Source and input identity

The launch ZIP's SHA-256 matched
`2e15abc74e897dfe1f8632e926261315f47e6b178c7d116aba2697808ae0d40c`.
All 18 outer checksum entries and all 134 nested accepted-evidence entries passed;
the nested ZIP matched
`08906c46c46f9d810b8b17d87b296c63f353f5952dbfca6459085ff48cd46425`.
Standalone accepted report, identity, review/ordered patches and events matched
the nested copies. All 51 starting-source hashes matched the accepted checkout.
The launch README, acceptance, owning instruction, action/reading sequence,
shared C03–C06/C12–C14 and repository authority/planning contracts were read.
Archived helpers were treated as evidence, never executed or reapplied.

| Identity | Exact value |
|---|---|
| Accepted base HEAD | `e25da48003f30aeacb900131be28c3e092f85283` |
| Accepted base tree | `bad2400116d4dbb4fd50446acd48034c82ffb2d8` |
| Tested clean implementation HEAD | `82f3ad0b6d8bd80825871562098e5919f2158475` |
| Tested clean implementation tree | `20c3d19851b4d8e2ec226efd2737b0152d021970` |
| Branch | `release/v2.0-phase2` |
| Worktree | `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase2` |
| Evidence root | `/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase2-20261108` |

All qualifying runtime/structural commands ran from that clean implementation
checkpoint. `commands.jsonl` binds exact parent commands, HEAD/tree/status,
start time, exit code and raw log SHA-256. Tracked runners define their executed
subcommands; raw nested logs and inspect records are retained. Environment and
linkage records contain actual tool commands, versions, flags and outputs.
The later completion commit changes only the result/current-authority documents;
`final-identity.json` records its exact HEAD/tree and verified path delta. The
prepared binary remains bound to the tested checkpoint, not the later report commit.

## Three executed proof packages

**Packaged process: PASS.** A real prepared `coldkeepd` serves embedded
React/TypeScript/Vite assets and typed `/api/v1` without a production Node process.
OpenAPI-to-TypeScript checking and Go DTO agreement passed. Browser external
traffic was blocked; both native and Docker journeys loaded only embedded assets
and service APIs, with zero external requests or page errors. The UI visibly
identifies the internal proof, incomplete gates, backend, source and asset digest.
Keyboard historical selection and visible completion were exercised. Screenshots
and browser observations are retained under `integrated/browser` and
`integrated/docker`.

Status, capabilities, bounded history and one synchronous restore are implemented.
Search/jobs return structured unsupported errors with false capabilities. Unknown
API paths and wrong methods never fall back to HTML. Exact Host/Origin admission,
wrong/missing authentication, expired/replayed bootstrap and unauthenticated
mutation refusal passed. Bootstrap/session credentials are transient and private;
this is not the later F11 cookie/per-tab/password design.

Prepared assets are generated from exact pinned inputs, then embedded in Go.
CLI-only `GOTOOLCHAIN=local go install ./cmd/coldkeep` passed from an archived
source copy with neither generated assets nor frontend dependencies in its graph.
Missing manifest diagnostics, deliberately stale frontend input, and corrupt
embedded bytes refused as expected. A no-CGO binary requesting SQLite returned
`sqlite_cgo_required` without fallback. The prepared source build is documented in
[phase2-build.md](phase2-build.md); daemon module installation is not advertised.

| Prepared identity | Observed value |
|---|---|
| Native binary SHA-256 | `1cea9a4a966fec48f50c8c1a40c7d32ebe346022b753f6daff027012ec44d848` |
| Embedded aggregate asset SHA-256 | `5dd6f2163bd7802217b7da03a8e7947c4bbb4279d6f529b7cd79425dd57700dd` |
| Frontend input fingerprint | `305ffe32da2a556b927f39147cb560364f3f0a179d692d07fa3d1adccd75d8b1` |
| Frontend lockfile SHA-256 | `76e17ca164c04647a2c2521807985a78db0295236bbc29e36a23dc14844b3682` |
| Native build | Go 1.26.10, CGO=1, linux/amd64, `-trimpath -buildvcs=false` |
| Native Go VCS metadata | revision absent, modified null; independently verified prepared binding is separate |
| Build/test frontend | Node 24.22.2, npm 11.19.1, React/react-dom 19.3.1, TypeScript 7.0.2, Vite 8.3.6 |
| Browser test | Playwright 1.65.0, Chromium 158.0.8127.18 |

**Authenticated engine journey: PASS.** Actual headless browsers authenticated,
loaded the copied historical snapshot, selected and restored each file through
the existing engine. The harness independently reread output bytes and metadata;
expected values were fixed before execution, not derived from new output.
The following outcomes matched on both native SQLite and PostgreSQL Docker:

| Historical selection | Bytes | Expected and observed SHA-256 |
|---|---:|---|
| `history/Case.txt` | 4 | `8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c` |
| `history/empty.txt` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `history/encrypted.bin` | 4096 | `6b1d4bfe25f0144686cade6a6f77f222e6ef929b949a9d688e418d56dd3af609` |

All six browser outputs had mode 0640 and mtime `2020-01-02T03:04:05Z`, as
expected. Source fixture payloads and oracle bytes remained unchanged. The
PostgreSQL test helper explicitly copied historical rows into a disposable
catalog; that helper is fixture setup, not product migration or passive fallback.

The canonical existing payload-root lease precedes catalog/schema/engine work.
Wrong backend/key/root, unprepared root and exceeded read/admission limits refuse.
A real second process remained excluded after the browser closed, even with its
invalid catalog configuration. Explicit daemon shutdown exited zero; a new daemon
then acquired ownership. The container also retained its lease after browser
closure and refused an actual second owner. A controlled mid-restore disconnect
test proved continuing work completes while shutdown waits and ownership stays
held. The physical-driver-close test proved release waits for actual close and
no replacement connection is opened. Startup failure and collision cases passed.
These are synchronous, bounded operations; lost responses have unknown outcomes,
metadata failures can leave bytes visible, and safe automatic retry is not claimed.

**Linux SQLite and PostgreSQL Docker: PASS.** The native experiment used an actual
CGO SQLite catalog and `LocalWriter`, existing engine store/finalize, full close,
reopen, engine restore and independent hash. Known nonempty input
`Coldkeep phase 2 native persistence\n` restored with expected/observed SHA-256
`64ab254c8aba6ce0e03aedb1f29fbbe2f6bbe07372e0a8527712dfd07925bd4f`.
SQLite reported 3.54.1. No simulated writer or in-memory persistence claim was used.
The separate copied historical browser cases supplied old-reader evidence.

Observed host: Ubuntu 24.04.5 LTS, Linux 7.0.0-42-generic, x86-64, ext4
(`/dev/nvme0n1p5`, rw/relatime), GCC 13.3.0, GNU ld 2.42, glibc 2.39.
The binary is dynamically linked with interpreter `/lib64/ld-linux-x86-64.so.2`
and libc.so.6; bundled SQLite does not require a separately installed libsqlite3.
`file`, `ldd`, `readelf` and `go version -m` outputs are retained. This proves the
observed host runtime only, not a minimum libc or clean-machine distribution.

The authorized sole Go dependency movement is mattn/go-sqlite3 v1.14.24 →
v1.14.53 and its two replaced checksum pairs. The maintained same-line release
updates embedded SQLite and includes lifetime corrections; official upstream
release/build documentation was checked on 8 November. Go 1.26.10, language
floor 1.25, pq 1.11.2, compress 1.20.2 and x/sys 0.44.0 remain fixed. Fresh affected
driver evidence is below. Frontend official sources, selected versions, registry
integrities and licenses are retained in `dependency-upstream.json` and
[THIRD_PARTY_NOTICES.md](../../../web/THIRD_PARTY_NOTICES.md).

The original CLI Dockerfile and separate daemon proof image both built and ran
with CGO disabled and no runtime Node. CLI image ID:
`sha256:ed1701594583de7f64307120f169763b7f4dfbde551516d73bcf2cdd93bd476b`;
daemon image ID:
`sha256:6a2dc1e2c7335676c6158ed548a43988ce28469d9a6e7b585f73b47fae63e59c`.
The frontend manifest describes its native asset-preparation environment;
Docker's independently retained build/diagnostics establish CGO=0 for the image.
The retained CLI read the PostgreSQL fixture history after daemon shutdown.

Docker Engine 29.9.2 and Compose 5.6.1 were observed. Official port-publication
and Engine 28 documentation were checked; the runner refuses pre-28 engines.
Actual daemon UID:GID was 1000:1000, read-only rootfs, all capabilities dropped,
no-new-privileges, 64 PID limit, 512 MiB memory and 64 MiB temporary filesystem.
Source fixtures and credentials were read-only; copied payload/control and fresh
output were explicit host directories; bootstrap custody was private. There was
no socket mount and no database host port. PostgreSQL 16.15 used its pinned image
and bounded tmpfs on a private dedicated network. Missing `create_host_path:false`
mount refused without creating an empty source. Mount/network inspect facts are
retained in `integrated/docker/docker.json`.

Only `127.0.0.1:36643` was published in this qualifying run. Connections to all
observed non-loopback host IPv4 addresses and host `::1` refused. An isolated
sibling Docker network could reach neither those published host endpoints nor
the daemon's direct container address. This establishes the selected local
confinement observations. No external physical LAN peer or routed IPv6 peer was
available; those paths remain unqualified. Network IPv6 was disabled. Fixed
mounts require controlled stop/recreation after uncertain reattachment; hot
reattachment is unimplemented. No global firewall/Docker configuration changed.

## Fresh validation, reuse and limitations

All commands below exited zero on the clean implementation checkpoint. Exact
arguments and raw log digests are in `commands.jsonl`; no cached Go test result
was counted as fresh (`-count=1`, race where applicable).

| Check | Actual result |
|---|---|
| Integrated ordinary-Linux proof runner | PASS; 24 required Go test/subtest run+pass pairs, all 10 critical events, browser, packaging and Docker stages |
| Current product oracle | PASS; unchanged 23 fixed cases, no missing/failed case |
| Retained recovery/export/import/coordination/command race gate | PASS; all unchanged 244 required events, 418 test/subtest passes, zero skips; original-loss, post-live-GC and large receipt retained |
| SQLite/reader/store affected selection | 609 test/subtest passes; 3 explicit existing skips described below; zero failures |
| Retained restore/pin/snapshot/maintenance selection | 35 test/subtest passes, zero skips |
| Full Python suite | PASS; 552 tests, fresh 295.520-second run |
| Focused authority suite | PASS; 27 tests, exact 0/2.01/2.02/2.03 and historical/future/publication boundaries |
| New proof-runner mutations | PASS; nonzero exit, every missing critical event, every missing/skipped required case refuse |
| CI audit and mutations | PASS; seven new proof-step mutations, all six retained recovery-step mutations and product fixture mutations |
| Go build / vet / pinned golangci-lint 2.9.0 | PASS |
| Frontend generated types / strict TypeScript lint / Vite build | PASS |
| go mod tidy -diff / dependency scope / gofmt / shell syntax / ShellCheck | PASS |
| Validation matrix / row-writer audit / smart quotes / whitespace | PASS |
| Current authority/governance and explicit frozen-checkout historical-v1 validators | PASS; authority label remains structural only |
| Frozen history / accepted source / predecessor preservation | PASS; 849-file digest and all predecessor identities/status preserved |

The broad affected selection also encountered existing skips, which are not
passes: macOS temp-path alias regression on Linux; SQLite nested retry-CAS flow
unsupported by its locking mode; concurrent SQLite store+GC overlap unsupported
by its active locking mode. SQLite live GC remains refused. None is a required
selected proof case; no expected skip bypasses the integrated or recovery gates.
The selected fixture proof does not claim these concurrency/lifecycle features.

Only the exact D19 23-case historical baseline is reused for comparison:
HEAD `d3581602aee2599547564322fe083baae1d52063`, tree
`96b2840cb9da9714705bb6c2998f46b7ed3e78ee`. Its original command, original context
status, exit, raw logs and digests are retained under `reused-d19` and verified
against the original evidence ZIP's checksum manifest and bytes. The original
baseline verified 1631 source blobs. The manifest/harness and old-writer fixtures
are unchanged. Historical driver pins remain part of that immutable baseline;
they do not certify the updated current driver. Fresh current oracle and affected
checks supply that evidence. Other historical broad results are preserved only,
not represented as fresh or substituted for required checks.

Initial development failures are retained: TypeScript lacked the Vite CSS
side-effect declaration; explicit PostgreSQL fixture schema setup lacked its
bootstrap opt-in; three staticcheck simplifications were required; the retained
CLI smoke initially used the wrong storage environment-variable name. Corrections
were the Vite declaration, test-helper-only bootstrap opt-in, equivalent code
simplifications and existing `COLDKEEP_STORAGE_DIR`. Final assertions/oracles and
accepted recovery sources were not weakened. All corrected paths passed at the
clean checkpoint. Development runs with dirty source are not qualifying evidence.

No selected proof is blocked. Windows/macOS/other architectures, clean-machine
native packaging, installers/signing/notarization, WebView/startup integration,
physical loss-domain independence, external LAN/routed IPv6 peer behavior and
hosted CI execution were not attempted or qualified. The local gate and audit
were exercised; no hosted CI run is claimed. See the
[native matrix](phase2-native-matrix.md) and [architecture decision](phase2-architecture.md).

## Preservation, cleanup and handoff

All 32 original worktree registrations remain: 23 present checkouts with unchanged
HEAD/tree/status and nine absent registrations preserved. All 129 original refs
remain unchanged. The accepted checkout stays clean at its stated base identity;
all 51 supplied source hashes still match there. Frozen v1 history remains 849
files with digest
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
Accepted reports, engine/readers/storage/schema/coordination/recovery formats,
fixture/oracle bytes, existing recovery runner and original CLI Dockerfile remain
unchanged. No real repository was adopted or migrated.

Owned daemon processes were joined. Owned PostgreSQL/daemon containers, networks,
private credentials, temporary source probes and copied working fixtures were
removed; no owned container/network/session remains. The current oracle's 84
working files (19,361,351 bytes) were removed after raw results were retained.
Local generated build assets, binary, private tools and proof image tags remain
build artifacts and are excluded from the review ZIP. Unrelated resources and
all predecessor evidence remain untouched. Cleanup and final preservation records
are included.

Review artifacts in the evidence root are `phase2-result.md`,
`final-identity.json`, `review.patch`, `ordered.patch`, individual `patches/`,
`coldkeep-phase2-evidence.zip`, its external `.sha256`, and
`artifact-verification.json`. The review and ordered patches must each replay
from the accepted base to the exact final tree; replay records bind the outcome.
The compact archive carries a complete `SHA256SUMS` plus raw relevant logs,
source/asset/runtime identity, browser observations, initial failures, preservation,
reuse and cleanup. Standalone report/identity/patches are checked byte-for-byte
against the archive. It contains no binaries, node_modules, runtime keys,
credentials or working catalogs/payloads. These are local artifacts only.

The concrete next proposal is **2.04 identities and explicit migration fences**
after review under a separate owning instruction, recommended GPT-6 Astra /
Extra High for identity/compatibility/migration reasoning. Its scope and acceptance
must be established before execution; it was not started here. This is a practical
sequence choice, not a new formal dependency. Actions 2.05, 2.07, 2.08, SQLite
lifecycle/default qualification, at least one later fully qualified native target,
and remaining product/release gates stay outstanding. See [next-slice.md](next-slice.md).
