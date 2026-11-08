# Action 2.02 internal delivery decision

Status: selected engineering proof, not product/native readiness. The owning
instruction is `coldkeep_phase2_codex_2026-11-08.md`. Product gates remain
INCOMPLETE; implementation remains ACTIVE. No 2.04 identity or migration work.

`cmd/coldkeepd` composes `internal/proofdaemon`, the existing engine and adapters,
and `internal/webui`. The frontend calls only `/api/v1`; it has no SQL or storage
engine. One process owns one explicitly configured disposable catalog/root/key
and one selected inherited snapshot. PostgreSQL remains the first Docker product
profile. SQLite is an explicitly selected Linux feasibility experiment.

The canonical root's existing `.coldkeep-control/repository.lock` is acquired
before opening the catalog, inspecting schema 17, or constructing the engine.
Preparation occurs only in explicit fixture setup. Unknown/missing backends,
unprepared roots, mismatched key/payload/catalog and unsafe/oversized fixtures
refuse. After bounded scalar/count checks, existing engine deep verification
validates the tuple; it does not initialize, migrate, repair or fall back.
No repository IDs, installation descriptors or schema/format changes are added.

Existing engine history queries can scan before limiting. This proof therefore
admits at most 32 snapshots and 256 historical/path/logical/current-file entries,
4096 recipe/chunk/block/reference/retired rows, 256 repair attempts, 128 containers
and repair-container rows, and 64 repository settings. Actual physical enumeration
stops at 130 root entries and refuses that boundary. Files are at most 16 MiB;
container files at most 64 MiB; encoded/decoded block bounds are 16 MiB. Names are
at most 1024 bytes and must already round-trip through the existing normalization
without change, with valid UTF-8. Unsupported names refuse explicitly. This
bounded fixture envelope is not a general scalable-history or capture design.

History is loaded once under the process lease; opaque random entry IDs and
session-specific cursors expose 32 entries per page. IDs do not encode SQL IDs,
absolute paths or credentials. Selected names are passed exactly to the existing
snapshot restore facade, under a fresh operator-configured destination, with
strict supported metadata and no overwrite. Failures report failed-or-partial:
bytes can be visible before a metadata error. No final-file reread is claimed
by the service; the proof harness performs its own independent reread.

Admission: 16 TCP connections before handler goroutines, 8 active requests,
8192-byte header budget, 2048-byte target, 4096-byte JSON body, one restore, no
operation queue and no detached jobs. One 256-bit bootstrap expires in two minutes;
exchange retires its private mode-0600 file and creates one 256-bit in-memory
bearer session for 30 minutes. Secrets never go into URLs, logs, command arguments,
cookies or browser persistent storage. Exact configured Host, same Origin for
mutations, hostile-Origin/Fetch-Site rejection and no CORS credential access apply.
Missing Origin on reads still requires the memory bearer; cookies are not accepted.
This does not implement the later F11 cookie/per-tab/password design.

Native listen/public URL must be the same explicit IP loopback endpoint. The
container may bind its interface, but the proof profile publishes only the fixed
host IPv4 loopback endpoint. Reverse-proxy forwarding headers confer no authority.
CSP permits only same-origin scripts/styles/connects and forbids framing, inline
scripts and external dependencies. HTML revalidates; Vite hashed assets are immutable.
Unknown API paths/methods have JSON errors and never fall back to SPA HTML.

The prototype finishes synchronous engine work after a client disconnect. It has
no durable acceptance, retry/idempotency promise, restartable task or HTTP 202.
Read/idle limits are 5/10/10 seconds and the response write limit is two minutes;
a lost/timed-out response has unknown outcome. PostgreSQL connect/statement/lock
limits are 5/5/1 seconds; SQLite busy timeout is one second. These timeouts do not
prove cleanup. Shutdown stops admission, drains handlers (including work whose
browser closed), joins the actual physical driver close, then releases the lease.
It waits for an OS-stalled operation rather than releasing ownership early. There
is one physical catalog connection, no automatic reconnect after uncertainty.
The accepted recovery session and its cleanup joins remain byte-identical.

The prepared source build and dependency policy are in [the build guide](phase2-build.md).
The native/exposure qualifications are in [the feasibility matrix](phase2-native-matrix.md).
Later owners: 2.04 identities/migration fences, 2.05 lifecycle, 2.07 durable jobs,
2.08 authentication, 2.14 SQLite lifecycle and 2.22–2.26 native distribution.
