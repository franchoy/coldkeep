# Prepared internal daemon builds

Use exact Go 1.26.10 with `GOTOOLCHAIN=local GOWORK=off`, Node 24.22.2 and npm
11.19.1. These are build/test tools; the installed program needs no Node process.
The lockfile pins React/react-dom 19.3.1, TypeScript 7.0.2, Vite 8.3.6, React type
packages 19.3.0 and Playwright 1.65.0. See [notices](../../../web/THIRD_PARTY_NOTICES.md).

From a clean local source checkpoint:

```sh
export GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=1
python3 scripts/generate_proof_types.py --check
python3 scripts/prepare_daemon.py
bin/coldkeepd --diagnostics
```

`prepare_daemon.py` verifies tool versions, uses `npm ci --ignore-scripts`, checks
types and UI source policy, builds static Vite assets, computes an input fingerprint,
embeds a manifest and builds Go with `-trimpath -buildvcs=false`. Generated assets
and node_modules are ignored and never committed. The manifest records clean
source HEAD/tree, lock/tool identities, source-input fingerprint and each asset's
SHA-256 plus aggregate digest. It is not stamped into the source that identifies
it, avoiding self-reference. Native VCS metadata may be absent and stays unknown;
independent prepared-source binding is a separate diagnostic field. The Docker
builder compiles the same prepared source/assets with CGO=0; record its separate
image identity and build flags rather than inferring them from the asset preparer.

`--allow-dirty` is explicitly diagnostic and marks the manifest dirty. Qualifying
proofs must use the default clean mode. `--check` rejects missing/damaged assets
and a changed frontend/config/lock/generator input fingerprint. Installed binaries
check the embedded manifest and actual embedded hashes; they do not require the
checkout or claim to detect hypothetical future source edits. HTML/API status
expose useful asset/application identity. Missing assets produce a named startup
or diagnostic refusal. The placeholder permits ordinary Go builds and independent
CLI diagnostics while making an unprepared daemon unusable.

CLI-only installation stays independent:

```sh
GOTOOLCHAIN=local GOWORK=off go install ./cmd/coldkeep
```

No frontend package or generated assets enter this command's dependency graph.
No daemon module-install or published v2 tag is advertised. Module-distribution
qualification requires actually shipping and testing the generated assets later.

The repeatable blocking integrated proof is:

```sh
npm --prefix web ci --ignore-scripts
node web/node_modules/playwright/cli.js install chromium
python3 scripts/run_phase2_proof.py --output /absolute/new/evidence-directory
```

It executes actual Go packages with race detection and named event enforcement,
then packaged browser restores, missing/stale/no-CGO/CLI-install experiments and
both Docker image paths. Docker, a C compiler and browser runtime libraries must
already be available locally. CI's ephemeral Ubuntu host installs browser runtime
packages explicitly. Failed, skipped or missing required evidence never passes.
The PostgreSQL fixture helper requires private explicit configuration and is only
executed by the Docker proof; ordinary package tests identify that helper as skipped.
It is not one of the core runner's required tests or a substitute for Docker proof.

Operator configuration is a private JSON file; fields are defined by
`proofdaemon.Config`. Use only explicit copied fixtures and freshly created output
and private runtime directories. `TestProofFixture` is a test-only setup helper,
never automatic daemon startup behavior. Invoke the binary with `--config` pointing
to that private file; give the one-use runtime bootstrap to the internal browser
through a trusted local channel. Do not put credentials in shell arguments.

Docker uses [the separate proof profile](../../../deploy/phase2/compose.yaml),
not the development smoke profile. Source fixtures mount read-only; copied payload
and legacy lock, fresh output and runtime bootstrap have distinct writable mounts;
operator config/key mounts read-only. PostgreSQL uses private-network, bounded
1 GiB tmpfs state with no host port. Application root is read-only, non-root, with
all capabilities dropped, no-new-privileges, 64 PIDs, 512 MiB memory and 64 MiB /tmp.
All binds use existing paths and `create_host_path: false`. No Docker socket mount.
Data is outside /app. Stop and recreate after uncertain mount reattachment; no hot
reattachment behavior is implemented. `proof_docker.py` prepares and cleans only
its owned disposable resources and retains the concrete mount/exposure observations.

The P1–P3 follow-up builds `scripts/proofprobe` with the same pinned Go toolchain,
CGO disabled, and mounts that private helper read-only in the existing pinned
proof image. It records source/binary/image identities, actual commands/statuses,
connect deadlines and independent listener accepts. The Docker runner uses
unique per-run image tags through PROOF_IMAGE and removes only its owned tags;
it preserves pre-existing local proof images. The integrated gate requires all
new Go cases, PostgreSQL scalar controls and TCP control/target results.
