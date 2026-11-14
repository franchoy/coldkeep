# Phase 3C1 bounded Go security maintenance

Status: **LOCALLY QUALIFIED**; independent final outgoing review and hosted
acceptance are **PENDING** at this source checkpoint. Phase 3C1 is not complete
until the external actual-head acceptance receipt closes it under BUILD section 7.

The owner authorized this bounded follow-up on 11 November 2026 after both
actual-head CI events at `e6922c95da8edb22b775ac13b831baefb023b14a` failed their
Ubuntu and Windows vulnerability scans for
[GO-2026-6859](https://vuln.go.dev/ID/GO-2026-6859.json). The finding reproduced on
unchanged accepted baseline `3aa9cae7b15037e6f51c22311e2b2ee0c0178b86` with Go
1.26.10. The prior [C1 qualification](phase3c1-result.md), all nine published
commits and their successes and failures remain preserved.

Active v2 development now pins Go **1.26.11** with `GOTOOLCHAIN=local`. The
[official distribution metadata](https://go.dev/dl/?mode=json&include=all)
binds `go1.26.11.linux-amd64.tar.gz` to SHA-256
`9849f4e04ee35303714e34fbc3ac8ab44ea3e448a95aa182f91e3b9b79c710ec`;
the downloaded archive matched its 66,999,944-byte size and checksum. The
compiler is task-local; no global configuration or product installation was
switched.

The [official Go Docker image](https://hub.docker.com/_/golang) is fixed to
`golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0`.
Registry index bytes, digest header, Linux amd64/arm64 child manifests and
configuration digests were verified; both configurations declare Go 1.26.11.
No floating toolchain or image is selected.

Only active toolchain/image pins and their build, install, CI, governance,
enforcement and documentation contracts change. Historical assertions retain
Go 1.26.10, including frozen v1 source-install instructions and snapshot
contracts. Existing regression identities remain; historically named container
tests explicitly distinguish frozen authority text from active file checks.
The remote consumer's reviewed-body checksum changes for exactly its two
compiler assertions; isolation, origin, selected graph, binary and cleanup
checks remain byte-identical otherwise.

All fourteen affected local obligations passed fresh with the verified task-local
Go 1.26.11 compiler at `05471b56c0231309bb07627d205c503f2744a51c`, tree
`3794bb730852d5d1c863561481c0f9f2a0eb2d0d`. The source remained clean and frozen
through the sequential queue. Nine standalone current/historical reader/writer
binaries and each staged route's nine binaries were rebuilt with fresh hashes
and Go build metadata. All original historical reader source bytes were checked
unchanged; standalone and per-route binary hashes are recorded separately.

| Fresh route | Passed tests / packages | Required tests / packages | Unrelated test / package skips | Wall seconds |
| --- | --- | --- | --- | --- |
| recovery | 896 / 4 | 722 / 4 | 0 / 0 | 623.958 |
| plain | 4722 / 46 | 722 / 4 | 139 / 3 | 854.135 |
| aes | 4722 / 46 | 722 / 4 | 139 / 3 | 804.543 |
| passive | 465 / 2 | 49 / 2 | 0 / 0 | 363.184 |

The required event checkers recomputed every mandatory identity from raw Go JSON.
Both schema20 recovery leaves, generation-only recipe references, exact native
names, ownership joins, SQL lifecycle/cleanup and isolated historical restores
passed. Staged reader hashes and resource inventories matched before/after;
each exact owned PostgreSQL fixture was disposed. AC was observed online before
each route, with no reconfirmation or agent power-policy change. The dedicated
recovery/passive 15-minute package bounds, full-codec 35-minute bounds and
2,400-second supervisor remained unchanged. Product and fixture limits stayed
fixed. No partial run or projected timeout result was accepted.

Fresh static/build/vet/module/lint and governance checks passed; Windows passed
compilation only. Prepared assets/native build and integrated Phase2 proof passed
85 native and 38 PostgreSQL cases, two browser profiles, four build/install
packaging cases, TCP boundaries, credential controls and owned-resource cleanup.
Current and unchanged D19-source oracles each returned the expected 23 outcomes:
12 restored, nine refused and two limits. The complete 658-test Python suite and
Go enforcement suite passed. Govulncheck v1.7.0, itself built with Go 1.26.11,
passed detailed, JSON and blocking scans with no vulnerabilities found. Tool,
compiler, binary, original source and generated asset provenance is retained.

Only this measured maintenance result and the current handoff link change after
the qualification commit. Exact Git bytes/modes bind every unaffected runtime,
test, harness, dependency, frontend and build input to that commit. Final static
and governance checks, independent review, patch replay and publication gates
must also pass on this documentation successor. Prior Go 1.26.10 executions
remain valid records of their original inputs and are not reused as evidence of
Go 1.26.11 execution. All failed attempts remain retained, including the earlier
e6922c95 hosted attempt's 84 logs and 35 artifacts, its four blocking scan failures
and two aggregate CI failures. The original 91-command ledger and approval stay
unchanged; the maintenance ledger adds fresh outcomes and explicit diagnostics.

The language floor remains **Go 1.25.0**, module identity remains `/v2`, and
third-party dependency versions and sums are unchanged. Test identities,
blocking security checks, product limits, fixture capacity, timeout and cleanup
contracts remain unchanged. No vulnerability suppression or gate waiver is
introduced.

Independent exact-source review, a freshly verified expected-old guarded
fast-forward of `release/v2.0`, both actual-head CI and CodeQL events, separate
findings/latest Codacy, source-bound evidence and final owned-resource cleanup
remain required. PR #129 stays draft; main and stable v1.13.19 remain unchanged.
No merge, tag, release, installation switch or Phase 3C2 work is authorized.
Parent 2.04 and Phase 3C remain ACTIVE; Phase 3C2 OUTSTANDING; implementation
ACTIVE; product gates INCOMPLETE.

Requested model/effort: GPT-6 Astra / Extra High. The parent identifies as
Codex/GPT-6; its exact runtime variant and effort are not exposed. The independent
review agent was requested as GPT-6 Astra / Extra High; those requested settings
do not establish independently observed runtime settings.
