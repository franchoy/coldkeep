# Phase 3C1 bounded Go security maintenance

Status: implementation checkpoint; affected qualification and hosted acceptance
are PENDING. Phase 3C1 is not complete.

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

Fresh compiler provenance and rebuilds of all current and historical-source
readers are required. Historical reader source bytes stay unchanged. Every
Go-affected local route, prepared native/browser/Docker proof, installation,
security scan and enforcement suite must be requalified. Prior Go 1.26.10
executions remain valid records of their original inputs and are not evidence
of Go 1.26.11 execution.

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
