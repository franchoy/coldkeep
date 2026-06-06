# v1.12.3 Validation Checklist

## Phase-Level Validation

- Phase 0: run `git diff --check`; run markdown lint if available.
- Phase 1: run `git diff --check`; run markdown lint if available. Closure requires confirming no
  Go files, tests, root README, version files, parser behavior, CLI behavior, JSON behavior,
  exit-code behavior, schema behavior, storage behavior, backend behavior, or engine behavior
  changed.
- Phase 2: run `git diff --check`; run the artifact tracking check. Closure requires confirming
  only approved generated outputs are removed or ignored and no intentional benchmark baselines are
  removed.
- Phase 3: run the final hygiene release gate and record the outcome before claiming readiness.
  Closure evidence is recorded in `docs/release/v1.12.3/release-gate.md`.

## Final Hygiene Release Gate

Mandatory final gate:

```bash
git status -sb
git log --oneline main..release/v1.12.3
gofmt -l $(git ls-files '*.go')
golangci-lint run ./...
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
git diff --check
git status -sb
```

The release is ready for PR review only after the Phase 3 gate is green and the result is
recorded.

Final gate status: green, recorded in `docs/release/v1.12.3/release-gate.md`.

## No-Go Checks

- No Go code changes.
- No test changes.
- No parser behavior changes.
- No CLI behavior changes.
- No JSON behavior changes.
- No exit-code behavior changes.
- No schema changes.
- No repository or storage format changes.
- No default backend changes.
- No engine, catalog, or storage migration.
- No v1.13 implementation work.
- No Codacy cleanup.
- No benchmark threshold changes.
- No benchmark baseline migration unless explicitly reviewed in a later release.
- No removal of committed baseline files unless they are explicitly proven generated and obsolete.

## Docs Checks

- v1.12 release-train status cleanup is factual and does not overclaim readiness.
- v1.12 docs clearly state the release train is complete and v1.13 is the next
  architecture-stabilization step.
- v1.12.3 docs remain hygiene-only and actionable.
- All v1.12.3 phases stay on `release/v1.12.3` until the final release gate is green.
- Phase documents preserve the distinction between planning, cleanup, validation, and release/tag
  readiness.

## Artifact Cleanup Checks

- Confirm whether the root benchmark and regression validation outputs are tracked:

```bash
git ls-files benchmark-none-w1.json benchmark-none-w4.json benchmark-zstd-w1.json benchmark-zstd-w4.json regression-report-none-w1.json regression-report-none-w4.json regression-report-zstd-w1.json regression-report-zstd-w4.json
```

- Verify each candidate file is a generated validation output, not an intentional committed
  baseline.
- Remove only approved generated outputs that are tracked.
- Add ignore rules only if they prevent the same generated local outputs from being retracked.
- Confirm ignore rules cover generated root `benchmark-none-*`, `benchmark-zstd-*`,
  `regression-report-*`, and optional local `artifacts/` outputs without matching committed
  benchmark baseline files.
- Confirm benchmark baselines and meaningful release evidence remain visible after cleanup.
