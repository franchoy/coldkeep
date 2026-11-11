# Action 2.01 product fixture oracle

`manifest.json` is the fixed expectation set. Keep its source-asset and restored
content hashes unchanged when diagnosing failures. Three retained files under
`internal/storage/testdata/compress_v1_18_supplemental` supply the persisted
old-writer repository. The manifest references its public disposable test key
separately; results never include the key value. No user repository or user key
is accepted by the harness.

The 23 cases are representative, not a Cartesian matrix:

- Seven retained files: empty, tiny, compressible/incompressible, multi-packed,
  plaintext/AES-GCM and none/zstd with actual persisted transforms.
- Reproducible legacy plaintext/AES, container major 0 and mixed legacy/packed
  recipes, using the named existing regression-builder patterns.
- Two inherited snapshot versions and Linux mode/mtime; Linux case and NFC/NFD
  pairs plus embedded-newline names. These are catalog-history fixtures, not
  newly scanned capture generations.
- Missing/truncated/corrupt payload, incomplete logical file, unavailable key,
  future schema, future packed metadata and actual future encoded-header refusal.
- Known limits: backslash normalization, and restore accepting future catalog
  block-version metadata while the encoded CKBL payload is still v1. The
  verifier rejects that metadata. Trailing-space snapshot names are refused.

The selected fixture platform is Linux; other targets are explicitly skipped
and the evidence runner rejects skipped/missing cases. UID/GID, ACL/xattr,
invalid UTF-8, empty-directory capture and Windows/macOS destination behavior
are not qualified by this set. The source-linked baseline ledger records owners.

From the repository root, with pinned Go 1.26.11 on PATH and a C compiler:

```sh
python3 scripts/run_product_fixtures.py --reader baseline --output /tmp/ck-fixtures-baseline-new
python3 scripts/run_product_fixtures.py --reader current --output /tmp/ck-fixtures-current-new
```

Use a new output directory each time; `--go /path/to/go` may select the pinned
executable without changing global configuration. The runner forces local
toolchain, CGO and no workspace overlay, strips ambient database/product config,
and runs `go test -race -count=1 -json -timeout=5m ./tests/productfixtures`.
All catalogs/payloads are disposable byte copies, including for refusal cases.
No product runtime constructor or migration is used to open the copied schema-17
catalog. Existing restore may pin/unpin chunks; final catalog meaning and payload
bytes are checked for preservation. Metadata is checked only where declared.

Baseline mode extracts exact D19 from local Git, verifies every original blob
before/after the run, and overlays only this manifest and the test harness.
The harness's `/v2/` import prefix is adapted to the historical module; no
production source is edited. Reported reader version must be 1.13.19.
Current mode reports 2.0.0-dev.0 and executes the same expectations.

`fixture-results.json` contains expected/observed hashes, metadata/refusal and
preservation outcomes; `execution.json` binds source/tree, command, toolchain,
manifest/harness hashes and exit status. Raw Go JSON and stderr are retained.
Working copies remain under the output directory for investigation, including
unexpected failures. Ordinary Linux CI runs current mode and uploads the compact
four output files; retained source fixtures remain the unchanged oracle.

This is an executable compatibility fixture harness, not a recovery exporter,
passive product-opening implementation, migration or platform qualification.
