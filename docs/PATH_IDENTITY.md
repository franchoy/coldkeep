# Path Identity Policy (v1.2)

Coldkeep path identity rules define how `physical_file.path` is normalized and compared.

This document is intentionally narrow: it covers current-state path identity for
the `physical_file` layer. Snapshot query/path behavior is documented separately
in `README.md` because snapshots use slash-normalized stored paths rather than
host filesystem identity.

## Rules

1. Paths are canonicalized structurally using:
   - absolute path resolution
   - path cleaning (`.` / `..` / separator normalization)
   - symlink resolution when resolvable (`EvalSymlinks`)

2. If symlink resolution fails with not-found (late-bound destination), identity falls back to cleaned absolute path.

3. Path identity is case-sensitive.

4. No case-folding is applied, regardless of OS.

## Examples

- `./data/../data/report.txt` and `data/report.txt` resolve to the same identity after cleaning.
- If `current` is a resolvable symlink to `releases/2026-04/report.txt`, both paths resolve to the same identity.
- `A.txt` and `a.txt` are different identities.

## Rationale

- Preserves correctness on case-sensitive filesystems (Linux).
- Avoids silent collisions (for example, `A.txt` vs `a.txt`).
- Keeps identity deterministic and aligned with filesystem semantics.
- Supports robust dedup/ref-count behavior by ensuring one canonical identity per resolved path.

## Scope

This policy applies to the current-state physical file layer (`physical_file`) in v1.2.
Any future cross-platform case behavior changes are policy changes and must be versioned and migration-reviewed before implementation.

## Non-Goals

- It does not define user-facing snapshot query matching semantics.
- It does not promise case-insensitive behavior on Windows or macOS.
- It does not treat unresolved, late-created symlink targets as equivalent until they can be resolved.
