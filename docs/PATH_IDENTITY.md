# Path Identity Policy (v1.2)

Coldkeep path identity rules define how `physical_file.path` is normalized and compared.

## Rules

1. Paths are canonicalized structurally using:
- absolute path resolution
- path cleaning (`.` / `..` / separator normalization)
- symlink resolution when resolvable (`EvalSymlinks`)

2. If symlink resolution fails with not-found (late-bound destination), identity falls back to cleaned absolute path.

3. Path identity is case-sensitive.

4. No case-folding is applied, regardless of OS.

## Rationale

- Preserves correctness on case-sensitive filesystems (Linux).
- Avoids silent collisions (for example, `A.txt` vs `a.txt`).
- Keeps identity deterministic and aligned with filesystem semantics.
- Supports robust dedup/ref-count behavior by ensuring one canonical identity per resolved path.

## Scope

This policy applies to the current-state physical file layer (`physical_file`) in v1.2.
Any future cross-platform case behavior changes are policy changes and must be versioned and migration-reviewed before implementation.
