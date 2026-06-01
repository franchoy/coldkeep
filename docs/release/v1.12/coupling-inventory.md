# v1.12 CLI / Business Logic Coupling Inventory

Purpose: identify what must move from CLI to engine/catalog. This is the most important Phase 0
artifact. Cells marked `TBD` are filled in as each migration phase begins; the `Initial findings`
section below records what Phase 0 inventory already confirmed.

## Command map

| Command | Current orchestration owner | Direct DB access | Direct storage-context access | Direct filesystem access | Rendering mixed with behavior | Target owner | v1.12 phase |
|---|---|---|---|---|---|---|---|
| stats | engine (routed) | No (via engine) | Yes (loads context for engine) | TBD | TBD | engine | Phase 1 |
| inspect | CLI → observability.Service | TBD | TBD | TBD | TBD | engine | Phase 1 |
| verify | CLI → maintenance | Yes (maintenance reopens DB) | Yes | TBD | TBD | engine | Phase 1 |
| snapshot create | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot list | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot files | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot stats | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot diff | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot restore | CLI/snapshot/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 5/7 |
| snapshot delete | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| gc | CLI/maintenance | TBD | TBD | TBD | TBD | engine + catalog | Phase 6 |
| restore | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 7 |
| store | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 8 |
| store-folder | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 8 |
| remove | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 9 |
| repair | CLI/maintenance | TBD | TBD | TBD | TBD | engine | Phase 9 |
| recovery | startup/recovery | TBD | TBD | TBD | TBD | engine/recovery | Phase 9 |
| config | CLI/storage config | TBD | TBD | TBD | TBD | deferred or engine | TBD |

## Initial findings (Phase 0 inventory)

These are confirmed by static inspection of `cmd/coldkeep/main.go` at the v1.11.0 baseline. Line
numbers are indicative and must be re-confirmed at the start of each migration phase.

- `db.ConnectDB` is called directly from `cmd/coldkeep/main.go` in 3 places (approx. lines 841, 2588,
  2641).
- `verify` reopens the DB inside `maintenance.VerifyCommandWithContainersDir`
  (`internal/maintenance/verify_command.go`) via `db.ConnectDB()`, ignoring any engine-owned DB.
- Storage context is loaded via the `loadDefaultStorageContextPhase` variable (approx. line 193),
  which wraps `storage.LoadDefaultStorageContext`; numerous commands call it directly.
- No direct `QueryContext`/`QueryRowContext`/`ExecContext`/`BeginTx`/`sql.Tx` calls were found in
  `main.go` production paths (these appear in test files only).

## Direct DB access patterns

Search targets:

- `db.ConnectDB`
- `QueryContext`
- `QueryRowContext`
- `ExecContext`
- `BeginTx`
- `sql.DB`
- `sql.Tx`

## Direct storage context patterns

Search targets:

- `OpenStorageContext`
- `LoadDefaultStorageContext`
- `storage.Context`
- `containersDir` / `container.ContainersDir`
- repository config loading

## Rendering mixed with behavior

Search targets:

- `fmt.Print`
- `fmt.Println`
- `log.Print`
- `os.Stdout`
- `os.Stderr`
- clirender calls inside logic-heavy code

## Validation duplicated outside engine

Search targets:

- path validation
- worker validation
- limit validation
- snapshot ID validation
- stored-path validation
- output mode validation
