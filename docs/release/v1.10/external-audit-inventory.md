# v1.10 External Audit Inventory

Status: Complete  
Owner phase: Phase 6 — External Audit Import

## Purpose

This document records the external audit findings imported for the Coldkeep v1.10 stabilization train.

The external audit is used to identify:

- CLI contract inconsistencies
- JSON output inconsistencies
- validation gaps
- restore and recovery edge cases
- GC and reachability risks
- packed-storage parity gaps
- benchmark/tooling correctness defects
- CI/script masking risks
- path normalization risks
- migration or compatibility risks

The external audit is not used as final remediation design.

Every imported finding must later become one of:

- fixed
- accepted with rationale
- deferred with rationale
- suppressed / not applicable with rationale
- duplicate of another tracked issue
- grouped into a remediation matrix row
- converted into a regression test or CI invariant

## Phase 6 Scope

Phase 6 imports and organizes the external audit findings.

Phase 6 does not:

- implement fixes
- create final remediation matrix rows
- change production code
- change test code
- change scripts
- change CI enforcement
- close findings as fixed
- permanently accept or suppress risks

Actual remediation begins in later v1.10.x releases.

# Preliminary External Audit Families

External audit findings are imported as raw issue rows and assigned a preliminary family.

The final remediation grouping will happen in Phase 7.

## Families

| Family ID | Family | Primary target | Primary domain |
|---|---|---|---|
| `EXT-CLI-001` | Extra positional arguments ignored | v1.10.1 | cli |
| `EXT-CLI-002` | Value flags accept another flag as value | v1.10.1 | cli |
| `EXT-CLI-003` | Boolean `=false` handled as presence/true | v1.10.1 | cli |
| `EXT-CLI-004` | Duplicate singleton flags silently last-win | v1.10.1 | cli |
| `EXT-CLI-005` | JSON shorthand rejected despite output-mode support | v1.10.1 | json |
| `EXT-VAL-001` | Empty explicit filters become broad/no-op operations | v1.10.2 | validation |
| `EXT-VAL-002` | Whitespace-only values treated as meaningful input | v1.10.2 | validation |
| `EXT-VAL-003` | Tag normalization is inconsistent | v1.10.2 | snapshot |
| `EXT-VAL-004` | Numeric limits/ranges accept zero, negative, NaN, or Infinity incorrectly | v1.10.2 | validation |
| `EXT-VAL-005` | Validation happens too late after repository initialization | v1.10.2 | validation |
| `EXT-REST-001` | Restore flags accepted outside valid mode | v1.10.5 | restore |
| `EXT-REST-002` | Stored-path restore empty value falls through | v1.10.5 | restore |
| `EXT-BENCH-001` | Benchmark duplicate case names overwrite previous rows | v1.10.6 | benchmark |
| `EXT-BENCH-002` | Benchmark report type/envelope not validated | v1.10.6 | benchmark |
| `EXT-BENCH-003` | Benchmark numeric finiteness not validated | v1.10.6 | benchmark |
| `EXT-BENCH-004` | Benchmark self-comparison or mismatched input accepted | v1.10.6 | benchmark |
| `EXT-SCRIPT-001` | Scripts emit non-standard JSON or malformed CSV | v1.10.6 | tooling |
| `EXT-SCRIPT-002` | Scripts append duplicate headers or mix schemas | v1.10.6 | tooling |
| `EXT-CI-001` | Release/smoke scripts miss required environment propagation | v1.10.6 | ci |
| `EXT-CODE-001` | Duplicate unreachable code or low-risk cleanup | backlog | tooling |

## Family Assignment Rule

Family assignment is preliminary.

Phase 6 assigns family IDs so the raw findings are easier to review.

Phase 7 will decide the final remediation matrix rows and may merge, split, or retarget these families.

# Import Rules

## Source

All rows imported from the external audit use:

```text
source=external-audit
```

## Source IDs

External audit raw source IDs use:

```text
EXT-RAW-0001
EXT-RAW-0002
EXT-RAW-0003
```

## Coldkeep IDs

External audit rows continue after the existing Codacy CK-110-xxxx IDs.

Do not restart CK-110-0001.

## Status

Initial status should normally be:

```text
open
```

Use triaged only when severity/domain/target/root invariant are already obvious and reviewed.

## Decision

Initial decision should normally be:

```text
investigate
```

Use fix only for findings that are unquestionably real and actionable.

Do not use fixed, accepted, deferred, or suppressed in Phase 6 unless this is only documenting an already-decided non-code planning outcome.

## Notes

The notes field should preserve:

- family ID
- original command/example
- original audit description
- any risk hint

## Validation checklist

```markdown
- [ ] Source is defined as `external-audit`
- [ ] External source ID format is defined
- [ ] CK ID continuation rule is defined
- [ ] Initial status rule is defined
- [ ] Initial decision rule is defined
- [ ] Notes preservation rule is defined
```

# Imported Baseline

## Source

Frozen evidence file:

```text
docs/release/v1.10/evidence/third_party_coldkeep_v1.9.0_code_analysis.baseline.txt
```

Imported into:

```text
docs/release/v1.10/issue-tracker.csv
```

Optional generated summary:

```text
docs/release/v1.10/external-audit-summary.csv
```

## Import Status

Status: Complete

The external audit was imported into issue-tracker.csv using the Phase 4 schema.

Each imported row preserves:

- synthetic external audit source ID
- source evidence file path
- preliminary family ID
- audit finding title
- audit detail text in notes
- first-pass target release
- first-pass severity
- first-pass domain
- first-pass root invariant

## Count Summary

Full generated counts are recorded in `external-audit-summary.csv`.

Minimum dimensions recorded:

- family
- severity
- domain
- release target
- regression-test requirement
- CI-gate requirement
- data-loss risk
- determinism risk
- recovery risk

# First-Pass Interpretation

The external audit findings are high-value because they mostly target behavior that scanners do not understand well.

## Highest-priority behavioral classes

The following classes are important because they affect correctness, safety, automation, or release confidence:

- ignored extra positional arguments
- empty filters broadening operations
- boolean `=false` acting as true
- inconsistent JSON shorthand support
- mixed or invalid JSON output
- benchmark duplicate/malformed input acceptance
- NaN/Infinity propagation
- late validation after repository initialization
- restore-mode validation gaps
- CI/release script environment gaps

## Most likely v1.10.1 input

Likely v1.10.1 CLI/JSON inputs:

- `EXT-CLI-001` extra positional arguments ignored
- `EXT-CLI-002` value flags accepting another flag
- `EXT-CLI-003` boolean false parsing
- `EXT-CLI-004` duplicate singleton flags
- `EXT-CLI-005` JSON shorthand mismatch

## Most likely v1.10.2 input

Likely v1.10.2 validation/security inputs:

- `EXT-VAL-001` empty filters broaden operations
- `EXT-VAL-002` whitespace-only values
- `EXT-VAL-003` tag normalization
- `EXT-VAL-004` invalid numeric values
- `EXT-VAL-005` late validation

## Most likely v1.10.5 input

Likely v1.10.5 restore/recovery inputs:

- `EXT-REST-001` restore flags accepted outside valid mode
- `EXT-REST-002` stored-path empty value fallthrough

## Most likely v1.10.6 input

Likely v1.10.6 CI/tooling/benchmark inputs:

- `EXT-BENCH-001` duplicate case identity
- `EXT-BENCH-002` benchmark type/envelope validation
- `EXT-BENCH-003` numeric finiteness validation
- `EXT-BENCH-004` self-comparison/mismatched input validation
- `EXT-SCRIPT-001` non-standard JSON output
- `EXT-SCRIPT-002` malformed or mixed CSV append behavior
- `EXT-CI-001` missing environment propagation

# Potential S0/S1 Candidate Feed

The following external audit rows may need review during Phase 12 — Initial S0/S1 Candidate Review.

```text
CK-110-1454: S1 EXT-VAL-001 v1.10.2 `remove --path ""` can broaden deletion scope
CK-110-1458: S2 EXT-SCRIPT-002 v1.10.6 `run_phase8_restore_sequence.sh` appends results without schema/version marker
CK-110-1467: S2 EXT-SCRIPT-002 v1.10.6 `scripts/run_phase8_restore_sequence.sh` can append duplicate CSV headers
CK-110-1478: S2 EXT-CLI-005 v1.10.1 `remove --json` is rejected
CK-110-1479: S1 EXT-CLI-005 v1.10.1 `gc --json` likely rejected
CK-110-1480: S1 EXT-CLI-005 v1.10.1 `repair --json` likely rejected
CK-110-1500: S3 EXT-CODE-001 backlog `restore` accepts and ignores `--mode` without `--stored-path`
CK-110-1501: S3 EXT-CODE-001 backlog `restore` accepts and ignores `--destination` without `--stored-path`
CK-110-1502: S1 EXT-REST-002 v1.10.5 `remove --stored-path "" 123` can fall through to ID removal mode
CK-110-1503: S1 EXT-REST-002 v1.10.5 `restore --stored-path "" ...` has the same empty-flag fallthrough
CK-110-1534: S2 EXT-VAL-004 v1.10.2 `restore --workers -1` is parsed without consistent validation
CK-110-1535: S3 EXT-CODE-001 backlog `snapshot restore --workers 0` behaves differently from normal restore
CK-110-1540: S2 EXT-VAL-004 v1.10.2 `restore --limit 0` is accepted
CK-110-1541: S2 EXT-VAL-004 v1.10.2 `snapshot restore --limit 0` behaves inconsistently
CK-110-1543: S3 EXT-CODE-001 backlog `remove --limit` is parsed but unused
CK-110-1554: S1 EXT-CLI-005 v1.10.1 `--json` shorthand is only partially implemented
CK-110-1561: S3 EXT-CODE-001 backlog `remove --dryRun` and `--failFast` aliases are accepted but not documented
CK-110-1573: S3 EXT-CODE-001 backlog `snapshot restore --mode override` can overwrite symlink targets
CK-110-1579: S2 EXT-VAL-004 v1.10.2 GC deletion trusts DB container filenames
CK-110-1581: S3 EXT-CODE-001 backlog Restore non-overwrite mode has TOCTOU overwrite race
CK-110-1593: S3 EXT-CODE-001 backlog `snapshot restore --mode override` checks raw path before normalization
CK-110-1611: S3 EXT-CODE-001 backlog Phase 8 restore script has missing-value crashes
CK-110-1614: S3 EXT-CODE-001 backlog Stored-path restore prefix mode still permits traversal
CK-110-1615: S3 EXT-CODE-001 backlog `RestoreDestinationOriginal` returns raw stored path
CK-110-1629: S1 EXT-REST-002 v1.10.5 `NormalizeSnapshotPath()` allows empty normalized components
CK-110-1634: S3 EXT-CODE-001 backlog Restore path sanitization differs between snapshot restore and storage restore
CK-110-1640: S3 EXT-CODE-001 backlog `scripts/run_phase8_gc_sequence.sh` validates neither `--remove-ratio` range nor numeric parse
CK-110-1658: S3 EXT-CODE-001 backlog Stored-path prefix restore has similar `..` escape risk
CK-110-1675: S1 EXT-REST-002 v1.10.5 `--mode=` silently becomes `original`
CK-110-1681: S1 EXT-REST-002 v1.10.5 `restore --input ""` is treated as no input file
CK-110-1682: S1 EXT-REST-002 v1.10.5 `remove --input ""` is treated as no input file
CK-110-1691: S3 EXT-CODE-001 backlog Snapshot restore can restore to the wrong path when the planned output already exists as a directory
CK-110-1692: S3 EXT-CODE-001 backlog Snapshot `--mode override` can also restore inside a directory
CK-110-1693: S3 EXT-CODE-001 backlog Snapshot restore collision detection can be bypassed by directory reinterpretation
CK-110-1696: S3 EXT-CODE-001 backlog Pre-release checklist contradicts README on stored-path restore
CK-110-1700: S3 EXT-CODE-001 backlog `snapshot restore --path <file> --mode override` is rejected even when exactly one file matches
CK-110-1701: S1 EXT-REST-002 v1.10.5 `snapshot restore snap missing_dir/` can succeed with 0 restored files
CK-110-1719: S3 EXT-CODE-001 backlog Restore can return an error after the file was already renamed into place
CK-110-1720: S3 EXT-CODE-001 backlog `restore --overwrite` is not portable to Windows
CK-110-1724: S3 EXT-CODE-001 backlog Restore write errors can be hidden behind misleading final errors
CK-110-1730: S3 EXT-CODE-001 backlog Restore loses chunk-location resolution errors
CK-110-1746: S3 EXT-CODE-001 backlog Remove-by-ID loses Postgres row-locking on physical mappings
CK-110-1747: S3 EXT-CODE-001 backlog Restore can leave chunks pinned if unpinning fails mid-loop
CK-110-1748: S3 EXT-CODE-001 backlog Packed restore recipe stores `ContainerID = 0`
CK-110-1749: S1 EXT-REST-002 v1.10.5 `remove --stored-paths --input ""` falls back to positional mode
CK-110-1750: S3 EXT-CODE-001 backlog `RestoreFileWithDBResult()` defaults to overwrite
CK-110-1751: S3 EXT-CODE-001 backlog `RestoreFileWithStorageContextResult()` also defaults to overwrite
CK-110-1761: S3 EXT-CODE-001 backlog `remove-by-stored-path` last reference leaves logical metadata behind
CK-110-1762: S3 EXT-CODE-001 backlog `remove-by-stored-path` can leave chunk `live_ref_count` stale
CK-110-1782: S3 EXT-CODE-001 backlog Packed block deletion helper ignores snapshot reachability
CK-110-1788: S3 EXT-CODE-001 backlog Stored-path restore in `--strict` mode can return error after content restore succeeded
CK-110-1803: S1 EXT-VAL-001 v1.10.2 Benchmark corpus cleanup can remove generated dirs without checking base-dir safety
CK-110-1811: S3 EXT-CODE-001 backlog `scripts/smoke.sh` only validates the first restored file per restore directory
CK-110-1830: S3 EXT-CODE-001 backlog Packed restore verification trusts DB chunk sizes before payload validation
CK-110-1835: S3 EXT-CODE-001 backlog Packed block deletion planning does not detect cross-container duplicate references
CK-110-1836: S1 EXT-REST-002 v1.10.5 `inspect block` legacy paths can falsely report "missing" for packed-only repositories
CK-110-1843: S3 EXT-CODE-001 backlog Packed restore path does not fully validate reconstructed chunk ordering
CK-110-1848: S3 EXT-CODE-001 backlog Restore collision planning does not account for packed multi-file reconstruction failures
CK-110-1898: S3 EXT-CODE-001 backlog Snapshot deletion leaves orphan `snapshot_path` rows
CK-110-1934: S3 EXT-CODE-001 backlog Benchmark restored-tree hashing follows symlinks
CK-110-1967: S3 EXT-CODE-001 backlog Remove-by-ID does not check `rows.Err()` after reading `file_chunk` rows
CK-110-1978: S3 EXT-CODE-001 backlog `RemoveFileWithDBResult()` misses `rows.Err()` after chunk scan
CK-110-1988: S1 EXT-REST-002 v1.10.5 Verify accepts malformed non-empty chunker versions that restore rejects
```

These are not finalized as S0/S1 in Phase 6.

Phase 12 will decide whether they are:

- confirmed S0/S1
- downgraded after evidence review
- merged into a broader matrix row
- converted into regression/CI gate requirements

Likely candidates include:

```text
remove --path "" can broaden deletion scope
stored-path restore empty value falls through
JSON breakage in correctness-critical commands
late validation after repository initialization for stateful commands
benchmark/CI false-success paths that can mask release failure
```

# Phase 6 Completion Statement

The external audit baseline has been imported into `issue-tracker.csv`.

Phase 6 completed:

- raw external audit text parse
- external audit issue row import
- synthetic source ID assignment
- first-pass family classification
- first-pass release target assignment
- first-pass severity assignment
- first-pass domain assignment
- generated summary counts
- likely v1.10.1/v1.10.2/v1.10.5/v1.10.6 input identification
- potential S0/S1 candidate feed for Phase 12

Phase 6 did not:

- fix findings
- close findings
- accept risks
- defer risks
- suppress findings
- create remediation matrix rows
- change production code
- change tests
- change scripts
- change CI enforcement
