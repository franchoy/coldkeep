# Coldkeep v1.12.1 Risk Register

Release name: v1.12.1 - Post-Migration CLI Contract Hardening

Status values: Open, Monitoring, Mitigated, Closed.

## CK-1121-R001 - CLI validation hardening causes behavior drift

Severity: High

Status: Monitoring

Mitigation: keep validation changes narrow, test both valid and invalid command forms, and compare
routed-command behavior where routing is involved.

Evidence required for closure:

- Focused tests show valid existing invocations still pass.
- New invalid invocations fail with clear errors.
- Full release gate passes.

Phase 1 evidence:

- Extra positional rejection is limited to the selected Phase 1 command batch.
- `snapshot stats <snapshotID>` remains valid; only a true trailing token after the optional
  snapshot ID is rejected.
- `verify system <fast|standard|full|deep>` remains valid; non-level extra tokens are rejected.

## CK-1121-R002 - JSON shorthand fixes alter automation output

Severity: High

Status: Open

Mitigation: limit JSON work to shorthand consistency, avoid unrelated JSON schema or field changes,
and add output contract tests for touched commands.

Evidence required for closure:

- `--json` shorthand tests pass for touched commands.
- Existing valid JSON output remains stable.
- Any intentional output-contract change is documented before release.

## CK-1121-R003 - Boolean parser changes affect existing scripts

Severity: Medium

Status: Monitoring

Mitigation: preserve known valid boolean forms, reject only invalid or inconsistent forms, and test
implicit true plus explicit true/false behavior.

Evidence required for closure:

- Boolean flag tests cover accepted and rejected forms.
- Compatibility checks confirm common script forms still work.
- Full release gate passes.

Phase 3 evidence:

- Parser-level boolean handling already honors explicit false; Phase 3 adds command-level regression
  coverage for the selected cases rather than rewriting parser semantics.
- `snapshot delete --force=false` and `snapshot delete --dry-run=false` remain usage failures instead
  of performing forced delete or dry-run preview.
- Bare `--force`, bare `--dry-run`, `--force=true`, and `--dry-run=true` remain valid for snapshot
  delete.
- `list --reverse=false` and `snapshot list --reverse=false` remain unsupported and do not introduce
  new reverse behavior.

## CK-1121-R004 - Empty filter rejection changes previously accepted commands

Severity: Medium

Status: Monitoring

Mitigation: reject empty values only where they have no safe semantic meaning, document the command
contract, and verify valid non-empty values are unchanged.

Evidence required for closure:

- Empty-value tests fail with clear validation errors.
- Non-empty value tests preserve previous behavior.
- Risk notes identify any intentional compatibility impact.

Phase 2 evidence:

- Empty and whitespace-only values are rejected for the selected Phase 2 flags only.
- Empty `--stored-path` is rejected before ID-based remove or restore handling can proceed.
- Explicit empty `snapshot create --id` is rejected instead of generating an implicit ID.
- Non-empty unsupported flags remain unsupported; Phase 2 does not add new search or snapshot-list
  filter behavior.

## CK-1121-R005 - Codacy cleanup creates correctness regression

Severity: Medium

Status: Open

Mitigation: accept only localized, behavior-preserving cleanup in v1.12.1, add tests for
logic-adjacent changes, and defer style-only or refactor-heavy findings.

Evidence required for closure:

- Cleanup diff is small and behavior-preserving.
- Relevant focused tests pass.
- Full release gate passes after cleanup.

## CK-1121-R006 - Patch release drifts into v1.13/v2 architecture work

Severity: High

Status: Monitoring

Mitigation: enforce the non-goals list during every phase, keep all work on `release/v1.12.1`, and
reject changes involving new migrations, storage formats, schema changes, backend defaults, daemon,
API, UI, NAS, cloud, or broad rewrites.

Evidence required for closure:

- Phase diffs contain no out-of-scope architecture or migration work.
- Final release review confirms schema, repository format, storage format, and default backend are
unchanged.
- Risk register is reviewed before tagging.

Phase 1 evidence:

- Phase 1 scope excludes search, simulate, benchmark, snapshot create/delete, architecture
  migration, schema changes, storage format changes, default backend changes, and Codacy cleanup.

Phase 2 evidence:

- Phase 2 scope excludes snapshot tag normalization, benchmark behavior, broad parser rewrites,
  architecture migration, schema changes, storage format changes, and default backend changes.

Phase 3 evidence:

- Phase 3 scope excludes parser rewrites, unrelated boolean flags, schema changes, storage format
  changes, default backend changes, and architecture migration.
