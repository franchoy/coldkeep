# Coldkeep v1.12.1 Risk Register

Release name: v1.12.1 - Post-Migration CLI Contract Hardening

Status values: Open, Monitoring, Mitigated, Closed.

## CK-1121-R001 - CLI validation hardening causes behavior drift

Severity: High

Status: Open

Mitigation: keep validation changes narrow, test both valid and invalid command forms, and compare
routed-command behavior where routing is involved.

Evidence required for closure:

- Focused tests show valid existing invocations still pass.
- New invalid invocations fail with clear errors.
- Full release gate passes.

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

Status: Open

Mitigation: preserve known valid boolean forms, reject only invalid or inconsistent forms, and test
implicit true plus explicit true/false behavior.

Evidence required for closure:

- Boolean flag tests cover accepted and rejected forms.
- Compatibility checks confirm common script forms still work.
- Full release gate passes.

## CK-1121-R004 - Empty filter rejection changes previously accepted commands

Severity: Medium

Status: Open

Mitigation: reject empty values only where they have no safe semantic meaning, document the command
contract, and verify valid non-empty values are unchanged.

Evidence required for closure:

- Empty-value tests fail with clear validation errors.
- Non-empty value tests preserve previous behavior.
- Risk notes identify any intentional compatibility impact.

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

Status: Open

Mitigation: enforce the non-goals list during every phase, keep all work on `release/v1.12.1`, and
reject changes involving new migrations, storage formats, schema changes, backend defaults, daemon,
API, UI, NAS, cloud, or broad rewrites.

Evidence required for closure:

- Phase diffs contain no out-of-scope architecture or migration work.
- Final release review confirms schema, repository format, storage format, and default backend are
unchanged.
- Risk register is reviewed before tagging.
