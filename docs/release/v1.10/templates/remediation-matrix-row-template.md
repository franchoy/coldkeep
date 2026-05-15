# Remediation Matrix Row Template

Status: Complete  
Owner phase: Phase 4 - Issue Tracking Schema Freeze

## CSV Header

```csv
matrix_id,root_invariant,title,summary,source_issue_ids,release_target,status,severity,domain,secondary_domains,data_loss_risk,security_risk,determinism_risk,recovery_risk,affected_commands,affected_files,expected_invariant,planned_fix,required_regression_tests,required_ci_gates,acceptance_criteria,closure_proof,decision_rationale,notes
```

## Example Row

```csv
CK-110-M001,"Commands must reject unexpected positional arguments","Reject ignored extra positional arguments","Several commands accept and ignore trailing positional arguments instead of failing usage validation.","CK-110-0001;CK-110-0002;CK-110-0003",v1.10.1,triaged,S2,cli,json,none,none,medium,none,"init;version;inspect;verify;snapshot;simulate;repair","cmd/coldkeep","Unexpected positional arguments are rejected before repository initialization where possible.","Centralize arity validation and add command-specific regression tests.","CLI contract tests for extra args","go test ./tests/cli/...","Every affected command rejects extra args with non-zero exit and no state mutation.",,,
```

## Required Fields

Every matrix row must include:

- matrix_id
- root_invariant
- title
- summary
- source_issue_ids
- release_target
- status
- severity
- domain
- risk fields
- expected_invariant
- acceptance_criteria

A matrix row should not be closed without closure_proof.

## Validation checklist

- [ ] Matrix row template file exists
- [ ] Header matches `remediation-matrix.csv`
- [ ] Example row exists
- [ ] Required fields listed
- [ ] Closure proof rule included
