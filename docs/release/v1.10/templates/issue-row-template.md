# Issue Row Template

Status: Complete  
Owner phase: Phase 4 - Issue Tracking Schema Freeze

## CSV Header

```csv
ck_id,title,source,source_id,source_file,source_line,source_rule,source_category,source_subcategory,source_severity,source_tool,source_commit,release_target,status,severity,domain,root_invariant,matrix_id,duplicate_of,breaking_risk,data_loss_risk,security_risk,determinism_risk,recovery_risk,requires_regression_test,requires_ci_gate,production_code,test_code,docs_only,area,critical_path,owner,decision,decision_rationale,planned_fix,validation_command,closure_proof,notes
```

## Minimal Open Row

```csv
CK-110-0001,"Pending title",external-audit,EXT-RAW-0001,,,,,,,,,v1.10.1,open,S2,cli,"Pending root invariant",,,none,none,none,none,none,true,false,false,false,false,tooling,,,investigate,,,,"Imported pending triage"
```

## Minimal Triaged Row

```csv
CK-110-0001,"Command accepts extra positional args",external-audit,EXT-RAW-0001,,,,,,,,,v1.10.1,triaged,S2,cli,"Commands must reject unexpected positional arguments",CK-110-M001,,low,none,none,medium,none,true,false,true,false,false,production,critical/cli-automation,,fix,,"Centralize arity validation","go test ./tests/cli/...",,
```

## Required Fields

Every row should eventually include:

- ck_id
- title
- source
- release_target
- status
- severity
- domain
- root_invariant
- risk fields
- area fields
- decision

Rows with accepted, deferred, suppressed, duplicate, or not-applicable decisions require rationale.

## Validation checklist

- [ ] Issue row template header matches `issue-tracker.csv`
- [ ] Minimal open row example exists
- [ ] Minimal triaged row example exists
- [ ] Required fields are listed
- [ ] Rationale requirement is documented
