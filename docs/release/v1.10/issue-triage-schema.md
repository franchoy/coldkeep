# v1.10 Issue Triage Schema

Status: Complete  
Owner phase: Phase 4 - Issue Tracking Schema Freeze

## Purpose

This document defines the canonical tracking schema for the Coldkeep v1.10 stabilization train.

The schema is used by:

- `issue-tracker.csv`
- `remediation-matrix.csv`
- accepted-risk records
- deferred-issue records
- suppressed-finding records
- known S0/S1 review
- later v1.10.x release checklists

## Design Principle

Coldkeep v1.10 tracks two different things:

1. Raw findings.
2. Remediation work packages.

Raw findings are recorded in `issue-tracker.csv`.

Remediation work packages are recorded in `remediation-matrix.csv`.

This separation prevents scanner count or repeated audit symptoms from becoming the release plan.

The release plan is based on root invariants.

## Raw Finding vs Remediation Matrix

### Raw finding

A raw finding is one specific imported item from:

- Codacy
- external audit
- manual review
- CI proposal
- release-gate review
- maintainer observation

Examples:

- one Codacy issue ID
- one external audit bullet
- one CI script gap
- one manual review note

### Remediation matrix row

A remediation matrix row groups one or more raw findings under one root invariant.

Examples:

| Raw findings | Matrix root invariant |
|---|---|
| `search --name ""`, `search --path ""`, `search --extension ""` | Empty explicit filters must be rejected before query execution |
| `--force=false`, `--dry-run=false`, `--overwrite=false` | Boolean flags must not treat explicit false as true |
| `benchmark compare` duplicate baseline, duplicate current, missing cases | Benchmark comparison must reject non-unique or incomplete case sets |
| container filename trust in GC, recovery, rollback, restore | DB container filenames must be validated before filesystem joins |

## ID Model

### Raw issue IDs

Raw issue IDs use:

```text
CK-110-0001
CK-110-0002
CK-110-0003
```

Rules:

- prefix is always CK-110
- numeric suffix is four digits
- IDs are never reused
- deleted/import-mistake rows should be marked status/duplicate or status/not-applicable, not physically removed after publication
- raw issue IDs identify individual imported findings

### Remediation matrix IDs

Matrix IDs use:

```text
CK-110-M001
CK-110-M002
CK-110-M003
```

Rules:

- prefix is always CK-110-M
- numeric suffix is three digits
- matrix IDs identify root-invariant work packages
- one matrix row may reference many raw issue IDs

### Accepted risk IDs

Accepted risk IDs use:

```text
CK-110-RISK-001
```

### Deferred issue decision IDs

Deferred decision IDs use:

```text
CK-110-DEF-001
```

### Suppression IDs

Suppression IDs use:

```text
CK-110-SUP-001
```

### CI gap IDs

CI gap IDs use:

```text
CK-110-CI-001
```

### Toolchain/dependency IDs

Toolchain/dependency IDs use:

```text
CK-110-DEP-001
```

### Known S0/S1 candidate IDs

Known S0/S1 candidates should reference the existing raw issue ID or matrix ID.

Do not create a separate S0/S1 ID if a raw issue or matrix row already exists.

## `issue-tracker.csv` Schema

`issue-tracker.csv` records raw findings.

Each row corresponds to one imported or manually recorded finding.

## Header

```csv
ck_id,title,source,source_id,source_file,source_line,source_rule,source_category,source_subcategory,source_severity,source_tool,source_commit,release_target,status,severity,domain,root_invariant,matrix_id,duplicate_of,breaking_risk,data_loss_risk,security_risk,determinism_risk,recovery_risk,requires_regression_test,requires_ci_gate,production_code,test_code,docs_only,area,critical_path,owner,decision,decision_rationale,planned_fix,validation_command,closure_proof,notes
```

## Field definitions

| Field | Required | Description |
|---|---|---|
| `ck_id` | yes | Internal issue ID, e.g. CK-110-0001 |
| `title` | yes | Short human-readable issue title |
| `source` | yes | Source: codacy, external-audit, manual, ci-proposal, release-gate, toolchain |
| `source_id` | when available | Original scanner/audit ID |
| `source_file` | when available | Source file path from scanner/audit |
| `source_line` | when available | Source line number |
| `source_rule` | when available | Scanner rule/pattern ID |
| `source_category` | when available | Scanner category, e.g. Complexity/Security/CodeStyle |
| `source_subcategory` | when available | Scanner subcategory, e.g. SQLInjection/FileAccess |
| `source_severity` | when available | Original scanner severity |
| `source_tool` | when available | Tool name, e.g. Codacy, Lizard, Opengrep, Trivy, markdownlint |
| `source_commit` | when available | Commit SHA associated with scanner finding |
| `release_target` | yes | Target release, e.g. v1.10.1 |
| `status` | yes | Status from labels.md |
| `severity` | yes | Coldkeep severity: S0-S4 |
| `domain` | yes | Primary domain from labels.md |
| `root_invariant` | yes | Deduplication invariant |
| `matrix_id` | after matrix phase | Linked remediation matrix ID |
| `duplicate_of` | if duplicate | ck_id or matrix_id this duplicates |
| `breaking_risk` | yes | none, low, medium, high |
| `data_loss_risk` | yes | none, low, medium, high |
| `security_risk` | yes | none, low, medium, high |
| `determinism_risk` | yes | none, low, medium, high |
| `recovery_risk` | yes | none, low, medium, high |
| `requires_regression_test` | yes | true or false |
| `requires_ci_gate` | yes | true or false |
| `production_code` | yes | true or false |
| `test_code` | yes | true or false |
| `docs_only` | yes | true or false |
| `area` | yes | Area label from labels.md, compact value |
| `critical_path` | if applicable | Critical-path label from labels.md, compact value or empty |
| `owner` | optional | Owner/assignee |
| `decision` | yes | fix, suppress, accept, defer, duplicate, investigate, not-applicable |
| `decision_rationale` | required for non-fix decisions | Why the decision is safe/valid |
| `planned_fix` | if fix | Short planned fix |
| `validation_command` | if known | Command/test proving fix or classification |
| `closure_proof` | at closure | Commit/test/CI/doc proof |
| `notes` | optional | Additional context |

## `remediation-matrix.csv` Schema

`remediation-matrix.csv` records deduplicated remediation work packages.

Each row corresponds to one root invariant.

## Header

```csv
matrix_id,root_invariant,title,summary,source_issue_ids,release_target,status,severity,domain,secondary_domains,data_loss_risk,security_risk,determinism_risk,recovery_risk,affected_commands,affected_files,expected_invariant,planned_fix,required_regression_tests,required_ci_gates,acceptance_criteria,closure_proof,decision_rationale,notes
```

## Field definitions

| Field | Required | Description |
|---|---|---|
| `matrix_id` | yes | Matrix ID, e.g. CK-110-M001 |
| `root_invariant` | yes | Stable deduplication invariant |
| `title` | yes | Short work-package title |
| `summary` | yes | Brief explanation of issue family |
| `source_issue_ids` | yes | Semicolon-separated ck_id list |
| `release_target` | yes | Target v1.10.x release |
| `status` | yes | Status from labels.md |
| `severity` | yes | Highest Coldkeep severity among grouped findings unless justified |
| `domain` | yes | Primary remediation domain |
| `secondary_domains` | optional | Semicolon-separated secondary domains |
| `data_loss_risk` | yes | none, low, medium, high |
| `security_risk` | yes | none, low, medium, high |
| `determinism_risk` | yes | none, low, medium, high |
| `recovery_risk` | yes | none, low, medium, high |
| `affected_commands` | if applicable | Semicolon-separated CLI commands/subcommands |
| `affected_files` | if applicable | Semicolon-separated file paths |
| `expected_invariant` | yes | Correct invariant after remediation |
| `planned_fix` | if fix | Planned remediation approach |
| `required_regression_tests` | yes | Required regression/invariant tests |
| `required_ci_gates` | yes | Required CI gates or none |
| `acceptance_criteria` | yes | What must be true to close the matrix row |
| `closure_proof` | at closure | Commit/test/CI/doc proof |
| `decision_rationale` | required if accepted/deferred/suppressed | Written rationale |
| `notes` | optional | Additional context |

## Matrix Severity Rule

A remediation matrix row normally inherits the highest severity among its source issues.

Downgrading below the highest source severity requires a written `decision_rationale`.

Example:

- If one grouped raw issue is S1 and five are S3, the matrix row is normally S1.
- If the S1 raw issue is later proven unreachable, the matrix severity may be downgraded with rationale.

## Matrix Closure Rule

A matrix row cannot be closed only because individual scanner findings were suppressed.

Closure requires one of:

- fix with regression/CI proof
- accepted risk rationale
- deferred issue rationale
- duplicate mapping
- proven not applicable with evidence

## Allowed Values

### `source`

Allowed values:

```text
codacy
external-audit
manual
ci-proposal
release-gate
toolchain
```

### `release_target`

Allowed values:

```text
v1.10.0
v1.10.1
v1.10.2
v1.10.3
v1.10.4
v1.10.5
v1.10.6
v1.10.7
v1.10.8
v1.10.9
v1.10.10
v1.10.11
v1.10.12
v1.11+
backlog
none
```

### `status`

Allowed values:

```text
open
triaged
fixed
accepted
deferred
suppressed
duplicate
blocked
```

### `severity`

Allowed values:

```text
S0
S1
S2
S3
S4
```

### Risk fields

Applies to:

- `breaking_risk`
- `data_loss_risk`
- `security_risk`
- `determinism_risk`
- `recovery_risk`

Allowed values:

```text
none
low
medium
high
```

### Boolean fields

Applies to:

- `requires_regression_test`
- `requires_ci_gate`
- `production_code`
- `test_code`
- `docs_only`

Allowed values:

```text
true
false
```

### `decision`

Allowed values:

```text
fix
suppress
accept
defer
duplicate
investigate
not-applicable
```

### `domain`

Allowed values are the compact domain names from labels.md, without the `domain/` prefix.

Examples:

```text
cli
json
validation
security
filesystem
storage
packed-storage
gc
refcount
restore
recovery
verify
snapshot
migration
benchmark
ci
codacy
docs
dependencies
concurrency
observability
tooling
```

## Required Fields By Status

### `status/open`

Required:

- `ck_id`
- `title`
- `source`
- `status`

Allowed only during import or initial capture.

### `status/triaged`

Required:

- `ck_id`
- `title`
- `source`
- `release_target`
- `status`
- `severity`
- `domain`
- `root_invariant`
- risk fields
- `decision`

### `status/fixed`

Required:

- all `triaged` fields
- `planned_fix`
- `validation_command` or `closure_proof`
- `closure_proof`

### `status/accepted`

Required:

- all `triaged` fields
- `decision=accept`
- `decision_rationale`
- risk fields
- review/reopen condition in `notes` or accepted-risk document reference

### `status/deferred`

Required:

- all `triaged` fields
- `decision=defer`
- `decision_rationale`
- target release
- deferral target in `notes` or deferred-issues document reference

### `status/suppressed`

Required:

- source tool/rule if scanner finding
- `decision=suppress`
- `decision_rationale`
- safety explanation
- review condition

### `status/duplicate`

Required:

- `duplicate_of`
- `decision=duplicate`
- `decision_rationale`

### `status/blocked`

Required:

- blocker explanation in `notes`
- owner if known
- next decision required

## Source-Specific Import Rules

### Codacy import rules

For Codacy findings, preserve:

- `issueId` as `source_id`
- `filePath` as `source_file`
- `lineNumber` as `source_line`
- `patternInfo.id` as `source_rule`
- `patternInfo.category` as `source_category`
- `patternInfo.subCategory` as `source_subcategory` when present
- `patternInfo.severityLevel` as `source_severity`
- `toolInfo.name` as `source_tool`
- `commitInfo.sha` as `source_commit`
- `message` as part of `notes` or `title`

Initial `status` may be `open` during import.

Coldkeep severity must be assigned separately.

### External audit import rules

For external audit findings:

- create one raw issue row per distinct bullet unless it is clearly a duplicate wording of the same symptom
- preserve the command/example in `notes`
- use `source=external-audit`
- use a synthetic `source_id`, e.g. `EXT-RAW-0001`
- assign preliminary root invariant where obvious
- do not over-deduplicate until Phase 7 matrix construction

### Manual finding rules

Manual findings must include:

- short title
- source location or reproduction
- reason it belongs in v1.10
- proposed domain
- proposed severity

### CI proposal import rules

CI proposal items should be imported only if they create a concrete v1.10 task or gap.

Examples:

- filesystem fault injection missing
- critical-path coverage gates missing
- mutation testing deferred
- cross-platform validation missing
- Codacy passive policy needed

Do not import broad philosophy statements as issues.

## Validation checklist

- [ ] Codacy import rules defined
- [ ] External audit import rules defined
- [ ] Manual finding rules defined
- [ ] CI proposal import rules defined
- [ ] Codacy scanner severity is preserved separately from Coldkeep severity
- [ ] External audit command examples are preserved

## Deduplication Rules

Deduplication is based on root invariant, not identical wording.

### Root invariant definition

A root invariant is the correctness rule that must hold after remediation.

Examples:

| Finding symptoms | Root invariant |
|---|---|
| `init garbage`, `version garbage`, `verify system extra` | Commands must reject unexpected positional arguments |
| `--force=false` deletes, `--dry-run=false` dry-runs | Boolean flag values must not be interpreted as presence-only true |
| `--name ""`, `--path ""`, `--extension ""` broaden search | Explicit empty filters must be rejected |
| duplicate benchmark cases overwrite previous rows | Benchmark case identity must be unique and validated |
| DB container filename joined into filesystem path | Stored container filenames must be validated before path joins |
| JSON command emits human output and success JSON | JSON mode must emit a single machine-consumable result |

### Deduplication process

1. Import raw findings into `issue-tracker.csv`.
2. Assign each raw finding a `root_invariant`.
3. Group findings with the same or equivalent invariant.
4. Create one `remediation-matrix.csv` row per root invariant.
5. Link raw issue rows to matrix rows through `matrix_id`.
6. Mark obvious duplicate raw rows as `status/duplicate` only when they add no unique command/file/risk context.

### Deduplication caution

Do not merge findings just because they look similar.

For example:

- `restore path traversal` and `snapshot path traversal` may share validation logic but different risk surfaces.
- `JSON mixed output` in `benchmark` and `restore` may require different severity.
- `test-only file permission` and `production file permission` must not be merged without area distinction.

## Validation checklist

- [ ] Root invariant definition exists
- [ ] Deduplication examples exist
- [ ] Deduplication process exists
- [ ] Matrix linking via `matrix_id` is defined
- [ ] Caution against over-merging is documented

## Closure Proof Rules

Every closed issue or matrix row must include closure proof.

### Accepted closure proof types

| Closure proof | Use case |
|---|---|
| Commit SHA | Code or doc fix |
| Regression test name | Specific bug regression |
| CI job name | Release gate or automation enforcement |
| Validation command | Manual or local reproducible validation |
| Documentation path | Policy/schema/docs-only closure |
| Accepted-risk record | Risk intentionally accepted |
| Deferred-issue record | Issue intentionally moved |
| Suppression record | Scanner finding suppressed with rationale |

### Required proof by severity

| Severity | Minimum closure proof |
|---|---|
| S0 | Regression/adversarial/integration test plus commit, or explicit accepted-risk decision |
| S1 | Regression/integration/CI proof where practical, or explicit rationale |
| S2 | Regression, validation command, CI proof, or documented rationale |
| S3 | Commit, test, or documentation proof |
| S4 | Documentation or suppression rationale is enough |

### Critical-path proof rule

Critical-path findings should normally include at least one of:

- regression test
- adversarial test
- integration test
- CI gate
- explicit accepted-risk rationale

## Validation checklist

- [ ] Closure proof types are defined
- [ ] Closure proof by severity is defined
- [ ] S0 closure proof is strict
- [ ] S1 closure proof is strict
- [ ] Critical-path proof rule is included

## CSV Formatting Rules

### Encoding

CSV files must be UTF-8.

### Header

The header row is frozen by Phase 4.

Do not reorder columns after import begins unless a migration note is added.

### Delimiters inside fields

Use semicolons for lists inside a single CSV field.

Examples:

```text
CK-110-0001;CK-110-0002
cli;json
go test ./...;go test -race ./...
```

### Newlines

Do not use raw newlines inside fields.

Replace long explanations with short text and move detailed rationale into Markdown files when needed.

### Commas

Fields containing commas must be quoted.

Prefer avoiding commas in short titles if practical.

### Boolean values

Use lowercase:

```text
true
false
```

### Empty values

Empty value means unknown/not applicable, depending on field.

Do not use:

```text
N/A
-
?
```

unless the schema explicitly allows it.

### Stable ordering

Recommended order for imported rows:

- source type
- source file
- source line
- source ID

Recommended order for matrix rows:

- severity
- release target
- domain
- matrix ID

### Publication rule

After a CSV is used by a later phase, do not rewrite IDs just to make numbering pretty.

## Validation checklist

- [ ] UTF-8 rule defined
- [ ] Header freeze rule defined
- [ ] Semicolon list rule defined
- [ ] Newline rule defined
- [ ] Boolean lowercase rule defined
- [ ] Empty value rule defined
- [ ] Stable ordering rule defined
- [ ] ID renumbering warning included

## Local Schema Validation Snippet

The following snippet can be used manually to validate the Phase 4 CSV headers.

```bash
python - <<'PY'
from pathlib import Path
import csv

expected = {
	"docs/release/v1.10/issue-tracker.csv": [
		"ck_id","title","source","source_id","source_file","source_line",
		"source_rule","source_category","source_subcategory","source_severity",
		"source_tool","source_commit","release_target","status","severity",
		"domain","root_invariant","matrix_id","duplicate_of","breaking_risk",
		"data_loss_risk","security_risk","determinism_risk","recovery_risk",
		"requires_regression_test","requires_ci_gate","production_code",
		"test_code","docs_only","area","critical_path","owner","decision",
		"decision_rationale","planned_fix","validation_command","closure_proof",
		"notes",
	],
	"docs/release/v1.10/remediation-matrix.csv": [
		"matrix_id","root_invariant","title","summary","source_issue_ids",
		"release_target","status","severity","domain","secondary_domains",
		"data_loss_risk","security_risk","determinism_risk","recovery_risk",
		"affected_commands","affected_files","expected_invariant","planned_fix",
		"required_regression_tests","required_ci_gates","acceptance_criteria",
		"closure_proof","decision_rationale","notes",
	],
}

for path, cols in expected.items():
	p = Path(path)
	with p.open(newline="", encoding="utf-8") as f:
		reader = csv.reader(f)
		header = next(reader)
	if header != cols:
		raise SystemExit(f"{path}: header mismatch\nexpected={cols}\nactual={header}")
	print(f"{path}: OK ({len(cols)} columns)")
PY
```

## Validation checklist

- [ ] Manual validation snippet included
- [ ] Snippet validates both CSV headers
- [ ] Snippet does not require third-party dependencies
- [ ] Snippet is documentation-only
- [ ] No CI workflow is changed
