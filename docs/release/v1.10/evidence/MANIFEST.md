# v1.10 Evidence Manifest

Status: Complete  
Owner phase: Phase 2 - Source Inventory & Evidence Freeze

## Purpose

This manifest records the frozen source material used to build the Coldkeep v1.10.0 baseline.

The evidence files in this directory are not remediation outputs. They are the input material used by later phases to create:

- issue inventory
- Codacy baseline
- external audit inventory
- severity classification
- remediation matrix
- CI baseline
- release gates
- S0/S1 candidate list

## Evidence Policy

Evidence files are copied as baseline snapshots and should not be edited in place.

If a file needs to be summarized, normalized, or classified, that derived work must be written to the appropriate v1.10 document outside this evidence directory.

Examples:

- Codacy classification belongs in `../codacy-baseline.md`
- external audit grouping belongs in `../external-audit-inventory.md`
- raw issue rows belong in `../issue-tracker.csv`
- root-invariant grouping belongs in `../remediation-matrix.csv`

## Manifest

| Frozen file | Original source filename | Class | Used for | Authority | Notes |
|---|---|---|---|---|---|
| `coldkeep_v1x_detailed_roadmap_updated_v1_10.baseline.docx` | `coldkeep_v1x_detailed_roadmap_updated_v1_10.docx` | baseline-plan | v1.x strategy, v1.10 release train boundaries, v1.11 deferral | Authoritative planning input | Establishes that v1.10 is a stabilization train before engine extraction |
| `coldkeep_codacy_all_issues.baseline.json` | `coldkeep_codacy_all_issues.json` | raw-evidence | Codacy issue import, scanner baseline, dependency/security/complexity surfacing | Raw scanner input | Must be classified; raw severity is not automatically Coldkeep severity |
| `third_party_coldkeep_v1.9.0_code_analysis.baseline.txt` | `third_party_codlkeep_v1.9.0_code_analysis.txt` | raw-evidence | External audit issue families, CLI/JSON/validation/restore/benchmark findings | Advisory audit input | Must be deduplicated by root invariant |
| `COLDKEEP_V1_10_RELEASE_CHAIN.baseline.md` | `COLDKEEP_V1_10_RELEASE_CHAIN.md` | baseline-plan | v1.10 process, severity model, release process, v1.10.0 scope | Authoritative release-planning input | Defines baseline/freeze purpose and later v1.10.x scopes |
| `ci_evolution_proposal.baseline.txt` | `ci_evolution_proposal.txt` | baseline-plan | CI strategy, Codacy policy, coverage/fault-injection direction | Authoritative CI-planning input | Codacy is passive/constrained; CI prioritizes correctness over style |

## Checksums

Checksums are recorded in:

```text
SHA256SUMS
```

To verify:

```bash
cd docs/release/v1.10/evidence
sha256sum -c SHA256SUMS
```

On macOS:

```bash
cd docs/release/v1.10/evidence
shasum -a 256 -c SHA256SUMS
```

## Derived Documents

The following files will be created or completed in later phases from this evidence:

| Derived file | Phase | Source evidence |
|---|---:|---|
| `../labels.md` | 3 | release-chain, roadmap |
| `../issue-triage-schema.md` | 4 | release-chain |
| `../codacy-baseline.md` | 5 | Codacy JSON |
| `../issue-tracker.csv` | 5-6 | Codacy JSON, external audit |
| `../external-audit-inventory.md` | 6 | external audit text |
| `../remediation-matrix.csv` | 7 | issue tracker, external audit, Codacy |
| `../ci-baseline.md` | 8 | current repository CI, CI proposal |
| `../release-gates.md` | 9 | release-chain, CI proposal |
| `../toolchain-vulnerability-plan.md` | 10 | Codacy JSON, go.mod, CI |
| `../codacy-policy.md` | 11 | CI proposal, Codacy findings |
| `../known-s0-s1.md` | 12 | remediation matrix, audit findings |

## Completeness Statement

Phase 2 is complete when all baseline evidence files are present, checksummed, and recorded in this manifest.
