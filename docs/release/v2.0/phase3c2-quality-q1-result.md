# C2-Q1 local source-quality qualification

Phase 3C2: COMPLETE
Phase 3C: COMPLETE
Local-profile action 2.04: COMPLETE
Quality unit C2-Q1: COMPLETE
Final Phase 3C2 acceptance: WITHHELD_CODACY
C2-G1: ACCEPTED (preserved historical work)
Implementation: ACTIVE
Product gates: INCOMPLETE
Profile: installed Linux/PostgreSQL16 schema20
Governing BUILD: coldkeep_phase3c2_quality_q1_build_2026-11-14.md
Accepted C1: 06bde38380dd3a42be5c180beb99bf1aa509b8ac
Qualification HEAD: 2e5fa105fb8eaca197c393612c3bffdd7ebe5420
Qualification tree: a05e9b05fde805ec023d1c999d8c661ca88b84c1
Qualification SHA-256: 2c19f301477374a74603f7620c57bf4a0eaae78a449ead9ab07b10d8b86ffa96
Ordinary schema18/19/20 writers and reclamation: REFUSED
IMPORT_READY maintenance/activation: REFUSED
Action 2.05: NOT IMPLEMENTED

These COMPLETE fields describe the integrated local profile. They do not clear
final C2 acceptance, hosted quality or product gates. Independent review of the
exact outgoing successor and both actual-head hosted observations follow this
record; their identities and outcomes belong to the external final receipt.

Requested model/effort: GPT-6 Astra / Extra High. Actual parent variant and
effort are not observable and remain unknown; they are not inferred from the
request. Independent review uses the same requested settings.

## Source boundary and correction

Entry HEAD: `98574469514767b769687cb383cf2fd13456943d`.
Entry tree: `58f01386571f8d0ef6a6965f552985d65dd831e5`.
Execution-input SHA-256: `f571b285090901da02695290ed468c68c76ad191d3a86dca3640b20ba283ba38`.
Original C2 and G1 results/qualifications remain byte-identical, bound as hashed
predecessors. Missing, malformed or stale Q1 records cannot fall back to G1.
All prior runtime CHECKS and byte/mode/tracked-addition coverage remain intact.
New quality checks bind the documentation review and generation measurements.

The supplied corrected documentation patch targets nine Markdown whitespace
findings and two BUILD/PLAN clarity findings. It removes 12 redundant blank
lines across nine files; two ancillary whitespace runs are not additional
current findings. All four intentional safety/scope rules remain intact.

Generation validation now separates request/response query variants, scope and
order checks, policy/selector admission, object/array walking and Unicode escape
scanning. The model and DTO token walkers remain separate, with their original
depth, count, key, string and byte limits, UseNumber behavior and typed decoding
order. Public signatures, types, field order/tags, constants, canonical bytes,
error identity, nil/empty distinctions and short-circuit order are preserved.
The operator-config decoder, other source families, SQL, protocols, pins,
workflows, staging budgets and product fences are unchanged.

## Target findings

Every target below is locally corrected and qualified. This table does not
claim a Codacy Fixed transition. Provider dispositions require the new hosted
inventory; local metric reduction alone does not establish such a transition.

| Entry issue identity | Entry location | Local correction/measurement |
| --- | --- | --- |
| `30e1eefeb350d84bb97c25b9ef55d9c6` | `PRE_RELEASE_CHECKLIST.md:22` | Corrected patch verified |
| `29b3ed93d1b1564849dd4f95ffe37cd0` | `COMPATIBILITY.md:13` | Corrected patch verified |
| `f4f4ede730301b6d5b93086ff4246d68` | `SECURITY.md:10` | Corrected patch verified |
| `b20d0c1e0a890dd9ac59a4c6e9c6e6d` | `.github/copilot-instructions.md:32` | Corrected patch verified |
| `f19bc178dfbeed8e576aa5745eb6db7b` | `.github/instructions/ci.instructions.md:36` | Corrected patch verified |
| `1ba74e2c870fb98f20215a5515f71a88` | `VALIDATION_MATRIX.md:13` | Corrected patch verified |
| `68aa29c0b1b202fa457bef27b1c87844` | `README.md:49` | Corrected patch verified |
| `70d6ff4faa371dd05dbfcb486abdc4e8` | `.github/prompts/critical-path-coverage.prompt.md:32` | Corrected patch verified |
| `caea93d8a6889b1c4926241d54c2251e` | `.github/prompts/regression-fix.prompt.md:32` | Corrected patch verified |
| `ebdc742512b34ddb39a7f828c2c781ee` | `internal/generation/json.go:86` | CCN 16→7; NLOC 33→22 |
| `4b10b15d71d473a4926d1895de3bb532` | `internal/generation/model.go:43` | CCN 11→8; NLOC 25→22 |
| `e87e3187cad08c72ec8ab217055177d` | `internal/generation/json.go:18` | CCN 16→5; NLOC 53→18 |
| `60ef51ba733acd342b90bcaf563fbc81` | `internal/generation/read.go:228` | CCN 59→7; NLOC 63→16 |
| `c3590e2f01a777d7b0bb3abf4fbeedc3` | `internal/generation/read.go:72` | CCN 35→6; NLOC 49→15 |
| `4937a06c0544daae1f6798635b94d0c3` | `internal/generation/read.go:221` | CCN 12→7; NLOC 6→4 |
| `fb6cf718472a94a95f4e3bcf00461d86` | `internal/generation/model.go:76` | CCN 18→5; NLOC 23→13 |
| `cffc5a180f910efc08ec9a5e74f885e5` | `internal/generation/read.go:187` | CCN 14→5; NLOC 5→4 |
| `37a70e6ddf1a2ebc3e9e232d7ac0e00c` | `internal/generation/read_test.go:99` | CCN 10→8; NLOC 27→20 |
| `81877ae9b0be349cf8a074676300a5c7` | `internal/generation/read_test.go:145` | CCN 10→6; NLOC 27→20 |
| `70a062763d48aacef4f0ad7ec12e77c7` | `internal/generation/read.go:228` | CCN 59→7; NLOC 63→16 |
| `7b8479e8f2a9b885ebb0d9f20a6b1d92` | `internal/generation/model_test.go:85` | CCN 9→5; NLOC 22→18 |
| `97f57189b67172ad9b32cb7c7c82361a` | `internal/generation/read.go:333` | CCN 20→8; NLOC 52→23 |
| `a7b638c552265f9163f6b1dad7239002` | `internal/generation/json.go:18` | CCN 16→5; NLOC 53→18 |
| `bb13925f9855ac71b451a27fe489d0ae` | `internal/generation/read.go:421` | CCN 20→6; NLOC 11→9 |
| `e841e221fe5001d3c27569fdd9b2f5c4` | `internal/generation/read.go:333` | CCN 20→8; NLOC 52→23 |
| `8aca81d45fed6c6dff3b44e6a913e7fd` | `internal/generation/read_test.go:66` | CCN 9→5; NLOC 48→33 |
| `4ffcc4547d3cfcb114c4e54b6ba627aa` | `internal/generation/read_test.go:178` | CCN 9→5; NLOC 30→12 |
| `d8c0e889322acec513caa60d254bc3a2` | `AGENTS.md:81` | Corrected patch verified |
| `7a2eba87da24bd9bef7bf2618575b7a8` | `AGENTS.md:57` | Corrected patch verified |

Pinned private Lizard 1.24.0 measured all 100 generation
functions, including new helpers: none exceeds complexity 8 or NLOC 50.
The local parser includes the preceding variable declaration in the two old
walker closures: NLOC 53/52 locally versus 52/51 in Codacy. Complexity matches.
The raw before/after measurements retain this wrapper difference.

## Fresh qualification

Every route below executed at the clean implementation checkpoint. No runtime
PASS was transplanted from the predecessor or inferred from an older hosted
run. D19 was rebuilt and executed afresh. Result-only completion preserves the
same execution-input digest. Database routes ran sequentially on separate owned
tmpfs PostgreSQL16 servers; independent non-database checks ran alongside them.
Timing observations do not establish universal latency or capacity guarantees.

| Route | Disposition | Elapsed seconds |
| --- | --- | --- |
| generation | PASS, fresh | 1.57 |
| recovery | PASS, fresh | 741.83 |
| passive | PASS, fresh | 374.38 |
| plain | PASS, fresh | 973.17 |
| aes | PASS, fresh | 931.62 |
| phase2 | PASS, fresh | 54.97 |
| python | PASS, fresh | 836.29 |
| go-ci | PASS, fresh | 684.41 |
| current-oracle | PASS, fresh | 3.01 |
| d19-oracle | PASS, fresh | 6.72 |
| static | PASS, fresh | 19.91 |
| vulnerability | PASS, fresh | 3.13 |
| staged-missing | PASS, fresh | 16.73 |
| staged-null | PASS, fresh | 17.20 |

Recovery retains all 866 entry named requirements and adds two meaningful
helper-boundary cases: 868 required tests/subtests plus four package passes.
Passive retains 55 plus two. Each complete codec route enforces 892 named
requirements plus four package passes. No missing, failed or skipped required
identity satisfies a gate. Optional/platform skips are listed in the external
raw event audits and compared with the accepted entry boundary.

The exact Node v24.22.2 consumer executed in the generation and staged routes.
All retained tests, subtest names, fixed expectations and malformed-input cases
remain. Added Unicode and nil-pointer/selection/rule tests address risks from
helper extraction and are mandatory in recovery and both complete codecs.
Both current and D19 readers matched all 23 fixed product-fixture outcomes.
The full Python suite passed 674 tests, including stale-G1 refusal,
qualification input mutations and missing/skipped/failed mandatory-event checks.
Real staged missing/null digest controls refused before starting a Go test child.
Build, vet, pinned lint, dependency boundaries, CI audit/enforcement, integrated
native/browser/Docker/package proof and reachable-vulnerability scan passed.

A separate bounded differential probe compared 10,037 inputs across six entry
and successor operations: 60,222 comparisons matched, including partial decoded
values and ErrInvalid identity. This is supporting evidence, not an exhaustive
proof. The invariant review and complete applicable routes remain necessary.

## Preserved limitations and next gate

The entry provider baseline is 98 Added findings and six Fixed records. Outside
Q1 are 30 other Lizard, 18 SQL applicability and 21 contextual findings. These
are entry counts, not a promised current total or an assumed 98-to-69 result.
The historical disappearance of 91 TSQLLint findings happened at unchanged
source and remains separate; it is not a source fix. ShellCheck exit 137 and
Opengrep's workflow matching error remain recorded analyzer coverage limits
until new logs establish their current state. No settings, ignores, suppression,
threshold changes, extra native reanalysis or provider messages were used.

Main, stable v1.13.19, draft PR #129, predecessor worktrees and sealed evidence
remain protected. No Windows native-byte runtime, broader delivery support,
hardware/media authenticity, physical failure-domain independence, real
reboot/unmount, cross-filesystem transfer or live schema20 GC survival is claimed.
No merge, release, product publication, ordinary writer activation or action
2.05 implementation follows from this local qualification.

External evidence root: `phase3c2-quality-q1-evidence-2026-11-14-c5rdhg0w`. It retains all raw commands, source
identities, tool versions, complete event/provenance/cleanup audits, per-finding
CSV/JSON and before/after metrics. Final outgoing patches, independent review,
guarded-publication receipt, hosted readback, preservation replay and archive
checksums are finalized there after this source record.
