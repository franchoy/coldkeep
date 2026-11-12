# Frozen Phase 3C2 qualification expectations

Frozen before the substantive qualification checkpoint. Required identities are
literal sets in `scripts/check_recovery_test_events.py` and
`scripts/check_passive_test_events.py`; missing, skipped or failed identities
and nonzero children are blocking. Recovery and both whole-codec routes require
856 tests/subtests plus four package passes: all accepted 722, six fixture cases
and 128 C2 cases. Passive requires all accepted 49 plus six fixture cases and
two package passes. Tests bind the exact retained C1 identity-set digest.

| Boundary | Expected qualifying behavior | Required rejection/failure behavior |
| --- | --- | --- |
| Owned reads | Exact source/generation/root/policy identities and outcomes; byte-sorted complete enumeration; repeatable first page; limit1/default100 maximum; root/nonroot children at exact counts 0/1/100/101/200/201; empty distinct from missing | Foreign/unknown scope and entry, invalid limit; RLS/policy-admission changes, invalid index, missing control; no consumer or new control on failed admission |
| Snapshot/cursor | Late transaction below upper bound absent in current view, visible in next; above/below-bound child inserts absent; exact returned cursor traversal | Changed watermark/last entry/query/source/parent; unknown version/order, malformed key; expired/cross-view reader; simultaneous calls |
| Lifetime | Rollback/driver close joined under lease, callback-return/page race joined, exact installed descriptor retained | Cancellation, callback failure, driver-close error, descriptor substitution, conflicting intent, lost snapshot; no premature outer Complete |
| Native names/DTO | Case, spaces, NFC/NFD, slash-free backslash/newline, literal replacement character and invalid UTF8 bytes; 255-byte components, 128-depth/3072-byte paths; disabled/unavailable-source history | Invalid raw JSON UTF8/surrogates, duplicate/unknown/aliased keys, traversal, noncanonical base64, count/byte overruns, trailing input; caller-lowered rows/bytes/deadline; oversized database field; exact caller response ceiling and single-item oversize refusal |
| Int64/nullable | Canonical decimal strings around 2^53 and signed limits through Go and real pinned Node parser; unknown distinct from zero | JSON number, null-as-decimal, overflow, signs/leading zeros/exponents/escaped digits |
| Physical index | Fresh checkpoint, exact nonunique source index, idempotent retry, unchanged logical/payload/binding identity | Wrong column order/relation/name type/collation/opclass/sort/nulls/uniqueness/predicate/include; invalid state, wrong root/repository/revision, pending intent/imported target; DDL cuts and lost Commit response |
| Recovery | Indexed/unindexed plaintext/encrypted schema20 packages through untouched C1 and current operators; generation-only payload and isolated historical restores | Imported-target maintenance; all retained profile/writer/reclamation fences |
| Query work | Actual parameterized EXPLAIN ANALYZE BUFFERS, ordinary planner, first/middle/final source/root/nonroot pages and exact selectors at 100/1000/5000 selected+distractor records | Largest-scope nodes exceeding bounded page work; per-page allocation scaling with unrelated catalog; no forced planner, OFFSET or Inventory slicing |
| Fixture | Explicit tmpfs over declared image volume; exact ID/owner inspection and absence after success/early exit | Unexpected persistent mount, foreign owner, failed cleanup, residual volume or missing evidence |
| Staging | Ten verified roles retained for entire Go child; complete C1 blobs/modes/compiler/binary provenance before and after | Missing/wrong/changed C1 source, mode, compiler or executable; changed reader during child; all historical identities retained |
| Integrated authority | Exact accepted reports/bridge, owning C2 result and input-bound qualification record clear only local2.04 | Missing/stale/conflicting result, changed/new executable input or mode, C1-only/old reports, product/release/future action claims |

Fresh final-source routes: focused race; dedicated recovery/passive; full plain
and AES-GCM; current and D19 23-case oracles (compiler-bound reuse only if proven);
full Python and Go CI-enforcement; build/vet/pinned lint; dependency/static and
CI mutation audit; integrated native/browser/Docker/package/install proof; current
reachable-vulnerability scan. Dedicated package15m, complete package35m and
whole child2400s ceilings remain unchanged. Heavy routes run sequentially on
separate disposable fixtures. Failed attempts remain evidence, never partial passes.

Exact independent outgoing review, guarded development push, bounded draft PR
body refresh and both actual-head CI/CodeQL events plus findings/latest Codacy
remain separate acceptance gates. Local completion alone does not satisfy them.
