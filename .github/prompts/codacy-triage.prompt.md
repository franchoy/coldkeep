# Coldkeep Codacy Triage Prompt

Classify Codacy findings as:

- real correctness/security risk;
- dependency vulnerability;
- test-only false positive;
- style-only noise;
- complexity hotspot worth tracking;
- duplicate of existing issue.

Do not recommend fixing style-only findings during v1.10 unless they block correctness or CI integrity.

For each finding:
- severity;
- domain;
- production vs test;
- data-loss risk;
- security risk;
- requires regression test;
- suppress/fix/defer/accept;
- rationale.