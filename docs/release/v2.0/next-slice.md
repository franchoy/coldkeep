# Next handoff after Phase 1B

Phase 1A / action 2.01 remains accepted complete at
`cc2c175549b9a185de8c79943ae5855a4aa8d44f`, tree
`c62d1d17780bb3037c4eee5e15106ebd6650d2aa`. Its report, ledger, product contract,
fixed fixture oracle and frozen v1 records remain unchanged. The active
execution label is **Phase 1B**; `current.json` keeps parent **2.03 ACTIVE**.
See the [unit result](phase1b-result.md) and [passive API](passive-inventory.md).
No unit completion qualifies full recovery or the product.

## Recommended next bounded BUILD unit

Use **GPT-6 Astra / Extra High (`gpt-6-astra`, `xhigh`)** for a complete local
recovery-copy/completion unit. Rationale: the catalog snapshot, physical closure,
key/reader custody and completion publication must bind the same cut despite
interruption. Record actual settings only if observable; no account/global
changes or paid-model contributor rule. Begin from the exact reviewed Phase 1B
identity, preserve both accepted predecessors and unrelated changes, and verify
its evidence before extending the same charter/authority. Obtain the next
explicit BUILD instruction; this handoff does not start that work.

1. Decide **explicit coordination preparation** for legacy repositories without
   `.coldkeep-control/repository.lock`. Passive acquisition deliberately refuses
   missing root/control/lock material. Preparation cannot silently move into the
   passive API, replace the persistent lock, or create another lock domain.
2. Establish a fresh owned cut. Retained inventory JSON is stale diagnostic
   material, not authority to copy after ownership ends. Include the **full
   catalog** and **every retained physical reference**, including repair journals,
   retired entries, zero-current recipes, pins, and whole physical containers.
   Keep conservative interrupted/inconsistent-state refusals; do not project or
   prune the catalog to make a copy appear complete.
3. Select supported PostgreSQL `pg_dump`/restore versions and snapshot workflow.
   Bind the dump and payload copy to one read-only catalog snapshot while writers
   and maintenance are excluded. Define ownership/privilege remapping without a
   superuser restore. The local proof used PostgreSQL 16.13; pinned hosted CI uses
   its existing PostgreSQL 16 image and has not been executed by this local run.
   SQLite remains fixture compatibility only. If selected later, use a supported
   backup API or documented fully closed procedure with relevant WAL; never
   copy only a live `.db` file or use immutable access to hide WAL.
4. Copy to a separate explicit destination with space/capacity checks and bounded
   buffers. Use an incomplete staging generation; verify lengths, checksums and
   all closure members before a versioned completion manifest/marker becomes
   eligible for import. Missing members, corrupt digests, interruption, stale
   cuts and unsupported requirements must never yield a completed generation.
   Resume requires the same validated cut or a clean restart, not old inventory.
5. Record independent **key and compatible-reader custody** and pre-identity
   legacy provenance without manufacturing persistent repository/key IDs or
   embedding raw keys by default. Byte copying on the same disk does not prove
   an independent failure domain; that classification needs explicit evidence.

## Remaining integrated action 2.03 acceptance

After copy/completion, implement constrained import into a fresh destination.
Trusted operator PostgreSQL dumps are executable backup material; never run
arbitrary uploaded SQL under ordinary service privileges. Bound member count,
expansion, paths, allocation and parser work. Reject incomplete/corrupt packages
before activation. Preserve source bytes and refuse ambiguous destinations.

Demonstrate encrypted and plaintext historical restoration with the original
installation, catalog and live repository inaccessible, using only the completed
copy plus separately held keys/readers. Run later live GC and prove the exported
history survives. Record content hashes and actual verification/fidelity scope;
retained historical restore success alone is not full admission evidence.
These integrated copying, completion, import, installation-loss restoration and
post-GC survival results are required **before schema additions**. Only their
owning instruction may authorize completing parent action 2.03.

Keep action **2.02** packaging/native-feasibility separate. Daemon shutdown and
internal writer drainage remain with their owning lifecycle action; possession
of a lifetime lease alone is insufficient. Admission epochs, token-retirement
frontiers, durable jobs, schemas, UI, adoption and product qualification retain
their existing owners. No publication, migration or deployment follows from this
local handoff.
