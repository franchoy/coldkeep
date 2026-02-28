# Security Policy

## Status

coldkeep (V0) is a research prototype and proof-of-concept storage
engine.

It is **not production-ready** and must not be used for sensitive,
regulated, or critical data.

The security model described below reflects the current prototype
capabilities and limitations.

------------------------------------------------------------------------

## Threat Model (V0)

coldkeep V0 is designed to protect against:

-   Accidental disk corruption
-   Partial container corruption
-   Incorrect chunk offsets
-   Basic concurrent write corruption within the database

coldkeep V0 is **not designed to protect against**:

-   Malicious actors with filesystem access
-   Malicious actors with database access
-   Coordinated tampering of containers and metadata
-   Insider threats
-   Host-level compromise

------------------------------------------------------------------------

## Security Properties Provided

### 1. Content Integrity (Data Plane)

Each chunk is stored with a SHA-256 hash.

Each logical file stores a SHA-256 hash of the complete file.

During restore:

-   Chunk payload is verified against its stored SHA-256 hash.
-   The fully reconstructed file is verified against the logical file
    SHA-256.

This protects against:

-   Silent disk corruption
-   Bit rot
-   Partial container damage
-   Offset misalignment during restore

------------------------------------------------------------------------

### 2. Transactional Metadata Consistency

Each file store operation runs inside a PostgreSQL transaction.

This ensures:

-   Atomic logical file creation
-   Consistent chunk reference updates
-   Deterministic chunk ordering
-   No partial metadata visibility

However:

Filesystem writes cannot be rolled back if a database transaction fails.
This may leave orphan container files on disk.

------------------------------------------------------------------------

### 3. Concurrency Controls

coldkeep V0 uses:

-   PostgreSQL transactions per stored file
-   `SELECT ... FOR UPDATE SKIP LOCKED` for container selection
-   Row-level locking for container size updates

This ensures:

-   Only one transaction appends to a specific open container at a time
-   Container size tracking remains consistent
-   Container layout is not interleaved

Concurrency guarantees are minimal and not stress-tested under heavy
parallel workloads.

------------------------------------------------------------------------

## Security Properties NOT Provided

coldkeep V0 does NOT provide:

-   Encryption at rest
-   Encryption in transit
-   Authentication
-   Authorization
-   Multi-tenant isolation
-   Tamper-evident metadata
-   Cryptographic signatures
-   Secure deletion guarantees
-   Formal integrity proofs
-   Merkle-tree validation
-   Forward secrecy
-   Key management

If an attacker can modify both container files and the database,
integrity checks can be bypassed.

------------------------------------------------------------------------

## Known Security Limitations

-   Crash consistency is incomplete.
-   Filesystem state and database state may temporarily diverge.
-   Container compression (if enabled) reduces random access safety.
-   Orphan bytes may accumulate inside containers under concurrency.
-   There is no background integrity verification process.

------------------------------------------------------------------------

## Recommended Usage

coldkeep V0 should only be used:

-   For experimentation
-   For academic exploration
-   For format design discussions
-   With disposable test data

It must not be used in:

-   Production systems
-   Regulated environments
-   Systems requiring strong security guarantees

------------------------------------------------------------------------

## Future Security Improvements (Roadmap)

Potential future improvements include:

-   Encrypted containers (AES-GCM or similar)
-   Authenticated metadata structures
-   Merkle-tree based verification
-   Framed container format with integrity boundaries
-   Background scrubbing / verification process
-   Access control and multi-user isolation
-   Secure deletion and shredding semantics
-   Cloud backend with integrity proofs

------------------------------------------------------------------------

## Reporting Security Issues

If you discover a potential vulnerability:

1.  Do NOT open a public GitHub issue immediately.
2.  Contact the repository maintainer directly.
3.  Provide a detailed description, reproduction steps, and impact
    analysis.

Because this is a prototype, response times and remediation guarantees
are not formally defined.