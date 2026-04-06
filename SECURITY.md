# Security Policy

## Status

coldkeep v1.0 provides a correctness-first storage core focused on
data integrity and deterministic restore.

It is suitable for controlled production environments where:

-   operational assumptions are respected (PostgreSQL correctness,
    filesystem durability)
-   storage and database are not externally modified
-   operators follow recovery and verification practices

coldkeep is **not** a hardened security system and does not provide
protection against malicious actors with system-level access.

The security model described below reflects current capabilities,
assumptions, and limits.

------------------------------------------------------------------------

## Threat Model

coldkeep is designed to protect against:

-   Accidental disk corruption
-   Partial container corruption
-   Incorrect chunk offsets
-   Basic concurrent write corruption within the database

coldkeep is **not designed to protect against**:

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

coldkeep uses:

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

coldkeep does NOT provide:

-   Mandatory encryption at rest for all deployments
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
-   Legacy whole-container compression artifacts may still exist in older
    datasets and can reduce random-access safety for those historical data
    paths.
-   Orphan bytes may accumulate inside containers under concurrency.
-   There is no background integrity verification process.

------------------------------------------------------------------------

## Recommended Usage

coldkeep is suitable for controlled production environments where the
documented trust boundary and operational assumptions are respected.

For higher-assurance or adversarial environments, use additional
external controls (host hardening, access controls, key-management
policy, and monitoring) or a system designed for stronger built-in
security guarantees.

------------------------------------------------------------------------

## Future Security Improvements (Roadmap)

Potential future improvements include:

-   Key rotation and stronger key lifecycle management
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

Response times and remediation guarantees are not formally defined.