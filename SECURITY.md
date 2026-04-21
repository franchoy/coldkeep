# Security Policy

## Status

coldkeep v1.4 is a correctness-first storage engine focused on
data integrity, deterministic restore, and explicit operator-visible
failure handling.

It is suitable for controlled production environments where:

- operational assumptions are respected (PostgreSQL correctness,
    filesystem durability)
- storage and database are not externally modified
- operators follow recovery and verification practices

coldkeep is **not** a hardened security system and does not provide
protection against malicious actors with system-level access.

Security posture summary:

- coldkeep is integrity-first, not security-first
- it is designed to detect and recover from many non-malicious corruption and lifecycle failures
- it does not attempt to defend against an attacker who can change both the database and on-disk containers

The security model described below reflects current capabilities,
assumptions, and limits.

------------------------------------------------------------------------

## Threat Model

coldkeep is designed to protect against:

- Accidental disk corruption
- Partial container corruption
- Incorrect chunk offsets
- Basic in-process concurrent write corruption within the database
- Lifecycle drift that can be detected and corrected by recovery or verification

coldkeep is **not designed to protect against**:

- Malicious actors with filesystem access
- Malicious actors with database access
- Coordinated tampering of containers and metadata
- Insider threats
- Host-level compromise

------------------------------------------------------------------------

## Security Properties Provided

### 1. Content Integrity (Data Plane)

Each chunk is stored with a SHA-256 hash.

Each logical file stores a SHA-256 hash of the complete file.

During restore:

- Chunk payload is verified against its stored SHA-256 hash.
- The fully reconstructed file is verified against the logical file
    SHA-256.

This protects against:

- Silent disk corruption
- Bit rot
- Partial container damage
- Offset misalignment during restore

------------------------------------------------------------------------

### 2. Transactional Metadata Consistency

Each file store operation runs inside a PostgreSQL transaction.

This ensures:

- Atomic logical file creation
- Consistent chunk reference updates
- Deterministic chunk ordering
- No partial metadata visibility

However:

Filesystem writes cannot be rolled back if a database transaction fails.
This may leave orphan container files on disk.

------------------------------------------------------------------------

### 3. Concurrency Controls

coldkeep uses:

- PostgreSQL transactions per stored file
- `SELECT ... FOR UPDATE SKIP LOCKED` for container selection
- Row-level locking for container size updates

This ensures:

- Only one transaction appends to a specific open container at a time
- Container size tracking remains consistent
- Container layout is not interleaved

These guarantees apply to the documented single-node, in-process model.
They are not a distributed locking or multi-writer coordination system.

------------------------------------------------------------------------

### 4. Optional Encryption At Rest

coldkeep supports an `aes-gcm` codec for block payload encryption.

When enabled with a valid `COLDKEEP_KEY`, this provides:

- confidentiality for stored block payloads at rest
- authenticated decryption failure on ciphertext or nonce tampering

It does not provide:

- automatic key generation, escrow, or rotation
- protection for plaintext filenames, operational metadata, or database access patterns
- protection against a host-level attacker who can read process memory or environment variables

------------------------------------------------------------------------

## Security Properties NOT Provided

coldkeep does NOT provide:

- Mandatory encryption at rest for all deployments
- Encryption in transit
- Authentication
- Authorization
- Multi-tenant isolation
- Secret management or key escrow
- Tamper-evident metadata
- Cryptographic signatures
- Secure deletion guarantees
- Formal integrity proofs
- Merkle-tree validation
- Forward secrecy
- Key management

If an attacker can modify both container files and the database,
integrity checks can be bypassed.

------------------------------------------------------------------------

## Known Security Limitations

- Crash consistency is incomplete.
- Filesystem state and database state may temporarily diverge.
- Legacy whole-container compression artifacts may still exist in older
    datasets and can reduce random-access safety for those historical data
    paths.
- Orphan bytes may accumulate inside containers under concurrency.
- There is no background integrity verification process.
- Snapshot and retention metadata are correctness features, not tamper-proof audit logs.
- Encryption keys are operator-managed; loss of key material means encrypted data cannot be recovered.

------------------------------------------------------------------------

## Recommended Usage

coldkeep is suitable for controlled production environments where the
documented trust boundary and operational assumptions are respected.

For higher-assurance or adversarial environments, use additional
external controls (host hardening, access controls, key-management
policy, and monitoring) or a system designed for stronger built-in
security guarantees.

At minimum, deploy coldkeep with:

- restricted database and filesystem access
- encrypted backups of key material when `aes-gcm` is enabled
- routine `doctor` / `verify` execution as part of operational hygiene
- monitoring for unexpected container-file or database changes outside coldkeep

------------------------------------------------------------------------

## Future Security Improvements (Roadmap)

Potential future improvements include:

- Key rotation and stronger key lifecycle management
- Authenticated metadata structures
- Merkle-tree based verification
- Framed container format with integrity boundaries
- Background scrubbing / verification process
- Access control and multi-user isolation
- Secure deletion and shredding semantics
- Cloud backend with integrity proofs

------------------------------------------------------------------------

## Reporting Security Issues

If you discover a potential vulnerability:

1. Do NOT open a public GitHub issue immediately.
1. Contact the repository maintainer directly or use a private GitHub security reporting channel if one is available for the repository.
1. Provide a detailed description, reproduction steps, and impact
   analysis.

Response times and remediation guarantees are not formally defined.
