# Security Policy

## Overview

coldkeep (V0) is an experimental storage engine prototype designed for:

- Deduplicated storage
- Chunk-based container layout
- Transactional metadata consistency
- Integrity verification on restore

---

## Security Model (V0)

coldkeep V0 provides:

- Per-file transactional metadata (atomic store)
- Chunk-level SHA256 hashing
- Full-file SHA256 hashing
- Integrity verification during restore
- Database row-level locking for concurrent safety

coldkeep V0 does NOT provide:

- Encryption at rest
- Authentication or authorization
- Cryptographic integrity protection of metadata
- Tamper-evident storage

---

## Data Integrity

Each chunk is stored with a SHA256 hash.
Each logical file is stored with a SHA256 hash of the full file.

During restore:

- Chunk payload is verified against stored chunk SHA256.
- Full restored file is verified against logical file SHA256.

This protects against:

- Accidental disk corruption
- Partial container corruption
- Incorrect chunk offsets

It does NOT protect against:

- Malicious database tampering
- Coordinated modification of container files and metadata
- Insider threats

---

## Concurrency Model

coldkeep V0 supports concurrent store-folder operations.

Concurrency safety is achieved using:

- PostgreSQL transactions per stored file
- SELECT ... FOR UPDATE SKIP LOCKED for container selection
- Row-level locking for container size updates

This ensures:

- Only one transaction appends to a specific open container at a time
- Container size tracking remains consistent
- No interleaved writes corrupt container layout

---

## Known Limitations

coldkeep V0 should not be used in production environments handling sensitive or regulated data.

Future security improvements may include:

- Encrypted containers
- Authenticated metadata
- Merkle-tree based integrity model
- Access control layer
- Background integrity verification

---

## Reporting Security Issues

If you discover a vulnerability:

Do not open a public issue immediately.

Instead, contact the repository maintainer directly.
