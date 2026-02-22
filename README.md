# Capsule (V0)

Capsule is an experimental deduplicated storage engine prototype.

It implements:

- Chunk-based file storage
- Container-based physical layout
- SHA256 deduplication
- Transactional metadata consistency
- Concurrent safe folder ingestion
- Optional container compression

⚠️ Capsule V0 is a research prototype and not production-ready.

---

# Architecture Overview

Capsule stores files using:

1. Logical files (full-file SHA256)
2. Chunks (chunk-level SHA256)
3. Containers (physical append-only files)

Each file is:

- Split into chunks
- Deduplicated against existing chunks
- Mapped to container offsets
- Stored transactionally in PostgreSQL

Container selection uses:

SELECT ... FOR UPDATE SKIP LOCKED

This ensures safe concurrent writes during store-folder.

---

# Features (V0)

- Per-file transactional storage
- Chunk-level deduplication
- Container rotation on max size
- Optional compression on seal
- Concurrent folder ingestion
- Integrity verification on restore
- Garbage collection
- Storage statistics

---

# Limitations (V0)

- No encryption at rest
- No authenticated metadata
- No access control
- No resumable uploads
- Compression occurs during store transaction
- Restore of compressed containers requires full decompression

---

# Requirements

- Go 1.23+
- PostgreSQL 14+ (tested with 16)
- Docker (optional but recommended)

---

# Quick Start (Docker)

Start PostgreSQL:

    docker compose up -d postgres

Build Capsule:

    docker compose build

Run stats:

    docker compose run --rm app ./capsule stats

Store a folder:

    docker compose run --rm -v ./input:/input app ./capsule store-folder /input

Restore a file:

    docker compose run --rm app ./capsule restore <file_id> /output/file.txt

---

# Local Development

Set environment variables:

    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_USER=capsule
    export DB_PASSWORD=capsule
    export DB_NAME=capsule

Initialize schema:

    psql -U capsule -d capsule -f db/init.sql

Build:

    go build -o capsule ./app

Run:

    ./capsule store-folder ./input

---

# Roadmap

Planned improvements beyond V0:

- Block-based container layout
- Background container compression
- Encryption at rest
- Authenticated metadata
- Snapshot support
- Resumable uploads
- Compaction and rewrite
- Pluggable storage backends

---

# License

MIT License
