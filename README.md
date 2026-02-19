# Capsule POC (V0)

Capsule is a normalized, deduplicated incremental backup engine written
in Go.

This repository contains a **V0 alpha prototype** designed for
experimentation, architectural validation, and feedback. It is **not
production-ready**.

------------------------------------------------------------------------

## 🚀 Features (V0)

-   Content-Defined Chunking (CDC)
-   SHA-256 based chunk deduplication
-   Append-only container storage
-   Container-level compression (Zstandard)
-   Reference-count-based garbage collection
-   Restore by logical file ID
-   Integrity verification during restore
-   Configurable storage directory via environment variable

------------------------------------------------------------------------

## 🏗 Architecture Overview

Logical File\
→ Ordered list of chunks\
→ Chunks stored in containers\
→ Containers sealed and compressed when full

### Container Record Format

Each chunk inside a container is stored as:

\[32 bytes SHA256\]\[4 bytes chunk_size\]\[chunk_data\]

Containers are compressed only after sealing.

------------------------------------------------------------------------

## 🗄 Database Schema (PostgreSQL)

Main tables:

-   `logical_file`
-   `chunk`
-   `file_chunk`
-   `container`

### logical_file

  Column          Type
  --------------- -----------
  id              BIGSERIAL
  original_name   TEXT
  total_size      BIGINT
  file_hash       TEXT
  created_at      TIMESTAMP

### chunk

  Column         Type
  -------------- ---------------
  id             BIGSERIAL
  sha256         TEXT (unique)
  size           INTEGER
  container_id   BIGINT
  chunk_offset   BIGINT
  ref_count      BIGINT

### file_chunk

  Column            Type
  ----------------- ---------
  logical_file_id   BIGINT
  chunk_id          BIGINT
  chunk_order       INTEGER

### container

  Column                  Type
  ----------------------- -----------
  id                      BIGSERIAL
  filename                TEXT
  current_size            BIGINT
  max_size                BIGINT
  sealed                  BOOLEAN
  compression_algorithm   TEXT
  compressed_size         BIGINT

------------------------------------------------------------------------

## 🛠 Requirements

-   Go 1.24+
-   PostgreSQL 14+

------------------------------------------------------------------------

## ⚙️ Configuration

Environment variables:

DB_HOST\
DB_PORT\
DB_USER\
DB_PASSWORD\
DB_NAME\
CAPSULE_STORAGE_DIR (optional, default: ./storage/containers)

------------------------------------------------------------------------

## 🐳 Quickstart (Docker)

Start PostgreSQL:

docker-compose up -d

Initialize schema:

psql -h localhost -U capsule -d capsule -f db/init.sql

Build the CLI:

go build -o capsule ./app

------------------------------------------------------------------------

## 📥 Usage

### Store a file

./capsule store myfile.bin

### Store a folder

./capsule store-folder ./myfolder

### Restore a file

Restore by logical file ID into a directory:

./capsule restore 12 ./restored

Result:

./restored/`<original_filename>`{=html}

Integrity of each chunk is verified during restore.

### Remove a file

./capsule remove 12

### Run garbage collection

./capsule gc

Deletes unreferenced chunks and empty containers.

### Show statistics

./capsule stats

------------------------------------------------------------------------

## ⚠️ Limitations (V0)

-   Restore loads entire container into memory
-   No encryption layer
-   No multi-tenant support
-   No container header validation
-   No streaming restore
-   No distributed locking
-   Not production-ready

------------------------------------------------------------------------

## 🛣 Roadmap (V1+)

-   Streaming restore
-   Encryption support
-   Snapshot/versioning
-   Container header validation
-   Multi-tenant support
-   Remote storage backends
-   Erasure coding

------------------------------------------------------------------------

## 📄 License

Apache License 2.0

------------------------------------------------------------------------

## 🤝 Contributing

This is an experimental storage engine prototype.\
Feedback, issues, and architectural discussions are welcome.

------------------------------------------------------------------------

## 🎯 Project Status

Capsule POC V0 is a working alpha prototype intended for experimentation
and architectural validation.
