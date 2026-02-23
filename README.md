# Capsule

![CI](https://github.com/franchoy/capsule_poc/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20prototype-orange)

Capsule is an experimental container-based storage prototype written in Go.
Capsule is an experimental container-based storage prototype written in
Go.

It explores content-defined chunking, containerized storage,
deduplication concepts, and database-backed metadata indexing.

------------------------------------------------------------------------

# ⚠️ Research Prototype -- Not Production Ready ⚠️

This project is an experimental storage prototype.

It is **NOT production-ready**.

Do NOT use for: - Valuable data - Sensitive data - Irreplaceable data -
Business workloads

Missing features include: - Encryption - Tamper protection -
Crash-consistency guarantees - Background compaction - Production-grade
content-defined chunking - Extensive recovery testing

Use at your own risk.

------------------------------------------------------------------------

## Project Structure

Repository layout:

    /app            → Application source code
    /db             → Database initialization scripts
    / scripts       → Utility scripts (smoke testing)
    /samples        → Sample test files
    go.mod          → Go module definition (moved to repository root)
    go.sum          → Go dependency checksums (moved to repository root)
    docker-compose.yml

> Note: `go.mod` and `go.sum` were intentionally moved to the repository
> root to align with Go module best practices and improve CI/Docker
> compatibility.

------------------------------------------------------------------------

## Requirements

-   Go 1.23+
-   Docker & Docker Compose
-   PostgreSQL (via Docker)

------------------------------------------------------------------------

## Quick Start (Docker)

Start PostgreSQL:

    docker compose up -d postgres

Run stats:

    docker compose run --rm app capsule stats

Store a folder:

    docker compose run --rm -v ./input:/input app capsule store-folder /input

------------------------------------------------------------------------

## Local Development

Build:

    go build -o capsule ./app

Run tests:

    go test ./...
    go test -race ./...

Run vet:

    go vet ./...

------------------------------------------------------------------------

## Known Limitations

-   Content-defined chunking uses a simplified rolling hash.
-   Deduplication efficiency is limited.
-   Containers are not encrypted.
-   Compression is synchronous.
-   No background compaction.
-   Not optimized for large-scale production workloads.

------------------------------------------------------------------------

## License

This project is licensed under the Apache License 2.0.

------------------------------------------------------------------------

## Development Note

This project was developed with the assistance of Large Language Models
(LLMs) as collaborative engineering tools.

All architectural decisions, design trade-offs, and implementation
validation were reviewed and validated by the project author.

LLMs were used to accelerate iteration and exploration, not to replace
engineering judgment.
