# coldkeep (POC)

![CI](https://github.com/franchoy/coldkeep_poc/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20prototype-orange)

> **Status:** Research prototype / proof‑of‑concept.  
> **Not production‑ready. Do not use for real or sensitive data.**

coldkeep is an experimental local-first content‑addressed file storage prototype written in Go. It stores files as **chunks** inside **container** files on disk, and tracks metadata in Postgres.

This repo is meant for learning, experimentation, and discussion — not for operational backup/storage use.

---

## What it does (today)

- Store a file (or folder) by splitting it into chunks.
- Deduplicate chunks using SHA‑256.
- Pack chunks into container files up to a max size.
- Restore a file by reconstructing it from chunks.
- Remove a logical file (decrements chunk ref counts).
- Run GC to delete unreferenced chunks/containers.
- Basic stats.

---

## Design sketch

- **logical_file**: user-facing file entry (name, size, root hash)
- **chunk**: content-addressed chunk (hash, size, ref_count)
- **file_chunk**: mapping from logical_file → ordered chunk list
- **container**: physical file on disk that stores records (chunks)
- **container_chunk**: mapping chunk → container + offset/length

**Containers** are plain files under `storage/containers/`.

---

## Quickstart (Docker)

Prereqs: Docker + Docker Compose.

```bash
docker compose up -d --build
```

Run commands:

```bash
docker compose run --rm app store /data/myfile.bin
docker compose run --rm app restore <logical_name_or_id> /data/restore-output.bin
docker compose run --rm app stats
docker compose run --rm app gc
```

> Tip: In Docker, `/data` is whatever you mount into the container.  
> You can mount your current folder like:
>
> ```bash
> docker compose run --rm -v "$PWD:/data" app store /data/somefile
> ```

---

## Quickstart (Local)

Prereqs: Go + Postgres.

1) Start Postgres (example with Docker):

```bash
docker compose up -d postgres
```

2) Build:

```bash
cd app
go build -o ../coldkeep .
cd ..
```

3) Run:

```bash
./coldkeep store ./somefile.bin
./coldkeep restore <logical_file_id> ./out.bin
./coldkeep stats
./coldkeep gc
```

---

## Configuration

The app reads DB connection info from environment variables (see `docker-compose.yml` for defaults). Storage goes to `./storage/` by default.

---

## Known limitations (important)

### Crash consistency / failure modes

coldkeep is **not crash-consistent**. Some operations combine filesystem writes with DB transactions, but the filesystem cannot be rolled back if a DB transaction fails. This means:

- A crash or error can leave **orphan container files** on disk.
- DB state can temporarily **disagree** with what exists on disk.
- Sealing/compressing a container may be partially applied if interrupted.

If you test this project, do so with disposable data and be prepared to delete `storage/` and re-init the DB.

### Whole-container compression breaks random access

If container compression is enabled (gzip/zstd), restores may become **very slow** because compressed streams are not seekable. The restore path may need to decompress from the beginning and discard bytes to reach an offset.

Default for this POC is **no container compression**.

### Concurrency & integrity

- Concurrency guarantees are minimal; correctness under heavy concurrent store/remove/gc is not a goal for v0.
- Integrity checking is basic; there is no end-to-end authentication of restored files beyond chunk hashing.
- concurrency can create orphan bytes inside containers
---

## Security

See [SECURITY.md](SECURITY.md).

This project is a prototype. It does not claim to provide strong security guarantees and should not be used to protect sensitive information.

---

## Development

### Tests

- Unit tests run with `go test ./...`
- Integration tests require Postgres and are gated by environment (see tests for details).

### Smoke script

`scripts/smoke.sh` is intended for **local runs** (it uses `bash` and `./coldkeep`). If you run it inside Docker, ensure you use a dev container that has bash and your repo mounted.

---

## Roadmap ideas

- Block/record framing that supports random access with compression.
- Stronger crash consistency (two-phase container state, write-ahead records).
- Better GC safety and concurrent operations.
- Segment/block hierarchy, erasure coding experiments, cloud backends.
- CLI improvements, versioned migrations, richer stats.

---

## Contributing

Contributions and discussion are welcome — especially around format design, correctness, and performance. See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Apache-2.0. See [LICENSE](LICENSE).
