📦 Capsule POC (V0)

A normalized, deduplicated incremental backup engine written in Go.

Capsule stores files as content-defined chunks inside append-only containers, enabling:

Global chunk deduplication

Incremental backups

Container-level compression

Reference-count-based garbage collection

Integrity verification during restore

This is a V0 alpha public release intended for experimentation and architectural feedback.

🚀 Features (V0)

Content-defined chunking (CDC)

SHA-256 chunk deduplication

Append-only container format

Container-level compression (Zstandard)

Reference counting for chunks

Garbage collection of unreferenced containers

Restore by file ID

Integrity verification during restore

🏗 Architecture Overview
Storage Model

Logical file
→ ordered list of chunks
→ chunks stored in containers
→ containers optionally compressed when sealed

Container Record Format

Each chunk is stored inside a container as:

[32 bytes SHA256][4 bytes chunk_size][chunk_data]


Containers are compressed only when sealed.

🗄 Database Schema (PostgreSQL)

Main tables:

logical_file

chunk

file_chunk

container

logical_file

Stores user-visible files.

column	type
id	BIGSERIAL
original_name	TEXT
total_size	BIGINT
file_hash	TEXT
created_at	TIMESTAMP
chunk

Deduplicated physical chunks.

column	type
id	BIGSERIAL
sha256	TEXT (unique)
size	INTEGER
container_id	BIGINT
chunk_offset	BIGINT
ref_count	BIGINT
file_chunk

Ordered mapping between logical file and chunks.

column	type
logical_file_id	BIGINT
chunk_id	BIGINT
chunk_order	INTEGER
container

Physical container files.

column	type
id	BIGSERIAL
filename	TEXT
current_size	BIGINT
max_size	BIGINT
sealed	BOOLEAN
compression_algorithm	TEXT
compressed_size	BIGINT
🛠 Requirements

Go 1.21+

PostgreSQL 14+

⚙️ Configuration

Environment variables:

DB_HOST
DB_PORT
DB_USER
DB_PASSWORD
DB_NAME
CAPSULE_STORAGE_DIR   (optional, default: ./storage/containers)

🐳 Quickstart (with Docker)
docker-compose up -d


Initialize database:

psql -h localhost -U capsule -d capsule -f db/init.sql


Build CLI:

go build -o capsule ./app

📥 Usage
Store a file
./capsule store myfile.bin

Store a folder
./capsule store-folder ./myfolder

Restore a file

Restore by logical file ID into a directory:

./capsule restore 12 ./restored


Result:

./restored/<original_filename>


Integrity of each chunk is verified during restore.

Remove a file
./capsule remove 12


Decrements chunk reference counts.

Run garbage collection
./capsule gc


Deletes:

chunks with ref_count = 0

containers that no longer contain chunks

Show statistics
./capsule stats


Displays:

total logical files

total containers

total compressed bytes

per-container stats

⚠️ Limitations (V0)

Restore loads full container into memory

No encryption

No multi-tenant support

No streaming restore

No container header validation yet

No concurrent multi-process locking

🛣 Roadmap (V1+)

Streaming restore

Encryption layer

Container header validation

Snapshot/version support

Multi-tenant isolation

Remote storage backend support

Erasure coding

📄 License

Apache License 2.0

🤝 Contributing

This is an experimental storage engine.
Issues, feedback, and architectural discussions are welcome.

🎯 Project Status

Capsule POC V0 is a working alpha prototype.
It is suitable for experimentation and architectural exploration, but not production use.