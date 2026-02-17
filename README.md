📦 Capsule

Capsule is a deduplicated, container-based storage engine written in Go.
It uses content-defined chunking (CDC), content-addressed storage, and container packing with compression.

⚠ Alpha Version – Not production ready.

🚀 What Capsule Does

Capsule stores files using the following pipeline:

File
 → Content-Defined Chunking (CDC)
 → SHA-256 hashing per chunk
 → Deduplication (global chunk index)
 → Append to container
 → Seal container when full
 → Compress container


Key properties:

Global chunk-level deduplication

Immutable sealed containers

Versioned container format

Integrity verification during restore

Database-backed logical file mapping

🏗 Architecture Overview

Capsule separates:

1️⃣ Logical Layer

Files are represented as ordered lists of chunk IDs.

Stored in PostgreSQL tables (file, file_chunk).

2️⃣ Physical Layer

Chunks are packed into containers.

Containers are sealed and compressed.

Each container is immutable once sealed.

3️⃣ Index Layer

The database maps:

chunk_hash → container_id + offset

file → ordered chunks

📂 Storage Model
Containers (V0 Format)

Each container file has the following layout:

[64-byte header]
[chunk_record]
[chunk_record]
...

Container Header (64 bytes)
Field	Description
Magic	"CAPSULE0"
Version	Major / Minor
Header Length	Always 64 (V0)
Flags	Reserved for future use
Created Timestamp	Unix nanoseconds
Max Container Size	Policy at creation
Container UID	16 random bytes
CRC32	Header integrity
Reserved	Future extension

The header allows future format evolution while maintaining backward compatibility.

Chunk Record (V0)

Each chunk is stored as:

[32 bytes SHA256 hash]
[4 bytes little-endian size]
[N bytes raw chunk data]


Important:

Offsets stored in the database refer to the start of this record.

Restore verifies chunk integrity by recomputing SHA256.

🗃 Database Schema (V0)

Core tables:

container – physical container metadata

chunk – content-addressed chunk index

file – logical file metadata

file_chunk – ordered mapping

schema_version – schema contract

Deduplication is enforced by a unique index on chunk.hash.

🔒 Container Lifecycle

New container created when no open container exists.

Chunks appended until max_size reached.

Container is sealed:

Marked sealed = TRUE

Compressed (zstd)

Original uncompressed file removed.

Sealed containers are immutable.

Restore automatically decompresses before extracting chunks.

♻ Garbage Collection

When files are removed:

Unreferenced chunks are deleted.

Containers with no remaining chunks can be removed.

(Alpha implementation — behavior may evolve.)

⚠ Alpha Status

Capsule V0 Alpha:

Does not include encryption.

Does not include block-level container layout.

Does not include snapshot manifests inside storage.

Schema may evolve in future versions.

Future releases will maintain backward compatibility with V0 containers.

🧪 Example Usage
capsule store myfile.bin
capsule restore <file_id> restored.bin
capsule stats
capsule remove <file_id>
capsule gc

🛣 Roadmap

Planned future milestones:

V1: Container-level encryption

V2: Block-based container layout

Snapshot metadata stored as content-addressed objects

Performance optimizations

Cloud backend support

🎯 Design Principles

Immutable containers

Self-describing physical format

Versioned storage contract

Separation of logical and physical layers

Future-proof evolution

📌 Version

Current version: v0.1.0-alpha