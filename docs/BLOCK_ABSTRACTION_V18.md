# v1.8 Block Abstraction Design Lock (Phase 1 Step 1)

Status: Locked implementation contract for v1.8 foundation work.

Purpose:

- Freeze non-negotiable constants and invariants before schema and runtime implementation.
- Prevent mid-implementation semantic drift.
- Keep v1.7 runtime behavior unchanged during Phase 1.

## Locked Constants

These values are the baseline for v1.8 unless an explicit later benchmark decision updates them.

```text
BlockFormatVersion = 1
BlockCodec         = "none"
TargetBlockSize    = 1 * 1024 * 1024   # 1 MiB
MaxBlockSize       = 1 * 1024 * 1024   # 1 MiB
BlockHashPolicy    = required
```

## Locked Invariants

1. Chunk hash is unchanged and always computed from plaintext chunk bytes.
2. Block payloads are immutable once committed.
3. A block is the atomic physical read/decode unit.
4. Each chunk belongs to exactly one block.
5. No chunk spans multiple blocks in v1.8.
6. Any open block builder must be flushed at operation end.
7. Every persisted v1.8 storage block must carry a plaintext block hash.

## Phase 1 Scope Guard

- Phase 1 may add schema/types/docs needed for v1.8.
- Phase 1 must not change v1.7 runtime behavior.
- Compatibility expectation remains additive: existing v1.7 data stays readable.

## Phase 1 Step 2 Findings (v1.7 Baseline)

- Current `blocks` model is effectively one chunk to one block (`blocks.chunk_id` is unique).
- Current block placement metadata already includes container placement (`container_id`, `block_offset`) and size fields.
- Current restore path resolves blocks by chunk identity (`JOIN blocks b ON b.chunk_id = c.id`).

## Phase 1 Step 3 Schema Strategy (Locked)

Chosen strategy: extend schema additively (recommended path).

v1.8 will not replace old layout in-place during upgrade. Instead, it introduces explicit packed-block entities while preserving v1.7 readability:

- `storage_blocks`: physical immutable block records (container placement, format/codec/sizes, integrity metadata).
- `chunk_block_refs`: chunk-to-block segment mapping (`chunk_id -> block_id + offset + size`).

Design intent:

- Preserve v1.7 data access via legacy `blocks` adapter behavior.
- Enable v1.8 packed blocks where one physical block can hold multiple chunks.
- Keep migration additive (no forced data rewrite).

## Naming and Compatibility Note

- `storage_blocks` is the canonical v1.8 packed-block table name in design docs and planning.
- Legacy `blocks` remains part of compatibility read-path support for mixed repositories.

## Phase 1 Step 4 Target Schema (Locked)

### Table: `storage_blocks`

Represents the physical stored block unit.

```sql
storage_blocks (
	id                BIGSERIAL PRIMARY KEY,

	format_version    INT NOT NULL,
	codec             TEXT NOT NULL, -- "none" for v1.8

	plaintext_size    BIGINT NOT NULL,
	stored_size       BIGINT NOT NULL,

	container_id      BIGINT NOT NULL,
	container_offset  BIGINT NOT NULL,

	block_hash        BYTEA NOT NULL, -- required for block validity verification

	created_at        TIMESTAMP NOT NULL DEFAULT NOW()
)
```

### Table: `chunk_block_refs`

Represents per-chunk placement inside one physical block.

```sql
chunk_block_refs (
	chunk_id          BIGINT NOT NULL,
	block_id          BIGINT NOT NULL,

	offset_in_block   BIGINT NOT NULL,
	size_in_block     BIGINT NOT NULL,

	PRIMARY KEY (chunk_id),
	FOREIGN KEY (block_id) REFERENCES storage_blocks(id)
)
```

### Existing Tables Kept Unchanged

- `chunk` identity remains plaintext-based (`chunk_hash = plaintext identity`).
- `file_chunk` restore recipe remains the same logical ordered mapping.

## Phase 1 Step 5 Compatibility Strategy (Locked)

v1.8 compatibility must support all of the following:

- v1.7 data (single-chunk blocks)
- v1.8 data (multi-chunk packed blocks)
- mixed repositories containing both layouts

Chosen implementation strategy for v1.8: Option A (cleanest).

- Read-path compatibility will treat each legacy v1.7 `blocks` row as an adapter-level storage block view with:
	- one chunk,
	- `offset_in_block = 0`,
	- `size_in_block = chunk/plaintext size`.
- New v1.8 packed data will use `storage_blocks` + `chunk_block_refs` directly.
- Mixed repositories are resolved through unified read abstraction that can load either legacy adapted layout or native packed layout.

Explicit Phase 1 decision:

- Option B (bulk migration converting legacy `blocks` into packed-table rows during upgrade) is deferred and is not part of Phase 1.
- No forced rewrite of existing physical payloads or metadata is allowed in Phase 1.

