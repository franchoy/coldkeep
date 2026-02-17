-- ============================================================
-- Capsule V0 Alpha - Initial Schema
-- ============================================================

-- ============================================================
-- Schema Version
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

INSERT INTO schema_version (version)
SELECT 1
WHERE NOT EXISTS (SELECT 1 FROM schema_version);


-- ============================================================
-- Containers
-- ============================================================

CREATE TABLE IF NOT EXISTS container (
    id BIGSERIAL PRIMARY KEY,
    filename TEXT NOT NULL UNIQUE,
    current_size BIGINT NOT NULL,
    max_size BIGINT NOT NULL,
    sealed BOOLEAN NOT NULL DEFAULT FALSE,
    compression_algorithm TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- Chunks
-- ============================================================

CREATE TABLE IF NOT EXISTS chunk (
    id BIGSERIAL PRIMARY KEY,
    hash BYTEA NOT NULL,
    size INTEGER NOT NULL,
    container_id BIGINT NOT NULL REFERENCES container(id) ON DELETE CASCADE,
    chunk_offset BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Prevent duplicate chunk storage (dedup core rule)
CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_hash_unique ON chunk(hash);


-- ============================================================
-- Files (Logical Files)
-- ============================================================

CREATE TABLE IF NOT EXISTS file (
    id BIGSERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    size BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- File ↔ Chunk Mapping (Ordered)
-- ============================================================

CREATE TABLE IF NOT EXISTS file_chunk (
    file_id BIGINT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    chunk_id BIGINT NOT NULL REFERENCES chunk(id) ON DELETE CASCADE,
    chunk_order INTEGER NOT NULL,
    PRIMARY KEY (file_id, chunk_order)
);

CREATE INDEX IF NOT EXISTS idx_file_chunk_file ON file_chunk(file_id);
CREATE INDEX IF NOT EXISTS idx_file_chunk_chunk ON file_chunk(chunk_id);
