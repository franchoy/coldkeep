BEGIN;

-- =========================
-- Schema versioning
-- =========================

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);

INSERT INTO schema_version(version)
SELECT 1
WHERE NOT EXISTS (SELECT 1 FROM schema_version);

-- =========================
-- Container table
-- =========================

CREATE TABLE IF NOT EXISTS container (
  id BIGSERIAL PRIMARY KEY,
  filename TEXT NOT NULL UNIQUE,
  current_size BIGINT NOT NULL DEFAULT 0 CHECK (current_size >= 0),
  max_size BIGINT NOT NULL CHECK (max_size > 0),
  sealed BOOLEAN NOT NULL DEFAULT FALSE,
  compression_algorithm TEXT NOT NULL DEFAULT 'none',
  compressed_size BIGINT NOT NULL DEFAULT 0 CHECK (compressed_size >= 0),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_container_sealed 
ON container(sealed);

-- =========================
-- Chunk table
-- =========================

CREATE TABLE IF NOT EXISTS chunk (
  id BIGSERIAL PRIMARY KEY,
  sha256 TEXT NOT NULL UNIQUE,
  size BIGINT NOT NULL CHECK (size > 0),
  container_id BIGINT NOT NULL 
    REFERENCES container(id) ON DELETE RESTRICT,
  chunk_offset BIGINT NOT NULL CHECK (chunk_offset >= 0),
  ref_count BIGINT NOT NULL DEFAULT 1 CHECK (ref_count >= 0),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chunk_container 
ON chunk(container_id);

CREATE INDEX IF NOT EXISTS idx_chunk_ref_count 
ON chunk(ref_count);

-- =========================
-- Logical file table
-- =========================

CREATE TABLE IF NOT EXISTS logical_file (
  id BIGSERIAL PRIMARY KEY,
  original_name TEXT NOT NULL,
  total_size BIGINT NOT NULL CHECK (total_size >= 0),
  file_hash TEXT NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logical_file_hash 
ON logical_file(file_hash);

-- =========================
-- File ↔ Chunk mapping
-- =========================

CREATE TABLE IF NOT EXISTS file_chunk (
  logical_file_id BIGINT NOT NULL
    REFERENCES logical_file(id) ON DELETE CASCADE,
  chunk_id BIGINT NOT NULL
    REFERENCES chunk(id) ON DELETE RESTRICT,
  chunk_order INTEGER NOT NULL CHECK (chunk_order >= 0),
  PRIMARY KEY (logical_file_id, chunk_order),
  UNIQUE (logical_file_id, chunk_id)
);

CREATE INDEX IF NOT EXISTS idx_file_chunk_chunk 
ON file_chunk(chunk_id);

COMMIT;