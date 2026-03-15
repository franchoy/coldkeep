BEGIN;

-- =========================
-- Schema versioning
-- =========================

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY
);
INSERT INTO schema_version(version)
SELECT 2
WHERE NOT EXISTS (SELECT 1 FROM schema_version);

-- =========================
-- Container table
-- =========================

CREATE TABLE IF NOT EXISTS container (
  id BIGSERIAL PRIMARY KEY,
  filename TEXT NOT NULL UNIQUE,
  sealed BOOLEAN NOT NULL DEFAULT FALSE,
  quarantine BOOLEAN NOT NULL DEFAULT FALSE,
  current_size BIGINT NOT NULL DEFAULT 0 CHECK (current_size >= 0),
  max_size BIGINT NOT NULL CHECK (max_size > 0),
  compression_algorithm TEXT NOT NULL DEFAULT 'none',
  compressed_size BIGINT NOT NULL DEFAULT 0 CHECK (compressed_size >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_container_sealed ON container(sealed);
CREATE INDEX IF NOT EXISTS idx_container_quarantine ON container(quarantine);
CREATE INDEX IF NOT EXISTS idx_container_sealed_quarantine ON container(sealed, quarantine);

-- =========================
-- Chunk table
-- =========================

CREATE TABLE IF NOT EXISTS chunk (
  id BIGSERIAL PRIMARY KEY,
  chunk_hash TEXT NOT NULL,
  size BIGINT NOT NULL CHECK (size > 0),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
  container_id BIGINT
    REFERENCES container(id) ON DELETE RESTRICT,
  chunk_offset BIGINT CHECK (chunk_offset >= 0),
  ref_count BIGINT NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_hash_size ON chunk(chunk_hash, size);
CREATE INDEX IF NOT EXISTS idx_chunk_container ON chunk(container_id);
CREATE INDEX IF NOT EXISTS idx_chunk_ref_count ON chunk(ref_count);
CREATE INDEX IF NOT EXISTS idx_chunk_status ON chunk(status);

-- =========================
-- Logical file table
-- =========================

CREATE TABLE IF NOT EXISTS logical_file (
  id BIGSERIAL PRIMARY KEY,
  original_name TEXT NOT NULL,
  total_size BIGINT NOT NULL CHECK (total_size >= 0),
  file_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (file_hash, total_size)
);

CREATE INDEX IF NOT EXISTS idx_logical_file_hash ON logical_file(file_hash);

CREATE INDEX IF NOT EXISTS idx_logical_file_status ON logical_file(status);

-- =========================
-- File ↔ Chunk mapping
-- =========================

CREATE TABLE IF NOT EXISTS file_chunk (
  logical_file_id BIGINT NOT NULL
    REFERENCES logical_file(id) ON DELETE CASCADE,
  chunk_id BIGINT NOT NULL
    REFERENCES chunk(id) ON DELETE RESTRICT,
  chunk_order BIGINT NOT NULL CHECK (chunk_order >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (logical_file_id, chunk_order)
  );

CREATE INDEX IF NOT EXISTS idx_file_chunk_logical_file_id ON file_chunk(logical_file_id);
CREATE INDEX IF NOT EXISTS idx_file_chunk_chunk_id ON file_chunk(chunk_id);


-- =========================
-- updated_at trigger
-- =========================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_container_updated_at
BEFORE UPDATE ON container
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_chunk_updated_at
BEFORE UPDATE ON chunk
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_logical_file_updated_at
BEFORE UPDATE ON logical_file
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;