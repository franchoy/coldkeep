BEGIN;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);

INSERT INTO schema_version(version)
SELECT 1
WHERE NOT EXISTS (SELECT 1 FROM schema_version);

CREATE TABLE IF NOT EXISTS container (
  id BIGSERIAL PRIMARY KEY,
  filename TEXT NOT NULL UNIQUE,
  current_size BIGINT NOT NULL DEFAULT 0,
  max_size BIGINT NOT NULL,
  sealed BOOLEAN NOT NULL DEFAULT FALSE,
  compression_algorithm TEXT NOT NULL DEFAULT 'none',
  compressed_size BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_container_sealed ON container(sealed);

CREATE TABLE IF NOT EXISTS chunk (
  id BIGSERIAL PRIMARY KEY,
  sha256 TEXT NOT NULL UNIQUE,
  size INTEGER NOT NULL,
  container_id BIGINT NOT NULL REFERENCES container(id) ON DELETE RESTRICT,
  chunk_offset BIGINT NOT NULL,
  ref_count BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chunk_container ON chunk(container_id);

CREATE TABLE IF NOT EXISTS logical_file (
  id BIGSERIAL PRIMARY KEY,
  original_name TEXT NOT NULL,
  total_size BIGINT NOT NULL,
  file_hash TEXT NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logical_file_hash ON logical_file(file_hash);

CREATE TABLE IF NOT EXISTS file_chunk (
  logical_file_id BIGINT NOT NULL REFERENCES logical_file(id) ON DELETE CASCADE,
  chunk_id BIGINT NOT NULL REFERENCES chunk(id) ON DELETE RESTRICT,
  chunk_order INTEGER NOT NULL,
  PRIMARY KEY (logical_file_id, chunk_order)
);

CREATE INDEX IF NOT EXISTS idx_file_chunk_chunk ON file_chunk(chunk_id);

COMMIT;
