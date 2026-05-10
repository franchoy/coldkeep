BEGIN;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY
);

DELETE FROM schema_version WHERE version < 15;
INSERT OR IGNORE INTO schema_version(version) VALUES (15);

CREATE TABLE IF NOT EXISTS container (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL UNIQUE,
  sealed INTEGER NOT NULL DEFAULT 0,
  sealing INTEGER NOT NULL DEFAULT 0,
  container_hash TEXT DEFAULT NULL,
  quarantine INTEGER NOT NULL DEFAULT 0,
  current_size INTEGER NOT NULL DEFAULT 0 CHECK (current_size >= 0),
  max_size INTEGER NOT NULL CHECK (max_size > 0),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_container_sealed ON container(sealed);
CREATE INDEX IF NOT EXISTS idx_container_sealing ON container(sealing);
CREATE INDEX IF NOT EXISTS idx_container_quarantine ON container(quarantine);
CREATE INDEX IF NOT EXISTS idx_container_sealed_quarantine ON container(sealed, quarantine);

CREATE TABLE IF NOT EXISTS chunk (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chunk_hash TEXT NOT NULL,
  size INTEGER NOT NULL CHECK (size > 0),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
  live_ref_count INTEGER NOT NULL DEFAULT 0 CHECK (live_ref_count >= 0),
  pin_count INTEGER NOT NULL DEFAULT 0 CHECK (pin_count >= 0),
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  chunker_version TEXT NOT NULL DEFAULT 'v1-simple-rolling'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_hash_size ON chunk(chunk_hash, size);
CREATE INDEX IF NOT EXISTS idx_chunk_live_ref_count ON chunk(live_ref_count);
CREATE INDEX IF NOT EXISTS idx_chunk_pin_count ON chunk(pin_count);
CREATE INDEX IF NOT EXISTS idx_chunk_status ON chunk(status);

CREATE TABLE IF NOT EXISTS logical_file (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  original_name TEXT NOT NULL,
  total_size INTEGER NOT NULL CHECK (total_size >= 0),
  file_hash TEXT NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 1 CHECK (ref_count >= 0),
  chunker_version TEXT NOT NULL DEFAULT 'v1-simple-rolling',
  status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (file_hash, total_size)
);

CREATE INDEX IF NOT EXISTS idx_logical_file_hash ON logical_file(file_hash);
CREATE INDEX IF NOT EXISTS idx_logical_file_status ON logical_file(status);

CREATE TABLE IF NOT EXISTS physical_file (
  path TEXT PRIMARY KEY CHECK (path != ''),
  logical_file_id INTEGER NOT NULL
    REFERENCES logical_file(id) ON DELETE CASCADE,
  mode INTEGER,
  mtime DATETIME,
  uid INTEGER,
  gid INTEGER,
  is_metadata_complete INTEGER NOT NULL DEFAULT 0 CHECK (is_metadata_complete IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_physical_file_logical_file_id ON physical_file(logical_file_id);

CREATE TABLE IF NOT EXISTS file_chunk (
  logical_file_id INTEGER NOT NULL
    REFERENCES logical_file(id) ON DELETE CASCADE,
  chunk_id INTEGER NOT NULL
    REFERENCES chunk(id) ON DELETE RESTRICT,
  chunk_order INTEGER NOT NULL CHECK (chunk_order >= 0),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (logical_file_id, chunk_order)
);

CREATE INDEX IF NOT EXISTS idx_file_chunk_logical_file_id ON file_chunk(logical_file_id);
CREATE INDEX IF NOT EXISTS idx_file_chunk_chunk_id ON file_chunk(chunk_id);

CREATE TABLE IF NOT EXISTS repository_config (
  key TEXT PRIMARY KEY CHECK (key != ''),
  value TEXT NOT NULL CHECK (value != '')
);

-- Fresh v1.5+ installs default to v2-fastcdc. Upgrade paths are handled in
-- internal/db/migrations.go to keep legacy repositories on v1 unless changed manually.
INSERT OR IGNORE INTO repository_config(key, value)
VALUES ('default_chunker', 'v2-fastcdc');

INSERT OR IGNORE INTO repository_config(key, value)
VALUES ('compression', 'none');

INSERT OR IGNORE INTO repository_config(key, value)
VALUES ('compression_level', '3');

CREATE TABLE IF NOT EXISTS blocks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chunk_id INTEGER NOT NULL UNIQUE
    REFERENCES chunk(id) ON DELETE RESTRICT,
  codec TEXT NOT NULL CHECK (codec IN ('plain', 'aes-gcm')),
  format_version INTEGER NOT NULL CHECK (format_version > 0),
  plaintext_size INTEGER NOT NULL CHECK (plaintext_size > 0),
  stored_size INTEGER NOT NULL CHECK (stored_size > 0),
  nonce BLOB,
  container_id INTEGER NOT NULL
    REFERENCES container(id) ON DELETE RESTRICT,
  block_offset INTEGER NOT NULL CHECK (block_offset >= 0),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_blocks_container_id ON blocks(container_id);
CREATE INDEX IF NOT EXISTS idx_blocks_codec ON blocks(codec);

CREATE TABLE IF NOT EXISTS storage_blocks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  format_version INTEGER NOT NULL CHECK (format_version > 0),
  codec TEXT NOT NULL CHECK (codec IN ('none', 'aes-gcm')),
  plaintext_size INTEGER NOT NULL CHECK (plaintext_size > 0),
  compression_codec TEXT NOT NULL DEFAULT 'none' CHECK (compression_codec IN ('none', 'zstd')),
  compression_level INTEGER,
  compressed_size INTEGER CHECK (compressed_size IS NULL OR compressed_size > 0),
  stored_size INTEGER NOT NULL CHECK (stored_size > 0),
  container_id INTEGER NOT NULL REFERENCES container(id) ON DELETE RESTRICT,
  container_offset INTEGER NOT NULL CHECK (container_offset >= 0),
  block_hash BLOB NOT NULL,
  compression_ratio REAL DEFAULT 1.0,
  -- DEPRECATED: lowercase-hex mirror of block_hash for compatibility/observability only.
  payload_hash TEXT,
  compressed_hash BLOB,
  physical_hash BLOB,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_storage_blocks_container_id ON storage_blocks(container_id);

CREATE TABLE IF NOT EXISTS chunk_block_refs (
  chunk_id INTEGER NOT NULL PRIMARY KEY REFERENCES chunk(id) ON DELETE RESTRICT,
  block_id INTEGER NOT NULL REFERENCES storage_blocks(id) ON DELETE RESTRICT,
  offset_in_block INTEGER NOT NULL CHECK (offset_in_block >= 0),
  size_in_block INTEGER NOT NULL CHECK (size_in_block > 0)
);

CREATE INDEX IF NOT EXISTS idx_chunk_block_refs_block_id ON chunk_block_refs(block_id);

CREATE TABLE IF NOT EXISTS snapshot (
  id TEXT PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
  label TEXT,
  parent_id TEXT REFERENCES snapshot(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshot_created_at ON snapshot(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshot_parent_id ON snapshot(parent_id);

CREATE TABLE IF NOT EXISTS snapshot_path (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE CHECK (path != '')
);

CREATE TABLE IF NOT EXISTS snapshot_file (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
  path_id INTEGER NOT NULL REFERENCES snapshot_path(id),
  logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
  size INTEGER,
  mode INTEGER,
  mtime TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_file_path_id ON snapshot_file(path_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_file_logical_file ON snapshot_file(logical_file_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_file_unique ON snapshot_file(snapshot_id, path_id);

COMMIT;
