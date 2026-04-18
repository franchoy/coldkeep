BEGIN;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY
);

UPDATE schema_version SET version = 7 WHERE version < 7;
INSERT OR IGNORE INTO schema_version(version) VALUES (7);

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
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
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

CREATE TABLE IF NOT EXISTS snapshot (
  id TEXT PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
  label TEXT
);

CREATE INDEX IF NOT EXISTS idx_snapshot_created_at ON snapshot(created_at);

CREATE TABLE IF NOT EXISTS snapshot_file (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
  path TEXT NOT NULL CHECK (path != ''),
  logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
  size INTEGER,
  mode INTEGER,
  mtime TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_file_path ON snapshot_file(path);
CREATE INDEX IF NOT EXISTS idx_snapshot_file_logical_file ON snapshot_file(logical_file_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_file_unique ON snapshot_file(snapshot_id, path);

COMMIT;
