BEGIN;

-- =========================
-- Schema versioning
-- =========================

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY
);
INSERT INTO schema_version(version)
SELECT 4
WHERE NOT EXISTS (SELECT 1 FROM schema_version);
-- =========================
-- Container table
-- =========================

CREATE TABLE IF NOT EXISTS container (
  id BIGSERIAL PRIMARY KEY,
  filename TEXT NOT NULL UNIQUE,
  sealed BOOLEAN NOT NULL DEFAULT FALSE,
  sealing BOOLEAN NOT NULL DEFAULT FALSE,
  container_hash TEXT DEFAULT NULL,
  quarantine BOOLEAN NOT NULL DEFAULT FALSE,
  current_size BIGINT NOT NULL DEFAULT 0 CHECK (current_size >= 0),
  max_size BIGINT NOT NULL CHECK (max_size > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_container_sealed ON container(sealed);
CREATE INDEX IF NOT EXISTS idx_container_sealing ON container(sealing);
CREATE INDEX IF NOT EXISTS idx_container_quarantine ON container(quarantine);
CREATE INDEX IF NOT EXISTS idx_container_sealed_quarantine ON container(sealed, quarantine);

ALTER TABLE container ADD COLUMN IF NOT EXISTS sealing BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_container_sealing ON container(sealing);
UPDATE schema_version SET version = 5 WHERE version < 5;
-- =========================
-- Chunk table
-- =========================

CREATE TABLE IF NOT EXISTS chunk (
  id BIGSERIAL PRIMARY KEY,
  chunk_hash TEXT NOT NULL,
  size BIGINT NOT NULL CHECK (size > 0),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
  live_ref_count BIGINT NOT NULL DEFAULT 0 CHECK (live_ref_count >= 0),
  pin_count BIGINT NOT NULL DEFAULT 0 CHECK (pin_count >= 0),
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  chunker_version TEXT NOT NULL DEFAULT 'v1-simple-rolling'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_hash_size ON chunk(chunk_hash, size);
CREATE INDEX IF NOT EXISTS idx_chunk_live_ref_count ON chunk(live_ref_count);
CREATE INDEX IF NOT EXISTS idx_chunk_pin_count ON chunk(pin_count);
CREATE INDEX IF NOT EXISTS idx_chunk_status ON chunk(status);

-- =========================
-- Logical file table
-- =========================

CREATE TABLE IF NOT EXISTS logical_file (
  id BIGSERIAL PRIMARY KEY,
  original_name TEXT NOT NULL,
  total_size BIGINT NOT NULL CHECK (total_size >= 0),
  file_hash TEXT NOT NULL,
  ref_count BIGINT NOT NULL DEFAULT 1 CHECK (ref_count >= 0),
  chunker_version TEXT NOT NULL DEFAULT 'v1-simple-rolling',
  status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (file_hash, total_size)
);

CREATE INDEX IF NOT EXISTS idx_logical_file_hash ON logical_file(file_hash);

CREATE INDEX IF NOT EXISTS idx_logical_file_status ON logical_file(status);

CREATE TABLE IF NOT EXISTS physical_file (
  path TEXT PRIMARY KEY CHECK (path <> ''),
  logical_file_id BIGINT NOT NULL
    REFERENCES logical_file(id) ON DELETE CASCADE,
  mode BIGINT,
  mtime TIMESTAMPTZ,
  uid BIGINT,
  gid BIGINT,
  is_metadata_complete BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_physical_file_logical_file_id ON physical_file(logical_file_id);

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
-- blocks table
-- =========================

CREATE TABLE IF NOT EXISTS blocks (
    id BIGSERIAL PRIMARY KEY,
    chunk_id BIGINT NOT NULL UNIQUE
        REFERENCES chunk(id) ON DELETE RESTRICT,
    codec TEXT NOT NULL CHECK (codec IN ('plain', 'aes-gcm')),
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    plaintext_size BIGINT NOT NULL CHECK (plaintext_size > 0),
    stored_size BIGINT NOT NULL CHECK (stored_size > 0),
    nonce BYTEA,
    container_id BIGINT NOT NULL
        REFERENCES container(id) ON DELETE RESTRICT,
    block_offset BIGINT NOT NULL CHECK (block_offset >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_blocks_container_id ON blocks(container_id);
CREATE INDEX IF NOT EXISTS idx_blocks_codec ON blocks(codec);

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

CREATE OR REPLACE TRIGGER trg_container_updated_at
BEFORE UPDATE ON container
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE TRIGGER trg_chunk_updated_at
BEFORE UPDATE ON chunk
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE TRIGGER trg_logical_file_updated_at
BEFORE UPDATE ON logical_file
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE TRIGGER trg_blocks_updated_at
BEFORE UPDATE ON blocks
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- Schema version 5: split ref_count into live_ref_count (file reachability) and pin_count (temporary restore pins)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'chunk' AND column_name = 'ref_count'
  ) THEN
    ALTER TABLE chunk RENAME COLUMN ref_count TO live_ref_count;
  END IF;
END $$;
ALTER TABLE chunk ADD COLUMN IF NOT EXISTS pin_count BIGINT NOT NULL DEFAULT 0 CHECK (pin_count >= 0);
CREATE INDEX IF NOT EXISTS idx_chunk_live_ref_count ON chunk(live_ref_count);
CREATE INDEX IF NOT EXISTS idx_chunk_pin_count ON chunk(pin_count);

-- Schema version 6: physical file metadata table for logical files.
ALTER TABLE logical_file ADD COLUMN IF NOT EXISTS ref_count BIGINT NOT NULL DEFAULT 1 CHECK (ref_count >= 0);

DO $$
DECLARE
  unique_con_name TEXT;
BEGIN
  SELECT con.conname INTO unique_con_name
  FROM pg_constraint con
  JOIN pg_class rel ON rel.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
  JOIN pg_attribute att ON att.attrelid = rel.oid
  WHERE nsp.nspname = 'public'
    AND rel.relname = 'physical_file'
    AND con.contype = 'u'
    AND att.attname = 'logical_file_id'
    AND att.attnum = ANY(con.conkey)
    AND cardinality(con.conkey) = 1
  LIMIT 1;

  IF unique_con_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE public.physical_file DROP CONSTRAINT %I', unique_con_name);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = 'public'
      AND rel.relname = 'physical_file'
      AND con.contype = 'c'
      AND con.conname = 'physical_file_path_not_empty'
  ) THEN
    ALTER TABLE public.physical_file
      ADD CONSTRAINT physical_file_path_not_empty CHECK (path <> '');
  END IF;
END $$;

UPDATE logical_file
SET ref_count = 1
WHERE ref_count IS NULL OR ref_count < 1;

INSERT INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete)
SELECT
  '/migrated/' ||
  CASE
    WHEN BTRIM(COALESCE(logical_file.original_name, '')) = '' THEN 'file'
    ELSE BTRIM(logical_file.original_name)
  END || '-' || logical_file.id::TEXT,
  logical_file.id,
  NULL,
  NULL,
  NULL,
  NULL,
  FALSE
FROM logical_file
WHERE NOT EXISTS (
  SELECT 1
  FROM physical_file
  WHERE physical_file.path =
    '/migrated/' ||
    CASE
      WHEN BTRIM(COALESCE(logical_file.original_name, '')) = '' THEN 'file'
      ELSE BTRIM(logical_file.original_name)
    END || '-' || logical_file.id::TEXT
);

UPDATE schema_version SET version = 6 WHERE version < 6;

-- Schema version 8: snapshot lineage metadata and normalized snapshot paths.
CREATE TABLE IF NOT EXISTS snapshot (
  id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
  label TEXT,
  parent_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_snapshot_created_at ON snapshot(created_at);

CREATE TABLE IF NOT EXISTS snapshot_path (
  id BIGSERIAL PRIMARY KEY,
  path TEXT NOT NULL UNIQUE CHECK (path <> '')
);

CREATE TABLE IF NOT EXISTS snapshot_file (
  id BIGSERIAL PRIMARY KEY,
  snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
  path_id BIGINT,
  logical_file_id BIGINT NOT NULL REFERENCES logical_file(id),
  size BIGINT,
  mode BIGINT,
  mtime TIMESTAMPTZ
);

ALTER TABLE snapshot ADD COLUMN IF NOT EXISTS parent_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_snapshot_parent_id'
  ) THEN
    ALTER TABLE snapshot
      ADD CONSTRAINT fk_snapshot_parent_id
      FOREIGN KEY (parent_id) REFERENCES snapshot(id) ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_snapshot_parent_id ON snapshot(parent_id);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'snapshot_file' AND column_name = 'path'
  ) THEN
    INSERT INTO snapshot_path(path)
    SELECT DISTINCT path
    FROM snapshot_file
    WHERE path IS NOT NULL AND path <> ''
    ON CONFLICT (path) DO NOTHING;
  END IF;
END $$;

ALTER TABLE snapshot_file ADD COLUMN IF NOT EXISTS path_id BIGINT;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'snapshot_file' AND column_name = 'path'
  ) THEN
    UPDATE snapshot_file sf
    SET path_id = sp.id
    FROM snapshot_path sp
    WHERE sf.path = sp.path AND sf.path_id IS NULL;
  END IF;
END $$;

DO $$
DECLARE
  has_path_column BOOLEAN;
  null_path_id_count BIGINT;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'snapshot_file' AND column_name = 'path'
  ) INTO has_path_column;

  IF has_path_column THEN
    SELECT COUNT(*)
    INTO null_path_id_count
    FROM snapshot_file
    WHERE path_id IS NULL;

    IF null_path_id_count > 0 THEN
      RAISE EXCEPTION 'snapshot_file.path_id backfill incomplete: % rows still NULL', null_path_id_count;
    END IF;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'snapshot_file' AND column_name = 'path_id'
  ) THEN
    -- Index/constraint finalization is performed in one canonical block below
    -- after legacy index cleanup (idx_snapshot_file_path / legacy unique index).
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_snapshot_file_path_id'
  ) THEN
    ALTER TABLE snapshot_file
      ADD CONSTRAINT fk_snapshot_file_path_id
      FOREIGN KEY (path_id) REFERENCES snapshot_path(id);
  END IF;
END $$;

DROP INDEX IF EXISTS idx_snapshot_file_path;
DROP INDEX IF EXISTS idx_snapshot_file_unique;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'snapshot_file' AND column_name = 'path'
  ) THEN
    ALTER TABLE snapshot_file DROP COLUMN path;
  END IF;
END $$;

ALTER TABLE snapshot_file ALTER COLUMN path_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_snapshot_file_path_id ON snapshot_file(path_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_file_logical_file ON snapshot_file(logical_file_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_file_unique ON snapshot_file(snapshot_id, path_id);

UPDATE schema_version SET version = 8 WHERE version < 8;

-- Schema version 9: explicit chunker metadata on logical files.
-- Canonical persisted identifier for historical rows: 'v1-simple-rolling'
-- (matches chunk.VersionV1SimpleRolling in Go code).
ALTER TABLE logical_file ADD COLUMN IF NOT EXISTS chunker_version TEXT;
ALTER TABLE logical_file ALTER COLUMN chunker_version SET DEFAULT 'v1-simple-rolling';
UPDATE logical_file
SET chunker_version = 'v1-simple-rolling'
WHERE chunker_version IS NULL;
ALTER TABLE logical_file ALTER COLUMN chunker_version SET NOT NULL;

UPDATE schema_version SET version = 9 WHERE version < 9;


-- Schema version 10: explicit chunker metadata on dedup chunks.
-- Canonical persisted identifier for historical rows: 'v1-simple-rolling'
-- (matches chunk.VersionV1SimpleRolling in Go code).
ALTER TABLE chunk ADD COLUMN IF NOT EXISTS chunker_version TEXT;
ALTER TABLE chunk ALTER COLUMN chunker_version SET DEFAULT 'v1-simple-rolling';
UPDATE chunk
SET chunker_version = 'v1-simple-rolling'
WHERE chunker_version IS NULL;
ALTER TABLE chunk ALTER COLUMN chunker_version SET NOT NULL;

UPDATE schema_version SET version = 10 WHERE version < 10;

COMMIT;
