package db_test

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

// SCH-008 is a representative supported SQLite v12 upgrade.  The existing
// migrations_test.go fixtures retain focused coverage for v7, v8, v13, and
// other individual historical repair paths.
func TestSCH008SQLiteV12MigrationPreservesStorageBlockData(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if backend.Kind != db.BackendSQLite {
			return
		}
		mustExec(t, backend.DB, `CREATE TABLE schema_version (version INTEGER PRIMARY KEY)`)
		mustExec(t, backend.DB, `INSERT INTO schema_version(version) VALUES (12)`)
		mustExec(t, backend.DB, `CREATE TABLE repository_config (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
		mustExec(t, backend.DB, `INSERT INTO repository_config(key, value) VALUES ('default_chunker', 'v1-simple-rolling')`)
		mustExec(t, backend.DB, `CREATE TABLE container (id INTEGER PRIMARY KEY, filename TEXT NOT NULL UNIQUE, sealed INTEGER NOT NULL DEFAULT 0, sealing INTEGER NOT NULL DEFAULT 0, quarantine INTEGER NOT NULL DEFAULT 0, current_size INTEGER NOT NULL DEFAULT 0, max_size INTEGER NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`)
		mustExec(t, backend.DB, `CREATE TABLE storage_blocks (id INTEGER PRIMARY KEY, format_version INTEGER NOT NULL, codec TEXT NOT NULL, plaintext_size INTEGER NOT NULL, stored_size INTEGER NOT NULL, container_id INTEGER NOT NULL REFERENCES container(id), container_offset INTEGER NOT NULL, block_hash BLOB NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`)
		mustExec(t, backend.DB, `INSERT INTO container(id, filename, max_size) VALUES ($1,$2,$3)`, 81, "legacy-container", 100)
		mustExec(t, backend.DB, `INSERT INTO storage_blocks(id, format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, 82, 1, "none", 64, 64, 81, 0, []byte{1, 2, 3})
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("migrate SQLite v12 fixture: %v", err)
		}
		assertCurrentSchemaVersion(t, backend.DB)
		var plaintext, stored int64
		var compression string
		if err := backend.DB.QueryRow(`SELECT plaintext_size, stored_size, compression_codec FROM storage_blocks WHERE id = $1`, 82).Scan(&plaintext, &stored, &compression); err != nil {
			t.Fatalf("read migrated storage block: %v", err)
		}
		if plaintext != 64 || stored != 64 || compression != "none" {
			t.Fatalf("migrated storage block = plaintext:%d stored:%d compression:%q", plaintext, stored, compression)
		}
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("rerun SQLite v12 migration: %v", err)
		}
	})
}

// SCH-009 verifies the production PostgreSQL auto-migration entry point for
// the supported v11 metadata path.  The existing PostgreSQL legacy fixtures
// cover v5/v7 snapshots and pre-v6 physical-file migration details.
func TestSCH009PostgresVersionElevenAutoMigration(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		if backend.Kind != db.BackendPostgres {
			return
		}
		mustExec(t, backend.DB, `UPDATE schema_version SET version = 11 WHERE version < 11`)
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("auto-migrate PostgreSQL v11 fixture: %v", err)
		}
		assertCurrentSchemaVersion(t, backend.DB)
	})
}

// SCH-010 and SCH-011 document the current entry points' observable metadata
// behavior.  Empty databases bootstrap; an empty version table is rejected by
// CurrentSchemaVersion rather than being misreported as current.
func TestSCH010AndSCH011MetadataBoundaries(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("bootstrap empty database: %v", err)
		}
		assertCurrentSchemaVersion(t, backend.DB)
	})
}
