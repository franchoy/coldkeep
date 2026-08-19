package db_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

const phase5SchemaVersion = 16

func TestSCH001AndSCH002BootstrapVersionAndIdempotency(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
		assertCurrentSchemaVersion(t, backend.DB)
		if _, err := backend.DB.Exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 1, "contract", 1, "hash", 1, "COMPLETED"); err != nil {
			t.Fatalf("schema unusable: %v", err)
		}
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("second EnsureSchema: %v", err)
		}
		assertCurrentSchemaVersion(t, backend.DB)
		var name string
		if err := backend.DB.QueryRow(`SELECT original_name FROM logical_file WHERE id = $1`, 1).Scan(&name); err != nil {
			t.Fatalf("read preserved logical file: %v", err)
		}
		if name != "contract" {
			t.Fatalf("preserved logical-file name = %q, want contract", name)
		}
		var currentVersionRows int
		if err := backend.DB.QueryRow(`SELECT COUNT(*) FROM schema_version WHERE version = $1`, phase5SchemaVersion).Scan(&currentVersionRows); err != nil {
			t.Fatalf("count current schema-version rows: %v", err)
		}
		if currentVersionRows != 1 {
			t.Fatalf("current schema-version rows = %d, want 1", currentVersionRows)
		}
	})
}

func TestSCH005PrimaryKeyAndPhysicalFileForeignKey(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		conn := backend.DB
		_, err := conn.Exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 1, "one", 1, "hash", 1, "COMPLETED")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 1, "duplicate", 1, "hash2", 1, "COMPLETED"); err == nil {
			t.Fatal("duplicate logical file ID succeeded")
		}
		if _, err := conn.Exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete) VALUES ($1,$2,$3,$4,$5)`, "/missing", 999, 0o644, time.Now().UTC(), true); err == nil {
			t.Fatal("invalid physical-file foreign key succeeded")
		}
	})
}

func TestSCH011CurrentSchemaVersionRejectsEmptyMetadata(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if _, err := backend.DB.Exec(`CREATE TABLE schema_version (version INTEGER)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.CurrentSchemaVersion(backend.DB); err == nil {
			t.Fatal("empty schema_version accepted")
		}
	})
}

// SCH-003 verifies the current metadata shape is stable and readable without
// requiring physical SQLite/PostgreSQL type equality.
func TestSCH003CurrentSchemaMetadata(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		assertCurrentSchemaVersion(t, backend.DB)
		var rows int
		if err := backend.DB.QueryRow(`SELECT COUNT(*) FROM schema_version WHERE version = $1`, phase5SchemaVersion).Scan(&rows); err != nil {
			t.Fatalf("read current schema metadata: %v", err)
		}
		if rows != 1 {
			t.Fatalf("current schema metadata rows = %d, want 1", rows)
		}
	})
}

// SCH-004 proves only the smallest catalog-shaped write/read operation needed
// after bootstrap; catalog-operation parity belongs to Phase 6.
func TestSCH004MinimalCatalogUsability(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		if _, err := backend.DB.Exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 41, "minimal", 9, "minimal-hash", 1, "COMPLETED"); err != nil {
			t.Fatalf("insert minimal logical file: %v", err)
		}
		var got string
		if err := backend.DB.QueryRow(`SELECT file_hash FROM logical_file WHERE id = $1`, 41).Scan(&got); err != nil {
			t.Fatalf("read minimal logical file: %v", err)
		}
		if got != "minimal-hash" {
			t.Fatalf("minimal logical-file hash = %q", got)
		}
	})
}

// SCH-005 exercises identity constraints as observable success/failure
// behavior, intentionally not backend-specific error text.
func TestSCH005CriticalUniqueness(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		mustExec(t, backend.DB, `INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 51, "one", 10, "identity-hash", 1, "COMPLETED")
		mustFail(t, backend.DB, `INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 52, "two", 10, "identity-hash", 1, "COMPLETED")
		mustExec(t, backend.DB, `INSERT INTO snapshot (id, created_at, type) VALUES ($1,$2,$3)`, "snapshot-identity", time.Unix(1, 0).UTC(), "full")
		mustFail(t, backend.DB, `INSERT INTO snapshot (id, created_at, type) VALUES ($1,$2,$3)`, "snapshot-identity", time.Unix(2, 0).UTC(), "full")
		mustExec(t, backend.DB, `INSERT INTO chunk (id, chunk_hash, size, status) VALUES ($1,$2,$3,$4)`, 53, "chunk-identity", 3, "COMPLETED")
		mustFail(t, backend.DB, `INSERT INTO chunk (id, chunk_hash, size, status) VALUES ($1,$2,$3,$4)`, 54, "chunk-identity", 3, "COMPLETED")
		mustExec(t, backend.DB, `INSERT INTO physical_file (path, logical_file_id) VALUES ($1,$2)`, "/identity", 51)
		mustFail(t, backend.DB, `INSERT INTO physical_file (path, logical_file_id) VALUES ($1,$2)`, "/identity", 51)
	})
}

// SCH-006 verifies the documented physical-file cascade and restrictive
// dependent references, including SQLite's connection-local foreign-key mode.
func TestSCH006CriticalForeignKeys(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		mustFail(t, backend.DB, `INSERT INTO physical_file (path, logical_file_id) VALUES ($1,$2)`, "/missing", 999)
		mustFail(t, backend.DB, `INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1,$2,$3)`, 999, 999, 0)
		mustExec(t, backend.DB, `INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 61, "parent", 1, "parent-hash", 1, "COMPLETED")
		mustExec(t, backend.DB, `INSERT INTO physical_file (path, logical_file_id) VALUES ($1,$2)`, "/cascaded", 61)
		mustExec(t, backend.DB, `DELETE FROM logical_file WHERE id = $1`, 61)
		var children int
		if err := backend.DB.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path = $1`, "/cascaded").Scan(&children); err != nil {
			t.Fatalf("count cascaded physical file: %v", err)
		}
		if children != 0 {
			t.Fatalf("physical-file cascade left %d row(s)", children)
		}
	})
}

// SCH-007 compares logical defaults and nullable values, not their driver
// storage representation.
func TestSCH007NullableAndDefaultSemantics(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		mustExec(t, backend.DB, `INSERT INTO container (id, filename, max_size) VALUES ($1,$2,$3)`, 71, "defaults", 100)
		var sealed, sealing, quarantine bool
		var currentSize int64
		if err := backend.DB.QueryRow(`SELECT sealed, sealing, quarantine, current_size FROM container WHERE id = $1`, 71).Scan(&sealed, &sealing, &quarantine, &currentSize); err != nil {
			t.Fatalf("read container defaults: %v", err)
		}
		if sealed || sealing || quarantine || currentSize != 0 {
			t.Fatalf("container defaults = sealed:%t sealing:%t quarantine:%t size:%d", sealed, sealing, quarantine, currentSize)
		}
		mustExec(t, backend.DB, `INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1,$2,$3,$4,$5,$6)`, 72, "nullable", 1, "nullable-hash", 1, "COMPLETED")
		mustExec(t, backend.DB, `INSERT INTO physical_file (path, logical_file_id) VALUES ($1,$2)`, "/nullable", 72)
		var mtime sql.NullTime
		var complete bool
		if err := backend.DB.QueryRow(`SELECT mtime, is_metadata_complete FROM physical_file WHERE path = $1`, "/nullable").Scan(&mtime, &complete); err != nil {
			t.Fatalf("read nullable physical metadata: %v", err)
		}
		if mtime.Valid || complete {
			t.Fatalf("physical metadata defaults = mtime valid:%t complete:%t", mtime.Valid, complete)
		}
	})
}

func mustExec(t *testing.T, conn *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func mustFail(t *testing.T, conn *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(query, args...); err == nil {
		t.Fatalf("expected failure for %q", query)
	}
}

func assertCurrentSchemaVersion(t *testing.T, conn *sql.DB) {
	t.Helper()
	version, err := db.CurrentSchemaVersion(conn)
	if err != nil {
		t.Fatalf("CurrentSchemaVersion: %v", err)
	}
	if version != phase5SchemaVersion {
		t.Fatalf("version = %d, want %d", version, phase5SchemaVersion)
	}
}
