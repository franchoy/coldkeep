package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// dummyDriver is a minimal sql.Driver stub that is neither sqlite3 nor pq,
// so BackendFromDB returns BackendUnknown for connections opened with it.
type dummyDriver struct{}

func (d dummyDriver) Open(_ string) (driver.Conn, error) { return nil, nil }

var registerOnce sync.Once

func registerDummyDriver() {
	registerOnce.Do(func() { sql.Register("dummy", dummyDriver{}) })
}

func TestRunMigrationsFailsWhenDBIsNil(t *testing.T) {
	err := RunMigrations(nil)
	if err == nil || !strings.Contains(err.Error(), "nil DB connection") {
		t.Fatalf("expected nil-DB error contract, got: %v", err)
	}
}

func TestEnsurePostgresSchemaFailsWhenDBIsNil(t *testing.T) {
	err := EnsurePostgresSchema(nil)
	if err == nil || !strings.Contains(err.Error(), "nil DB connection") {
		t.Fatalf("expected nil-DB error contract, got: %v", err)
	}
}

func TestRunMigrationsSucceedsOnSQLiteInMemory(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("expected RunMigrations to succeed on sqlite, got: %v", err)
	}
}

func TestRunMigrationsFailsWhenSQLiteDBIsClosed(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}

	err = RunMigrations(dbconn)
	if err == nil || !strings.Contains(err.Error(), "enable sqlite foreign keys") {
		t.Fatalf("expected wrapped foreign-keys pragma error contract, got: %v", err)
	}
}

func TestRunMigrationsRejectsNonSQLiteBackend(t *testing.T) {
	registerDummyDriver()
	dbconn, err := sql.Open("dummy", "")
	if err != nil {
		t.Fatalf("open dummy db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	err = RunMigrations(dbconn)
	if err == nil || !strings.Contains(err.Error(), "RunMigrations requires sqlite backend") {
		t.Fatalf("expected non-sqlite error contract, got: %v", err)
	}
}

func sqliteTableExists(t *testing.T, dbconn *sql.DB, tableName string) bool {
	t.Helper()

	var count int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		tableName,
	).Scan(&count); err != nil {
		t.Fatalf("check table %s existence: %v", tableName, err)
	}

	return count == 1
}

func sqliteIndexExists(t *testing.T, dbconn *sql.DB, indexName string) bool {
	t.Helper()

	var count int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`,
		indexName,
	).Scan(&count); err != nil {
		t.Fatalf("check index %s existence: %v", indexName, err)
	}

	return count == 1
}

func sqliteTestTableHasColumn(t *testing.T, dbconn *sql.DB, tableName, columnName string) bool {
	t.Helper()

	rows, err := dbconn.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		t.Fatalf("pragma table_info(%s): %v", tableName, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			cid      int
			name     string
			dataType string
			notNull  int
			defaultV sql.NullString
			primaryK int
		)
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultV, &primaryK); err != nil {
			t.Fatalf("scan pragma table_info(%s): %v", tableName, err)
		}
		if strings.EqualFold(name, columnName) {
			return true
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pragma table_info(%s): %v", tableName, err)
	}

	return false
}

func TestRunMigrationsCreatesSnapshotSchemaVersionEight(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations first pass: %v", err)
	}

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema version after first pass: %v", err)
	}
	if schemaVersion != 10 {
		t.Fatalf("expected schema version 10 after first migration pass, got %d", schemaVersion)
	}

	if !sqliteTableExists(t, dbconn, "snapshot") {
		t.Fatal("expected snapshot table to exist after migration")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_file") {
		t.Fatal("expected snapshot_file table to exist after migration")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_path") {
		t.Fatal("expected snapshot_path table to exist after migration")
	}

	for _, indexName := range []string{
		"idx_snapshot_created_at",
		"idx_snapshot_parent_id",
		"idx_snapshot_file_snapshot_id",
		"idx_snapshot_file_path_id",
		"idx_snapshot_file_logical_file",
		"idx_snapshot_file_unique",
	} {
		if !sqliteIndexExists(t, dbconn, indexName) {
			t.Fatalf("expected index %s to exist after migration", indexName)
		}
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations second pass (idempotency): %v", err)
	}

	var schemaVersionAfterSecondRun int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersionAfterSecondRun); err != nil {
		t.Fatalf("read schema version after second pass: %v", err)
	}
	if schemaVersionAfterSecondRun != 10 {
		t.Fatalf("expected schema version to stay 10 after idempotent rerun, got %d", schemaVersionAfterSecondRun)
	}

	if !sqliteTableExists(t, dbconn, "snapshot") {
		t.Fatal("expected snapshot table to remain after idempotent rerun")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_file") {
		t.Fatal("expected snapshot_file table to remain after idempotent rerun")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_path") {
		t.Fatal("expected snapshot_path table to remain after idempotent rerun")
	}

	for _, indexName := range []string{
		"idx_snapshot_created_at",
		"idx_snapshot_parent_id",
		"idx_snapshot_file_snapshot_id",
		"idx_snapshot_file_path_id",
		"idx_snapshot_file_logical_file",
		"idx_snapshot_file_unique",
	} {
		if !sqliteIndexExists(t, dbconn, indexName) {
			t.Fatalf("expected index %s to remain after idempotent rerun", indexName)
		}
	}
}

func TestLoadPostgresSchemaIncludesPhaseOneV8Foundation(t *testing.T) {
	schemaSQL, err := loadPostgresSchema()
	if err != nil {
		t.Fatalf("load postgres schema: %v", err)
	}

	checks := []string{
		"UPDATE schema_version SET version = 9 WHERE version < 9",
		"UPDATE schema_version SET version = 10 WHERE version < 10",
		"ALTER TABLE chunk ADD COLUMN IF NOT EXISTS chunker_version TEXT",
		"ALTER TABLE snapshot ADD COLUMN IF NOT EXISTS parent_id",
		"ON DELETE SET NULL",
		"CREATE TABLE IF NOT EXISTS snapshot_path",
		"ALTER TABLE snapshot_file ADD COLUMN IF NOT EXISTS path_id",
		"DROP INDEX IF EXISTS idx_snapshot_file_path",
		"CREATE INDEX IF NOT EXISTS idx_snapshot_file_path_id ON snapshot_file(path_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_file_unique ON snapshot_file(snapshot_id, path_id)",
	}

	for _, check := range checks {
		if !strings.Contains(schemaSQL, check) {
			t.Fatalf("expected postgres schema to contain %q", check)
		}
	}
}

func TestLoadSQLiteSchemaCreatesPhaseOneV8FreshBootstrap(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	schemaSQL, err := loadSQLiteSchema()
	if err != nil {
		t.Fatalf("load sqlite schema: %v", err)
	}

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable sqlite foreign_keys: %v", err)
	}

	if _, err := dbconn.Exec(schemaSQL); err != nil {
		t.Fatalf("apply sqlite schema directly: %v", err)
	}

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if schemaVersion != 10 {
		t.Fatalf("expected direct sqlite bootstrap schema version 10, got %d", schemaVersion)
	}

	if !sqliteTableExists(t, dbconn, "snapshot") {
		t.Fatal("expected snapshot table in direct sqlite bootstrap")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_path") {
		t.Fatal("expected snapshot_path table in direct sqlite bootstrap")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_file") {
		t.Fatal("expected snapshot_file table in direct sqlite bootstrap")
	}

	if !sqliteTestTableHasColumn(t, dbconn, "snapshot", "parent_id") {
		t.Fatal("expected snapshot.parent_id in direct sqlite bootstrap")
	}
	if !sqliteTestTableHasColumn(t, dbconn, "snapshot_file", "path_id") {
		t.Fatal("expected snapshot_file.path_id in direct sqlite bootstrap")
	}
	if sqliteTestTableHasColumn(t, dbconn, "snapshot_file", "path") {
		t.Fatal("did not expect legacy snapshot_file.path in direct sqlite bootstrap")
	}
}

func TestRunMigrationsMigratesLegacySnapshotV7ToV8WithoutDataLoss(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	legacySchema := `
		PRAGMA foreign_keys = ON;
		CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
		INSERT INTO schema_version(version) VALUES (7);
		CREATE TABLE logical_file (
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
		CREATE TABLE snapshot (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMP NOT NULL,
			type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
			label TEXT
		);
		CREATE INDEX idx_snapshot_created_at ON snapshot(created_at);
		CREATE TABLE snapshot_file (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
			path TEXT NOT NULL CHECK (path != ''),
			logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
			size INTEGER,
			mode INTEGER,
			mtime TIMESTAMP
		);
		CREATE INDEX idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id);
		CREATE INDEX idx_snapshot_file_path ON snapshot_file(path);
		CREATE INDEX idx_snapshot_file_logical_file ON snapshot_file(logical_file_id);
		CREATE UNIQUE INDEX idx_snapshot_file_unique ON snapshot_file(snapshot_id, path);
	`
	if _, err := dbconn.Exec(legacySchema); err != nil {
		t.Fatalf("create legacy snapshot schema v7: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := dbconn.Exec(
			`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
			fmt.Sprintf("f%d.txt", i),
			int64(10+i),
			fmt.Sprintf("hash-%d", i),
			"COMPLETED",
		); err != nil {
			t.Fatalf("insert logical_file row %d: %v", i, err)
		}
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label) VALUES ('snap-a', CURRENT_TIMESTAMP, 'full', 'A')`); err != nil {
		t.Fatalf("insert snapshot snap-a: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label) VALUES ('snap-b', CURRENT_TIMESTAMP, 'partial', 'B')`); err != nil {
		t.Fatalf("insert snapshot snap-b: %v", err)
	}

	seedRows := []struct {
		snapshotID string
		path       string
		logicalID  int
		size       int
	}{
		{snapshotID: "snap-a", path: "docs/a.txt", logicalID: 1, size: 11},
		{snapshotID: "snap-a", path: "docs/b.txt", logicalID: 2, size: 22},
		{snapshotID: "snap-b", path: "docs/a.txt", logicalID: 1, size: 11},
		{snapshotID: "snap-b", path: "img/x.png", logicalID: 3, size: 33},
	}
	for i, row := range seedRows {
		if _, err := dbconn.Exec(
			`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id, size, mode, mtime) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
			row.snapshotID,
			row.path,
			row.logicalID,
			row.size,
			int64(0644),
		); err != nil {
			t.Fatalf("insert snapshot_file seed row %d: %v", i, err)
		}
	}

	var preRowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file`).Scan(&preRowCount); err != nil {
		t.Fatalf("count pre-migration snapshot_file rows: %v", err)
	}

	var preDistinctPaths int
	if err := dbconn.QueryRow(`SELECT COUNT(DISTINCT path) FROM snapshot_file`).Scan(&preDistinctPaths); err != nil {
		t.Fatalf("count pre-migration distinct snapshot_file paths: %v", err)
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations v7->v8: %v", err)
	}

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema version after migration: %v", err)
	}
	if schemaVersion != 10 {
		t.Fatalf("expected schema version 10 after migration, got %d", schemaVersion)
	}

	if !sqliteTestTableHasColumn(t, dbconn, "snapshot", "parent_id") {
		t.Fatal("expected snapshot.parent_id after migration")
	}
	if !sqliteTestTableHasColumn(t, dbconn, "snapshot_file", "path_id") {
		t.Fatal("expected snapshot_file.path_id after migration")
	}
	if sqliteTestTableHasColumn(t, dbconn, "snapshot_file", "path") {
		t.Fatal("did not expect legacy snapshot_file.path after migration")
	}

	var postRowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file`).Scan(&postRowCount); err != nil {
		t.Fatalf("count post-migration snapshot_file rows: %v", err)
	}
	if preRowCount != postRowCount {
		t.Fatalf("snapshot_file row count changed during migration: pre=%d post=%d", preRowCount, postRowCount)
	}

	var postPathCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_path`).Scan(&postPathCount); err != nil {
		t.Fatalf("count snapshot_path rows after migration: %v", err)
	}
	if preDistinctPaths != postPathCount {
		t.Fatalf("snapshot_path normalization mismatch: distinct_pre_paths=%d snapshot_path_rows=%d", preDistinctPaths, postPathCount)
	}

	var unresolvedPathRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		LEFT JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sp.id IS NULL
	`).Scan(&unresolvedPathRefs); err != nil {
		t.Fatalf("count unresolved snapshot_file.path_id references: %v", err)
	}
	if unresolvedPathRefs != 0 {
		t.Fatalf("expected all snapshot_file.path_id values to resolve, found %d unresolved", unresolvedPathRefs)
	}

	var unresolvedSnapshotRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		LEFT JOIN snapshot s ON s.id = sf.snapshot_id
		WHERE s.id IS NULL
	`).Scan(&unresolvedSnapshotRefs); err != nil {
		t.Fatalf("count unresolved snapshot_file.snapshot_id references: %v", err)
	}
	if unresolvedSnapshotRefs != 0 {
		t.Fatalf("expected all snapshot_file.snapshot_id values to resolve, found %d unresolved", unresolvedSnapshotRefs)
	}

	var unresolvedLogicalFileRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		LEFT JOIN logical_file lf ON lf.id = sf.logical_file_id
		WHERE lf.id IS NULL
	`).Scan(&unresolvedLogicalFileRefs); err != nil {
		t.Fatalf("count unresolved snapshot_file.logical_file_id references: %v", err)
	}
	if unresolvedLogicalFileRefs != 0 {
		t.Fatalf("expected all snapshot_file.logical_file_id values to resolve, found %d unresolved", unresolvedLogicalFileRefs)
	}

	var duplicatePairs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT snapshot_id, path_id
			FROM snapshot_file
			GROUP BY snapshot_id, path_id
			HAVING COUNT(*) > 1
		)
	`).Scan(&duplicatePairs); err != nil {
		t.Fatalf("count duplicate (snapshot_id, path_id) pairs: %v", err)
	}
	if duplicatePairs != 0 {
		t.Fatalf("expected no duplicate (snapshot_id, path_id) pairs, found %d", duplicatePairs)
	}

	for i, row := range seedRows {
		var found int
		if err := dbconn.QueryRow(
			`SELECT COUNT(*)
			 FROM snapshot_file sf
			 JOIN snapshot_path sp ON sp.id = sf.path_id
			 WHERE sf.snapshot_id = ? AND sf.logical_file_id = ? AND sf.size = ? AND sp.path = ?`,
			row.snapshotID,
			row.logicalID,
			row.size,
			row.path,
		).Scan(&found); err != nil {
			t.Fatalf("verify migrated row %d: %v", i, err)
		}
		if found != 1 {
			t.Fatalf("expected exactly one migrated row for seed row %d, got %d", i, found)
		}
	}

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT path_id FROM snapshot_file WHERE snapshot_id = ? LIMIT 1), ?)`,
		"snap-a",
		"snap-a",
		3,
	); err == nil {
		t.Fatal("expected uniqueness violation for duplicate (snapshot_id, path_id)")
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "snap-a", 999999, 1); err == nil {
		t.Fatal("expected foreign key failure for unknown snapshot_path id")
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label, parent_id) VALUES ('parent-snap', CURRENT_TIMESTAMP, 'full', 'P', NULL)`); err != nil {
		t.Fatalf("insert parent snapshot with NULL parent_id: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label, parent_id) VALUES ('child-snap', CURRENT_TIMESTAMP, 'partial', 'C', 'parent-snap')`); err != nil {
		t.Fatalf("insert child snapshot with parent_id: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = 'parent-snap'`); err != nil {
		t.Fatalf("delete parent snapshot: %v", err)
	}

	var childParentID sql.NullString
	if err := dbconn.QueryRow(`SELECT parent_id FROM snapshot WHERE id = 'child-snap'`).Scan(&childParentID); err != nil {
		t.Fatalf("read child parent_id after parent delete: %v", err)
	}
	if childParentID.Valid {
		t.Fatalf("expected child parent_id to be NULL after parent delete, got %q", childParentID.String)
	}

	if sqliteIndexExists(t, dbconn, "idx_snapshot_file_path") {
		t.Fatal("legacy idx_snapshot_file_path should be removed after migration")
	}
	if !sqliteIndexExists(t, dbconn, "idx_snapshot_file_path_id") {
		t.Fatal("expected idx_snapshot_file_path_id after migration")
	}
	if !sqliteIndexExists(t, dbconn, "idx_snapshot_file_unique") {
		t.Fatal("expected idx_snapshot_file_unique on (snapshot_id, path_id) after migration")
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("rerun migrations for idempotency: %v", err)
	}

	var postRerunRowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file`).Scan(&postRerunRowCount); err != nil {
		t.Fatalf("count snapshot_file rows after rerun: %v", err)
	}
	if postRerunRowCount != postRowCount {
		t.Fatalf("snapshot_file rows changed after idempotent rerun: before=%d after=%d", postRowCount, postRerunRowCount)
	}
}

func TestRunMigrationsBackfillsChunkerVersionForLegacyLogicalFileAndChunkRows(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	legacySchema := `
		PRAGMA foreign_keys = ON;
		CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
		INSERT INTO schema_version(version) VALUES (8);

		CREATE TABLE container (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			filename TEXT NOT NULL UNIQUE,
			sealed BOOLEAN NOT NULL DEFAULT 0,
			sealing BOOLEAN NOT NULL DEFAULT 0,
			quarantine BOOLEAN NOT NULL DEFAULT 0,
			current_size INTEGER NOT NULL DEFAULT 0,
			max_size INTEGER NOT NULL DEFAULT 1048576,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE logical_file (
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

		CREATE TABLE chunk (
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

		CREATE UNIQUE INDEX idx_chunk_hash_size ON chunk(chunk_hash, size);

		CREATE TABLE file_chunk (
			logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
			chunk_id INTEGER NOT NULL REFERENCES chunk(id),
			chunk_order INTEGER NOT NULL,
			PRIMARY KEY (logical_file_id, chunk_order)
		);

		CREATE TABLE blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chunk_id INTEGER NOT NULL REFERENCES chunk(id),
			codec TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			plaintext_size INTEGER NOT NULL,
			stored_size INTEGER NOT NULL,
			nonce BLOB,
			container_id INTEGER NOT NULL REFERENCES container(id),
			block_offset INTEGER NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(chunk_id, container_id, block_offset)
		);
	`
	if _, err := dbconn.Exec(legacySchema); err != nil {
		t.Fatalf("create legacy pre-v1.5 schema: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, sealed, current_size, max_size)
		 VALUES (?, 1, ?, ?) RETURNING id`,
		"migration-pre-v1.5.bin",
		4096,
		1048576,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert legacy container: %v", err)
	}

	var logicalFileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES (?, ?, ?, ?, ?) RETURNING id`,
		"legacy-file.bin",
		int64(11),
		"legacy-file-hash",
		"COMPLETED",
		1,
	).Scan(&logicalFileID); err != nil {
		t.Fatalf("insert legacy logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES (?, ?, ?, ?, ?) RETURNING id`,
		"legacy-chunk-hash",
		int64(11),
		"COMPLETED",
		1,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert legacy chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, 0)`,
		logicalFileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert legacy file_chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES (?, 'plain', 1, 11, 11, x'', ?, 0)`,
		chunkID,
		containerID,
	); err != nil {
		t.Fatalf("insert legacy block: %v", err)
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations pre-v1.5 -> current: %v", err)
	}

	if !sqliteTestTableHasColumn(t, dbconn, "logical_file", "chunker_version") {
		t.Fatal("expected logical_file.chunker_version after migration")
	}
	if !sqliteTestTableHasColumn(t, dbconn, "chunk", "chunker_version") {
		t.Fatal("expected chunk.chunker_version after migration")
	}

	var logicalFileVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = ?`, logicalFileID).Scan(&logicalFileVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if logicalFileVersion != "v1-simple-rolling" {
		t.Fatalf("expected logical_file chunker_version=v1-simple-rolling, got %q", logicalFileVersion)
	}

	var chunkVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE id = ?`, chunkID).Scan(&chunkVersion); err != nil {
		t.Fatalf("read chunk.chunker_version: %v", err)
	}
	if chunkVersion != "v1-simple-rolling" {
		t.Fatalf("expected chunk chunker_version=v1-simple-rolling, got %q", chunkVersion)
	}

	var nullOrEmptyLogicalVersions int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE chunker_version IS NULL OR TRIM(chunker_version) = ''`).Scan(&nullOrEmptyLogicalVersions); err != nil {
		t.Fatalf("count null/empty logical_file.chunker_version: %v", err)
	}
	if nullOrEmptyLogicalVersions != 0 {
		t.Fatalf("expected no null/empty logical_file.chunker_version rows, got %d", nullOrEmptyLogicalVersions)
	}

	var nullOrEmptyChunkVersions int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunker_version IS NULL OR TRIM(chunker_version) = ''`).Scan(&nullOrEmptyChunkVersions); err != nil {
		t.Fatalf("count null/empty chunk.chunker_version: %v", err)
	}
	if nullOrEmptyChunkVersions != 0 {
		t.Fatalf("expected no null/empty chunk.chunker_version rows, got %d", nullOrEmptyChunkVersions)
	}

	// Lightweight readability/restorability proof: the legacy linked file/chunk/block/container
	// graph remains queryable via the core restore join shape after migration.
	var joinedRows int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = ? AND c.status = 'COMPLETED' AND ctr.quarantine = 0
	`, logicalFileID).Scan(&joinedRows); err != nil {
		t.Fatalf("query restore-style chunk graph after migration: %v", err)
	}
	if joinedRows != 1 {
		t.Fatalf("expected one restorable chunk graph row after migration, got %d", joinedRows)
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("rerun migrations for idempotency: %v", err)
	}
}

func TestPostgresFreshBootstrapCreatesPhaseOneV8Schema(t *testing.T) {
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run live postgres migration tests")
	}

	host := getenvOrDefault("DB_HOST", "127.0.0.1")
	port := getenvOrDefault("DB_PORT", "5432")
	user := getenvOrDefault("DB_USER", "coldkeep")
	password := getenvOrDefault("DB_PASSWORD", "coldkeep")
	sslMode := getenvOrDefault("DB_SSLMODE", "disable")
	maintenanceDB := getenvOrDefault("COLDKEEP_TEST_DB_MAINTENANCE", "postgres")

	adminConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		host,
		port,
		user,
		password,
		maintenanceDB,
		sslMode,
	)
	adminDB, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		t.Fatalf("open postgres admin connection: %v", err)
	}
	defer func() { _ = adminDB.Close() }()

	if err := adminDB.Ping(); err != nil {
		t.Fatalf("ping postgres admin connection: %v", err)
	}

	testDBName := fmt.Sprintf("coldkeep_phase1_v8_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		t.Fatalf("create temporary postgres database %s: %v", testDBName, err)
	}
	defer func() {
		_, _ = adminDB.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, testDBName)
		_, _ = adminDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
	}()

	t.Setenv("DB_HOST", host)
	t.Setenv("DB_PORT", port)
	t.Setenv("DB_USER", user)
	t.Setenv("DB_PASSWORD", password)
	t.Setenv("DB_SSLMODE", sslMode)
	t.Setenv("DB_NAME", testDBName)
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")

	dbconn, err := ConnectDB()
	if err != nil {
		t.Fatalf("connect db with auto-bootstrap: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema_version after bootstrap: %v", err)
	}
	if schemaVersion != 10 {
		t.Fatalf("expected schema_version=10 after fresh postgres bootstrap, got %d", schemaVersion)
	}

	for _, tableName := range []string{"snapshot", "snapshot_path", "snapshot_file"} {
		var exists bool
		if err := dbconn.QueryRow(`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`, tableName).Scan(&exists); err != nil {
			t.Fatalf("check table existence %s: %v", tableName, err)
		}
		if !exists {
			t.Fatalf("expected table %s to exist after bootstrap", tableName)
		}
	}

	var hasParentID bool
	if err := dbconn.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'snapshot' AND column_name = 'parent_id'
		)
	`).Scan(&hasParentID); err != nil {
		t.Fatalf("check snapshot.parent_id column: %v", err)
	}
	if !hasParentID {
		t.Fatal("expected snapshot.parent_id column")
	}

	var hasPathID bool
	if err := dbconn.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'snapshot_file' AND column_name = 'path_id'
		)
	`).Scan(&hasPathID); err != nil {
		t.Fatalf("check snapshot_file.path_id column: %v", err)
	}
	if !hasPathID {
		t.Fatal("expected snapshot_file.path_id column")
	}

	var hasLegacyPath bool
	if err := dbconn.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'snapshot_file' AND column_name = 'path'
		)
	`).Scan(&hasLegacyPath); err != nil {
		t.Fatalf("check legacy snapshot_file.path column: %v", err)
	}
	if hasLegacyPath {
		t.Fatal("did not expect legacy snapshot_file.path column in fresh v8 bootstrap")
	}

	var hasOldPathIndex bool
	if err := dbconn.QueryRow(`SELECT to_regclass('public.idx_snapshot_file_path') IS NOT NULL`).Scan(&hasOldPathIndex); err != nil {
		t.Fatalf("check old idx_snapshot_file_path: %v", err)
	}
	if hasOldPathIndex {
		t.Fatal("did not expect legacy idx_snapshot_file_path index in fresh v8 bootstrap")
	}

	var hasPathIDIndex bool
	if err := dbconn.QueryRow(`SELECT to_regclass('public.idx_snapshot_file_path_id') IS NOT NULL`).Scan(&hasPathIDIndex); err != nil {
		t.Fatalf("check idx_snapshot_file_path_id: %v", err)
	}
	if !hasPathIDIndex {
		t.Fatal("expected idx_snapshot_file_path_id index")
	}

	var hasUniqueIndex bool
	if err := dbconn.QueryRow(`SELECT to_regclass('public.idx_snapshot_file_unique') IS NOT NULL`).Scan(&hasUniqueIndex); err != nil {
		t.Fatalf("check idx_snapshot_file_unique: %v", err)
	}
	if !hasUniqueIndex {
		t.Fatal("expected idx_snapshot_file_unique index")
	}

	if _, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES ('p1.txt', 10, 'phase1-hash-1', 'COMPLETED')`); err != nil {
		t.Fatalf("insert logical_file row 1: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES ('p2.txt', 20, 'phase1-hash-2', 'COMPLETED')`); err != nil {
		t.Fatalf("insert logical_file row 2: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label, parent_id) VALUES ('snap-parent', NOW(), 'full', 'parent', NULL)`); err != nil {
		t.Fatalf("insert parent snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label, parent_id) VALUES ('snap-child', NOW(), 'partial', 'child', 'snap-parent')`); err != nil {
		t.Fatalf("insert child snapshot: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_path(path) VALUES ('docs/a.txt')`); err != nil {
		t.Fatalf("insert snapshot_path docs/a.txt: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ('snap-parent', (SELECT id FROM snapshot_path WHERE path = 'docs/a.txt'), 1, 10)`); err != nil {
		t.Fatalf("insert snapshot_file parent row: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ('snap-child', (SELECT id FROM snapshot_path WHERE path = 'docs/a.txt'), 2, 20)`); err != nil {
		t.Fatalf("insert snapshot_file child row with shared path_id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-parent', (SELECT id FROM snapshot_path WHERE path = 'docs/a.txt'), 2)`); err == nil {
		t.Fatal("expected unique violation on duplicate (snapshot_id, path_id)")
	}

	// Remove snapshot_file rows belonging to snap-parent before deleting the snapshot;
	// snapshot_file.snapshot_id has no ON DELETE CASCADE, so we clear them first.
	if _, err := dbconn.Exec(`DELETE FROM snapshot_file WHERE snapshot_id = 'snap-parent'`); err != nil {
		t.Fatalf("delete snapshot_file rows for parent snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = 'snap-parent'`); err != nil {
		t.Fatalf("delete parent snapshot: %v", err)
	}

	var childParentID sql.NullString
	if err := dbconn.QueryRow(`SELECT parent_id FROM snapshot WHERE id = 'snap-child'`).Scan(&childParentID); err != nil {
		t.Fatalf("read child parent_id after parent delete: %v", err)
	}
	if childParentID.Valid {
		t.Fatalf("expected child parent_id NULL after deleting parent, got %q", childParentID.String)
	}

	schemaSQL, err := loadPostgresSchema()
	if err != nil {
		t.Fatalf("load postgres schema for idempotency rerun: %v", err)
	}
	if _, err := dbconn.Exec(schemaSQL); err != nil {
		t.Fatalf("rerun postgres schema SQL for idempotency: %v", err)
	}

	var pathRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_path WHERE path = 'docs/a.txt'`).Scan(&pathRows); err != nil {
		t.Fatalf("count normalized path rows after schema rerun: %v", err)
	}
	if pathRows != 1 {
		t.Fatalf("expected exactly one normalized snapshot_path row after rerun, got %d", pathRows)
	}
}

func TestPostgresV7SnapshotMigrationToV8WithoutDataLoss(t *testing.T) {
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run live postgres migration tests")
	}

	host := getenvOrDefault("DB_HOST", "127.0.0.1")
	port := getenvOrDefault("DB_PORT", "5432")
	user := getenvOrDefault("DB_USER", "coldkeep")
	password := getenvOrDefault("DB_PASSWORD", "coldkeep")
	sslMode := getenvOrDefault("DB_SSLMODE", "disable")
	maintenanceDB := getenvOrDefault("COLDKEEP_TEST_DB_MAINTENANCE", "postgres")

	adminConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		host,
		port,
		user,
		password,
		maintenanceDB,
		sslMode,
	)
	adminDB, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		t.Fatalf("open postgres admin connection: %v", err)
	}
	defer func() { _ = adminDB.Close() }()

	if err := adminDB.Ping(); err != nil {
		t.Fatalf("ping postgres admin connection: %v", err)
	}

	testDBName := fmt.Sprintf("coldkeep_phase1_v7_to_v8_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		t.Fatalf("create temporary postgres database %s: %v", testDBName, err)
	}
	defer func() {
		_, _ = adminDB.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, testDBName)
		_, _ = adminDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
	}()

	testConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		host,
		port,
		user,
		password,
		testDBName,
		sslMode,
	)
	dbconn, err := sql.Open("postgres", testConnStr)
	if err != nil {
		t.Fatalf("open postgres test database connection: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := dbconn.Ping(); err != nil {
		t.Fatalf("ping postgres test database connection: %v", err)
	}

	legacyV7SQL := `
		CREATE TABLE schema_version (
			version INTEGER PRIMARY KEY
		);
		INSERT INTO schema_version(version) VALUES (7);

		CREATE TABLE logical_file (
			id BIGSERIAL PRIMARY KEY
		);

		CREATE TABLE snapshot (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL,
			type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
			label TEXT
		);

		CREATE INDEX idx_snapshot_created_at ON snapshot(created_at);

		CREATE TABLE snapshot_file (
			id BIGSERIAL PRIMARY KEY,
			snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
			path TEXT NOT NULL CHECK (path <> ''),
			logical_file_id BIGINT NOT NULL REFERENCES logical_file(id),
			size BIGINT,
			mode BIGINT,
			mtime TIMESTAMPTZ
		);

		CREATE INDEX idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id);
		CREATE INDEX idx_snapshot_file_path ON snapshot_file(path);
		CREATE INDEX idx_snapshot_file_logical_file ON snapshot_file(logical_file_id);
		CREATE UNIQUE INDEX idx_snapshot_file_unique ON snapshot_file(snapshot_id, path);
	`
	if _, err := dbconn.Exec(legacyV7SQL); err != nil {
		t.Fatalf("create legacy postgres v7 snapshot schema: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO logical_file(id) VALUES (1), (2), (3)`); err != nil {
		t.Fatalf("insert legacy logical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label) VALUES ('snap-a', NOW(), 'full', 'A'), ('snap-b', NOW(), 'partial', 'B')`); err != nil {
		t.Fatalf("insert legacy snapshot rows: %v", err)
	}

	type seedRow struct {
		snapshotID string
		path       string
		logicalID  int
		size       int
	}
	seedRows := []seedRow{
		{snapshotID: "snap-a", path: "docs/a.txt", logicalID: 1, size: 11},
		{snapshotID: "snap-a", path: "docs/b.txt", logicalID: 2, size: 22},
		{snapshotID: "snap-b", path: "docs/a.txt", logicalID: 1, size: 11},
		{snapshotID: "snap-b", path: "img/x.png", logicalID: 3, size: 33},
	}
	for i, row := range seedRows {
		if _, err := dbconn.Exec(
			`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id, size, mode, mtime) VALUES ($1, $2, $3, $4, $5, NOW())`,
			row.snapshotID,
			row.path,
			row.logicalID,
			row.size,
			int64(0644),
		); err != nil {
			t.Fatalf("insert legacy snapshot_file row %d: %v", i, err)
		}
	}

	var preRowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file`).Scan(&preRowCount); err != nil {
		t.Fatalf("count pre-migration snapshot_file rows: %v", err)
	}

	var preDistinctPaths int
	if err := dbconn.QueryRow(`SELECT COUNT(DISTINCT path) FROM snapshot_file`).Scan(&preDistinctPaths); err != nil {
		t.Fatalf("count pre-migration distinct snapshot_file paths: %v", err)
	}

	schemaSQL, err := loadPostgresSchema()
	if err != nil {
		t.Fatalf("load postgres schema SQL: %v", err)
	}
	v8MigrationSQL, err := extractPostgresV8MigrationSQL(schemaSQL)
	if err != nil {
		t.Fatalf("extract postgres v8 migration SQL block: %v", err)
	}

	if _, err := dbconn.Exec(v8MigrationSQL); err != nil {
		t.Fatalf("apply postgres v8 migration block: %v", err)
	}

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema_version after migration: %v", err)
	}
	if schemaVersion != 8 {
		t.Fatalf("expected schema_version=8 after v8 migration block, got %d", schemaVersion)
	}

	var hasPathID bool
	if err := dbconn.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'snapshot_file' AND column_name = 'path_id'
		)
	`).Scan(&hasPathID); err != nil {
		t.Fatalf("check snapshot_file.path_id existence: %v", err)
	}
	if !hasPathID {
		t.Fatal("expected snapshot_file.path_id after migration")
	}

	var hasLegacyPath bool
	if err := dbconn.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'snapshot_file' AND column_name = 'path'
		)
	`).Scan(&hasLegacyPath); err != nil {
		t.Fatalf("check legacy snapshot_file.path existence: %v", err)
	}
	if hasLegacyPath {
		t.Fatal("did not expect legacy snapshot_file.path after migration")
	}

	var postRowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file`).Scan(&postRowCount); err != nil {
		t.Fatalf("count post-migration snapshot_file rows: %v", err)
	}
	if postRowCount != preRowCount {
		t.Fatalf("snapshot_file row count changed after migration: pre=%d post=%d", preRowCount, postRowCount)
	}

	var postPathCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_path`).Scan(&postPathCount); err != nil {
		t.Fatalf("count snapshot_path rows after migration: %v", err)
	}
	if postPathCount != preDistinctPaths {
		t.Fatalf("snapshot_path normalization mismatch: preDistinct=%d snapshotPathRows=%d", preDistinctPaths, postPathCount)
	}

	var unresolvedPathRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		LEFT JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sp.id IS NULL
	`).Scan(&unresolvedPathRefs); err != nil {
		t.Fatalf("count unresolved snapshot_file.path_id references: %v", err)
	}
	if unresolvedPathRefs != 0 {
		t.Fatalf("expected all snapshot_file.path_id values to resolve, found %d unresolved", unresolvedPathRefs)
	}

	var unresolvedSnapshotRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		LEFT JOIN snapshot s ON s.id = sf.snapshot_id
		WHERE s.id IS NULL
	`).Scan(&unresolvedSnapshotRefs); err != nil {
		t.Fatalf("count unresolved snapshot_file.snapshot_id references: %v", err)
	}
	if unresolvedSnapshotRefs != 0 {
		t.Fatalf("expected all snapshot_file.snapshot_id values to resolve, found %d unresolved", unresolvedSnapshotRefs)
	}

	var unresolvedLogicalFileRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		LEFT JOIN logical_file lf ON lf.id = sf.logical_file_id
		WHERE lf.id IS NULL
	`).Scan(&unresolvedLogicalFileRefs); err != nil {
		t.Fatalf("count unresolved snapshot_file.logical_file_id references: %v", err)
	}
	if unresolvedLogicalFileRefs != 0 {
		t.Fatalf("expected all snapshot_file.logical_file_id values to resolve, found %d unresolved", unresolvedLogicalFileRefs)
	}

	var duplicatePairs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT snapshot_id, path_id
			FROM snapshot_file
			GROUP BY snapshot_id, path_id
			HAVING COUNT(*) > 1
		) dup
	`).Scan(&duplicatePairs); err != nil {
		t.Fatalf("count duplicate (snapshot_id, path_id) pairs: %v", err)
	}
	if duplicatePairs != 0 {
		t.Fatalf("expected no duplicate (snapshot_id, path_id) pairs, found %d", duplicatePairs)
	}

	for i, row := range seedRows {
		var found int
		if err := dbconn.QueryRow(
			`SELECT COUNT(*)
			 FROM snapshot_file sf
			 JOIN snapshot_path sp ON sp.id = sf.path_id
			 WHERE sf.snapshot_id = $1 AND sf.logical_file_id = $2 AND sf.size = $3 AND sp.path = $4`,
			row.snapshotID,
			row.logicalID,
			row.size,
			row.path,
		).Scan(&found); err != nil {
			t.Fatalf("verify migrated row %d: %v", i, err)
		}
		if found != 1 {
			t.Fatalf("expected exactly one migrated row for seed row %d, got %d", i, found)
		}
	}

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-a', (SELECT path_id FROM snapshot_file WHERE snapshot_id = 'snap-a' LIMIT 1), 3)`,
	); err == nil {
		t.Fatal("expected unique violation on duplicate (snapshot_id, path_id)")
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-a', 999999, 1)`); err == nil {
		t.Fatal("expected foreign key violation on unknown path_id")
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label, parent_id) VALUES ('p-parent', NOW(), 'full', 'PP', NULL)`); err != nil {
		t.Fatalf("insert parent snapshot with NULL parent_id: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, created_at, type, label, parent_id) VALUES ('p-child', NOW(), 'partial', 'PC', 'p-parent')`); err != nil {
		t.Fatalf("insert child snapshot with parent_id: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = 'p-parent'`); err != nil {
		t.Fatalf("delete parent snapshot: %v", err)
	}

	var childParentID sql.NullString
	if err := dbconn.QueryRow(`SELECT parent_id FROM snapshot WHERE id = 'p-child'`).Scan(&childParentID); err != nil {
		t.Fatalf("read child parent_id after deleting parent: %v", err)
	}
	if childParentID.Valid {
		t.Fatalf("expected child parent_id to be NULL after parent delete, got %q", childParentID.String)
	}

	var hasOldPathIndex bool
	if err := dbconn.QueryRow(`SELECT to_regclass('public.idx_snapshot_file_path') IS NOT NULL`).Scan(&hasOldPathIndex); err != nil {
		t.Fatalf("check legacy path index presence: %v", err)
	}
	if hasOldPathIndex {
		t.Fatal("did not expect legacy idx_snapshot_file_path after migration")
	}

	var hasPathIDIndex bool
	if err := dbconn.QueryRow(`SELECT to_regclass('public.idx_snapshot_file_path_id') IS NOT NULL`).Scan(&hasPathIDIndex); err != nil {
		t.Fatalf("check idx_snapshot_file_path_id presence: %v", err)
	}
	if !hasPathIDIndex {
		t.Fatal("expected idx_snapshot_file_path_id after migration")
	}

	if _, err := dbconn.Exec(v8MigrationSQL); err != nil {
		t.Fatalf("rerun postgres v8 migration block for idempotency: %v", err)
	}

	var postRerunRowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file`).Scan(&postRerunRowCount); err != nil {
		t.Fatalf("count snapshot_file rows after idempotent rerun: %v", err)
	}
	if postRerunRowCount != postRowCount {
		t.Fatalf("snapshot_file row count changed after idempotent rerun: before=%d after=%d", postRowCount, postRerunRowCount)
	}
}

func extractPostgresV8MigrationSQL(schemaSQL string) (string, error) {
	start := strings.Index(schemaSQL, "-- Schema version 8:")
	if start < 0 {
		return "", fmt.Errorf("schema version 8 marker not found")
	}
	v8AndAfter := schemaSQL[start:]
	end := strings.Index(v8AndAfter, "\n-- Schema version 9:")
	if end < 0 {
		end = strings.Index(v8AndAfter, "\nCOMMIT;")
		if end < 0 {
			return "", fmt.Errorf("schema version 8 block terminator not found")
		}
	}
	return strings.TrimSpace(v8AndAfter[:end]), nil
}

func getenvOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func TestLoadPostgresAutoBootstrapEnabledReadsCurrentEnv(t *testing.T) {
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "false")
	if loadPostgresAutoBootstrapEnabled() {
		t.Fatal("expected auto-bootstrap to be disabled")
	}

	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	if !loadPostgresAutoBootstrapEnabled() {
		t.Fatal("expected auto-bootstrap to be enabled after env change")
	}

	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", " 'On' ")
	if !loadPostgresAutoBootstrapEnabled() {
		t.Fatal("expected quoted mixed-case truthy env value to be enabled")
	}
}

func TestRunMigrationsBackfillsPhysicalFileForLegacyLogicalFiles(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	legacySchema := `
		CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
		INSERT INTO schema_version(version) VALUES (5);
		CREATE TABLE logical_file (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			original_name TEXT NOT NULL,
			total_size INTEGER NOT NULL,
			file_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := dbconn.Exec(legacySchema); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}

	for i := 0; i < 3; i++ {
		name := "same-name.txt"
		if i == 2 {
			name = ""
		}
		if _, err := dbconn.Exec(
			`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
			name,
			int64(128+i),
			fmt.Sprintf("hash-%d", i),
			"COMPLETED",
		); err != nil {
			t.Fatalf("insert legacy logical_file row %d: %v", i, err)
		}
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations first pass: %v", err)
	}

	var refCount int
	if err := dbconn.QueryRow(`
		SELECT ref_count
		FROM logical_file
		ORDER BY id ASC
		LIMIT 1
	`).Scan(&refCount); err != nil {
		t.Fatalf("query migrated logical_file columns: %v", err)
	}
	if refCount != 1 {
		t.Fatalf("unexpected ref_count after migration: got %d want 1", refCount)
	}

	var physicalCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file`).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical_file rows: %v", err)
	}
	if physicalCount != 3 {
		t.Fatalf("expected one physical_file row per logical_file row, got %d", physicalCount)
	}

	var duplicatePaths int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT path FROM physical_file GROUP BY path HAVING COUNT(*) > 1
		)
	`).Scan(&duplicatePaths); err != nil {
		t.Fatalf("check duplicate paths: %v", err)
	}
	if duplicatePaths != 0 {
		t.Fatalf("expected unique migrated paths, found %d duplicates", duplicatePaths)
	}

	var prefixedPaths int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path LIKE '/migrated/%'`).Scan(&prefixedPaths); err != nil {
		t.Fatalf("count migrated path prefix matches: %v", err)
	}
	if prefixedPaths != 3 {
		t.Fatalf("expected all paths under /migrated, got %d", prefixedPaths)
	}

	var physicalMetadataComplete int
	if err := dbconn.QueryRow(`
		SELECT is_metadata_complete
		FROM physical_file
		ORDER BY logical_file_id ASC
		LIMIT 1
	`).Scan(&physicalMetadataComplete); err != nil {
		t.Fatalf("query physical_file metadata completion flag: %v", err)
	}
	if physicalMetadataComplete != 0 {
		t.Fatalf("unexpected physical_file.is_metadata_complete after migration: got %d want 0", physicalMetadataComplete)
	}

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if schemaVersion < 6 {
		t.Fatalf("expected schema version >= 6 after migration, got %d", schemaVersion)
	}

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations second pass (idempotency): %v", err)
	}

	var physicalCountAfterSecondRun int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file`).Scan(&physicalCountAfterSecondRun); err != nil {
		t.Fatalf("count physical_file rows after second run: %v", err)
	}
	if physicalCountAfterSecondRun != physicalCount {
		t.Fatalf("expected idempotent physical_file backfill, got %d then %d", physicalCount, physicalCountAfterSecondRun)
	}
}

func TestRunMigrationsAllowsMultiplePhysicalFilesPerLogicalFile(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
		"data.bin",
		int64(64),
		"hash-shared",
		"COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file row: %v", err)
	}

	logicalID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted logical_file id: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, ?)`,
		"/user/a/data.bin",
		logicalID,
		1,
	); err != nil {
		t.Fatalf("insert first physical_file row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, ?)`,
		"/user/b/data.bin",
		logicalID,
		1,
	); err != nil {
		t.Fatalf("insert second physical_file row for same logical_file: %v", err)
	}

	var mappedCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = ?`, logicalID).Scan(&mappedCount); err != nil {
		t.Fatalf("count physical_file rows for logical_file: %v", err)
	}
	if mappedCount != 2 {
		t.Fatalf("expected 2 physical_file rows for the same logical_file, got %d", mappedCount)
	}
}

func TestRunMigrationsRejectsEmptyPhysicalFilePath(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
		"tiny.txt",
		int64(4),
		"hash-tiny",
		"COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file row: %v", err)
	}

	logicalID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted logical_file id: %v", err)
	}

	_, err = dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, ?)`,
		"",
		logicalID,
		1,
	)
	if err == nil {
		t.Fatalf("expected empty path insert to fail")
	}
}
