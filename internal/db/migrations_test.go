package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"

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

func TestRunMigrationsCreatesSnapshotSchemaVersionSeven(t *testing.T) {
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
	if schemaVersion != 7 {
		t.Fatalf("expected schema version 7 after first migration pass, got %d", schemaVersion)
	}

	if !sqliteTableExists(t, dbconn, "snapshot") {
		t.Fatal("expected snapshot table to exist after migration")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_file") {
		t.Fatal("expected snapshot_file table to exist after migration")
	}

	for _, indexName := range []string{
		"idx_snapshot_created_at",
		"idx_snapshot_file_snapshot_id",
		"idx_snapshot_file_path",
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
	if schemaVersionAfterSecondRun != 7 {
		t.Fatalf("expected schema version to stay 7 after idempotent rerun, got %d", schemaVersionAfterSecondRun)
	}

	if !sqliteTableExists(t, dbconn, "snapshot") {
		t.Fatal("expected snapshot table to remain after idempotent rerun")
	}
	if !sqliteTableExists(t, dbconn, "snapshot_file") {
		t.Fatal("expected snapshot_file table to remain after idempotent rerun")
	}

	for _, indexName := range []string{
		"idx_snapshot_created_at",
		"idx_snapshot_file_snapshot_id",
		"idx_snapshot_file_path",
		"idx_snapshot_file_logical_file",
		"idx_snapshot_file_unique",
	} {
		if !sqliteIndexExists(t, dbconn, indexName) {
			t.Fatalf("expected index %s to remain after idempotent rerun", indexName)
		}
	}
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
