package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	dbschema "github.com/franchoy/coldkeep/db"
)

const requiredPostgresSchemaVersion = 7

func sqliteTableHasColumn(dbconn *sql.DB, ctx context.Context, tableName, columnName string) (bool, error) {
	rows, err := dbconn.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return false, err
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
			return false, err
		}
		if strings.EqualFold(name, columnName) {
			return true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return false, err
	}

	return false, nil
}

func sqliteTableSQL(dbconn *sql.DB, ctx context.Context, tableName string) (string, error) {
	var sqlText sql.NullString
	err := dbconn.QueryRowContext(
		ctx,
		`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`,
		tableName,
	).Scan(&sqlText)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if !sqlText.Valid {
		return "", nil
	}
	return sqlText.String, nil
}

func sqlitePhysicalFileNeedsRebuild(tableSQL string) bool {
	if tableSQL == "" {
		return false
	}
	normalized := strings.ToLower(tableSQL)
	return strings.Contains(normalized, "logical_file_id integer not null unique") ||
		!strings.Contains(normalized, "check (path != '')")
}

func rebuildSQLitePhysicalFileTable(dbconn *sql.DB, ctx context.Context) error {
	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE physical_file_v2 (
			path TEXT PRIMARY KEY CHECK (path != ''),
			logical_file_id INTEGER NOT NULL
				REFERENCES logical_file(id) ON DELETE CASCADE,
			mode INTEGER,
			mtime DATETIME,
			uid INTEGER,
			gid INTEGER,
			is_metadata_complete INTEGER NOT NULL DEFAULT 0 CHECK (is_metadata_complete IN (0, 1))
		)
	`); err != nil {
		return fmt.Errorf("create physical_file_v2 table: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT INTO physical_file_v2 (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete)
		SELECT path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete
		FROM physical_file
	`); err != nil {
		return fmt.Errorf("copy physical_file rows into v2: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `DROP TABLE physical_file`); err != nil {
		return fmt.Errorf("drop legacy physical_file table: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `ALTER TABLE physical_file_v2 RENAME TO physical_file`); err != nil {
		return fmt.Errorf("rename physical_file_v2 table: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_physical_file_logical_file_id ON physical_file(logical_file_id)`); err != nil {
		return fmt.Errorf("create physical_file logical_file_id index: %w", err)
	}

	return nil
}

func runSQLitePhysicalFileMigration(dbconn *sql.DB, ctx context.Context) error {
	hasRefCount, err := sqliteTableHasColumn(dbconn, ctx, "logical_file", "ref_count")
	if err != nil {
		return fmt.Errorf("inspect logical_file.ref_count: %w", err)
	}
	if !hasRefCount {
		if _, err := dbconn.ExecContext(ctx, `ALTER TABLE logical_file ADD COLUMN ref_count INTEGER NOT NULL DEFAULT 1 CHECK (ref_count >= 0)`); err != nil {
			return fmt.Errorf("add logical_file.ref_count: %w", err)
		}
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS physical_file (
			path TEXT PRIMARY KEY CHECK (path != ''),
			logical_file_id INTEGER NOT NULL
				REFERENCES logical_file(id) ON DELETE CASCADE,
			mode INTEGER,
			mtime DATETIME,
			uid INTEGER,
			gid INTEGER,
			is_metadata_complete INTEGER NOT NULL DEFAULT 0 CHECK (is_metadata_complete IN (0, 1))
		)
	`); err != nil {
		return fmt.Errorf("create physical_file table: %w", err)
	}

	tableSQL, err := sqliteTableSQL(dbconn, ctx, "physical_file")
	if err != nil {
		return fmt.Errorf("read physical_file schema: %w", err)
	}
	if sqlitePhysicalFileNeedsRebuild(tableSQL) {
		if err := rebuildSQLitePhysicalFileTable(dbconn, ctx); err != nil {
			return err
		}
	}

	if _, err := dbconn.ExecContext(ctx, `
		UPDATE logical_file
		SET ref_count = 1
		WHERE ref_count IS NULL OR ref_count < 1
	`); err != nil {
		return fmt.Errorf("backfill logical_file.ref_count: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete)
		SELECT
			'/migrated/' ||
			CASE
				WHEN TRIM(COALESCE(original_name, '')) = '' THEN 'file'
				ELSE TRIM(original_name)
			END || '-' || CAST(id AS TEXT),
			id,
			NULL,
			NULL,
			NULL,
			NULL,
			0
		FROM logical_file
	`); err != nil {
		return fmt.Errorf("backfill physical_file: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		UPDATE schema_version
		SET version = 6
		WHERE version < 6
	`); err != nil {
		return fmt.Errorf("update sqlite schema_version to 6: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_version(version) VALUES (6)
	`); err != nil {
		return fmt.Errorf("insert sqlite schema_version 6: %w", err)
	}

	return nil
}

func runSQLiteSnapshotMigration(dbconn *sql.DB, ctx context.Context) error {
	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS snapshot (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMP NOT NULL,
			type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
			label TEXT
		)
	`); err != nil {
		return fmt.Errorf("create snapshot table: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_created_at ON snapshot(created_at)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_created_at: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS snapshot_file (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
			path TEXT NOT NULL CHECK (path != ''),
			logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
			size INTEGER,
			mode INTEGER,
			mtime TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create snapshot_file table: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_snapshot_id: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_file_path ON snapshot_file(path)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_path: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_file_logical_file ON snapshot_file(logical_file_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_logical_file: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_file_unique ON snapshot_file(snapshot_id, path)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_unique: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		DELETE FROM schema_version WHERE version < 7
	`); err != nil {
		return fmt.Errorf("clean sqlite schema_version before 7: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_version(version) VALUES (7)
	`); err != nil {
		return fmt.Errorf("insert sqlite schema_version 7: %w", err)
	}

	return nil
}

func loadSQLiteSchema() (string, error) {
	if dbschema.SQLiteSchema == "" {
		return "", errors.New("embedded sqlite schema is empty")
	}
	return dbschema.SQLiteSchema, nil
}

func loadPostgresSchema() (string, error) {
	if dbschema.PostgresSchema == "" {
		return "", errors.New("embedded postgres schema is empty")
	}
	return dbschema.PostgresSchema, nil
}

func loadPostgresAutoBootstrapEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("COLDKEEP_DB_AUTO_BOOTSTRAP"))
	raw = strings.Trim(raw, "\"'")
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func ensurePostgresVersion(dbconn *sql.DB, ctx context.Context) error {
	var version int
	err := dbconn.QueryRowContext(ctx, `SELECT version FROM schema_version ORDER BY version DESC LIMIT 1`).Scan(&version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("schema_version table is empty")
		}
		return fmt.Errorf("query schema_version: %w", err)
	}
	if version < requiredPostgresSchemaVersion {
		return fmt.Errorf(
			"postgres schema version too old: have %d, need at least %d; apply db/schema_postgres.sql",
			version,
			requiredPostgresSchemaVersion,
		)
	}
	return nil
}

// EnsurePostgresSchema validates the runtime PostgreSQL schema.
// If COLDKEEP_DB_AUTO_BOOTSTRAP is enabled and schema_version is missing,
// it bootstraps by applying the embedded db/schema_postgres.sql.
func EnsurePostgresSchema(dbconn *sql.DB) error {
	if dbconn == nil {
		return errors.New("nil DB connection")
	}

	ctx, cancel := NewOperationContext(context.Background())
	defer cancel()

	var schemaVersionTable sql.NullString
	if err := dbconn.QueryRowContext(ctx, `SELECT to_regclass('public.schema_version')`).Scan(&schemaVersionTable); err != nil {
		return fmt.Errorf("check schema_version table: %w", err)
	}

	if !schemaVersionTable.Valid {
		if !loadPostgresAutoBootstrapEnabled() {
			return errors.New(
				"postgres schema is not initialized (missing schema_version table); apply db/schema_postgres.sql or set COLDKEEP_DB_AUTO_BOOTSTRAP=true",
			)
		}

		schemaSQL, err := loadPostgresSchema()
		if err != nil {
			return err
		}
		if _, err := dbconn.ExecContext(ctx, schemaSQL); err != nil {
			return fmt.Errorf("bootstrap postgres schema: %w", err)
		}
	}

	if err := ensurePostgresVersion(dbconn, ctx); err != nil {
		return err
	}

	return nil
}

// RunMigrations applies the embedded SQLite schema to a DB connection.
// It is intended for simulated/local SQLite contexts only.
func RunMigrations(dbconn *sql.DB) error {
	if dbconn == nil {
		return errors.New("nil DB connection")
	}
	if backend := BackendFromDB(dbconn); backend != BackendSQLite {
		return fmt.Errorf("RunMigrations requires sqlite backend, got %s", backend)
	}
	ctx, cancel := NewOperationContext(context.Background())
	defer cancel()

	if _, err := dbconn.ExecContext(ctx, `PRAGMA foreign_keys = ON;`); err != nil {
		return fmt.Errorf("enable sqlite foreign keys: %w", err)
	}

	schemaSQL, err := loadSQLiteSchema()
	if err != nil {
		return err
	}

	if _, err := dbconn.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply sqlite schema: %w", err)
	}

	if err := runSQLitePhysicalFileMigration(dbconn, ctx); err != nil {
		return err
	}

	if err := runSQLiteSnapshotMigration(dbconn, ctx); err != nil {
		return err
	}

	return nil
}
