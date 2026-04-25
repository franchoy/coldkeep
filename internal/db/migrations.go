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

const requiredPostgresSchemaVersion = 11

type sqliteContextExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func sqliteTableHasColumn(dbconn sqliteContextExecutor, ctx context.Context, tableName, columnName string) (bool, error) {
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

func sqliteTableSQL(dbconn sqliteContextExecutor, ctx context.Context, tableName string) (string, error) {
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

func sqliteSchemaTableExists(dbconn sqliteContextExecutor, ctx context.Context, tableName string) (bool, error) {
	var count int
	err := dbconn.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		tableName,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func sqlitePhysicalFileNeedsRebuild(tableSQL string) bool {
	if tableSQL == "" {
		return false
	}
	normalized := strings.ToLower(tableSQL)
	return strings.Contains(normalized, "logical_file_id integer not null unique") ||
		!strings.Contains(normalized, "check (path != '')")
}

func rebuildSQLitePhysicalFileTable(dbconn sqliteContextExecutor, ctx context.Context) error {
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

func runSQLitePhysicalFileMigration(dbconn sqliteContextExecutor, ctx context.Context) error {
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

func runSQLiteSnapshotMigration(dbconn sqliteContextExecutor, ctx context.Context) error {
	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS snapshot (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMP NOT NULL,
			type TEXT NOT NULL CHECK (type IN ('full', 'partial')),
			label TEXT,
			parent_id TEXT REFERENCES snapshot(id) ON DELETE SET NULL
		)
	`); err != nil {
		return fmt.Errorf("create snapshot table: %w", err)
	}

	snapshotSQL, err := sqliteTableSQL(dbconn, ctx, "snapshot")
	if err != nil {
		return fmt.Errorf("read snapshot schema: %w", err)
	}
	normalizedSnapshotSQL := strings.ToLower(snapshotSQL)
	if normalizedSnapshotSQL != "" && !strings.Contains(normalizedSnapshotSQL, "parent_id") {
		if _, err := dbconn.ExecContext(ctx, `ALTER TABLE snapshot ADD COLUMN parent_id TEXT REFERENCES snapshot(id) ON DELETE SET NULL`); err != nil {
			return fmt.Errorf("add snapshot.parent_id: %w", err)
		}
	} else if normalizedSnapshotSQL != "" && strings.Contains(normalizedSnapshotSQL, "parent_id") && !strings.Contains(normalizedSnapshotSQL, "on delete set null") {
		return fmt.Errorf("snapshot.parent_id exists without ON DELETE SET NULL semantics")
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_created_at ON snapshot(created_at)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_created_at: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_parent_id ON snapshot(parent_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_parent_id: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS snapshot_path (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE CHECK (path != '')
		)
	`); err != nil {
		return fmt.Errorf("create snapshot_path table: %w", err)
	}

	hasSnapshotFileTable, err := sqliteSchemaTableExists(dbconn, ctx, "snapshot_file")
	if err != nil {
		return fmt.Errorf("check snapshot_file existence: %w", err)
	}
	if !hasSnapshotFileTable {
		if _, err := dbconn.ExecContext(ctx, `
			CREATE TABLE snapshot_file (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
				path_id INTEGER NOT NULL REFERENCES snapshot_path(id),
				logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
				size INTEGER,
				mode INTEGER,
				mtime TIMESTAMP
			)
		`); err != nil {
			return fmt.Errorf("create snapshot_file table: %w", err)
		}
	}

	hasPathID, err := sqliteTableHasColumn(dbconn, ctx, "snapshot_file", "path_id")
	if err != nil {
		return fmt.Errorf("inspect snapshot_file.path_id: %w", err)
	}
	hasPathText, err := sqliteTableHasColumn(dbconn, ctx, "snapshot_file", "path")
	if err != nil {
		return fmt.Errorf("inspect snapshot_file.path: %w", err)
	}

	if !hasPathID || hasPathText {
		if _, err := dbconn.ExecContext(ctx, `
			INSERT OR IGNORE INTO snapshot_path(path)
			SELECT DISTINCT path
			FROM snapshot_file
			WHERE path IS NOT NULL AND path != ''
		`); err != nil {
			return fmt.Errorf("backfill snapshot_path: %w", err)
		}

		if _, err := dbconn.ExecContext(ctx, `
			CREATE TABLE snapshot_file_v8 (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
				path_id INTEGER NOT NULL REFERENCES snapshot_path(id),
				logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
				size INTEGER,
				mode INTEGER,
				mtime TIMESTAMP
			)
		`); err != nil {
			return fmt.Errorf("create snapshot_file_v8 table: %w", err)
		}

		if hasPathText {
			if _, err := dbconn.ExecContext(ctx, `
				INSERT INTO snapshot_file_v8 (id, snapshot_id, path_id, logical_file_id, size, mode, mtime)
				SELECT
					sf.id,
					sf.snapshot_id,
					sp.id,
					sf.logical_file_id,
					sf.size,
					sf.mode,
					sf.mtime
				FROM snapshot_file sf
				JOIN snapshot_path sp ON sp.path = sf.path
			`); err != nil {
				return fmt.Errorf("copy snapshot_file rows into snapshot_file_v8: %w", err)
			}
		} else {
			if _, err := dbconn.ExecContext(ctx, `
				INSERT INTO snapshot_file_v8 (id, snapshot_id, path_id, logical_file_id, size, mode, mtime)
				SELECT id, snapshot_id, path_id, logical_file_id, size, mode, mtime
				FROM snapshot_file
			`); err != nil {
				return fmt.Errorf("copy snapshot_file rows from partially migrated table: %w", err)
			}
		}

		if _, err := dbconn.ExecContext(ctx, `DROP TABLE snapshot_file`); err != nil {
			return fmt.Errorf("drop legacy snapshot_file table: %w", err)
		}

		if _, err := dbconn.ExecContext(ctx, `ALTER TABLE snapshot_file_v8 RENAME TO snapshot_file`); err != nil {
			return fmt.Errorf("rename snapshot_file_v8 table: %w", err)
		}
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_file_snapshot_id ON snapshot_file(snapshot_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_snapshot_id: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_file_path_id ON snapshot_file(path_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_path_id: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_snapshot_file_logical_file ON snapshot_file(logical_file_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_logical_file: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_file_unique ON snapshot_file(snapshot_id, path_id)
	`); err != nil {
		return fmt.Errorf("create idx_snapshot_file_unique: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		DELETE FROM schema_version WHERE version < 8
	`); err != nil {
		return fmt.Errorf("clean sqlite schema_version before 8: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_version(version) VALUES (8)
	`); err != nil {
		return fmt.Errorf("insert sqlite schema_version 8: %w", err)
	}

	return nil
}

func runSQLiteChunkerVersionMigration(dbconn sqliteContextExecutor, ctx context.Context) error {
	hasChunkerVersion, err := sqliteTableHasColumn(dbconn, ctx, "logical_file", "chunker_version")
	if err != nil {
		return fmt.Errorf("inspect logical_file.chunker_version: %w", err)
	}
	if !hasChunkerVersion {
		if _, err := dbconn.ExecContext(ctx, `ALTER TABLE logical_file ADD COLUMN chunker_version TEXT NOT NULL DEFAULT 'v1-simple-rolling'`); err != nil {
			return fmt.Errorf("add logical_file.chunker_version: %w", err)
		}
	}

	if _, err := dbconn.ExecContext(ctx, `
		UPDATE logical_file
		SET chunker_version = 'v1-simple-rolling'
		WHERE chunker_version IS NULL
	`); err != nil {
		return fmt.Errorf("backfill logical_file.chunker_version: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		DELETE FROM schema_version WHERE version < 9
	`); err != nil {
		return fmt.Errorf("clean sqlite schema_version before 9: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_version(version) VALUES (9)
	`); err != nil {
		return fmt.Errorf("insert sqlite schema_version 9: %w", err)
	}

	return nil
}

func sqliteHasTable(dbconn sqliteContextExecutor, ctx context.Context, tableName string) (bool, error) {
	rows, err := dbconn.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		return true, rows.Err()
	}
	return false, rows.Err()
}

func runSQLiteChunkChunkerVersionMigration(dbconn sqliteContextExecutor, ctx context.Context) error {
	tableExists, err := sqliteHasTable(dbconn, ctx, "chunk")
	if err != nil {
		return fmt.Errorf("inspect chunk table existence: %w", err)
	}
	if !tableExists {
		// chunk table absent in legacy schemas; still advance schema version.
		if _, err := dbconn.ExecContext(ctx, `
			DELETE FROM schema_version WHERE version < 10
		`); err != nil {
			return fmt.Errorf("clean sqlite schema_version before 10: %w", err)
		}

		if _, err := dbconn.ExecContext(ctx, `
			INSERT OR IGNORE INTO schema_version(version) VALUES (10)
		`); err != nil {
			return fmt.Errorf("insert sqlite schema_version 10: %w", err)
		}

		return nil
	}

	hasChunkerVersion, err := sqliteTableHasColumn(dbconn, ctx, "chunk", "chunker_version")
	if err != nil {
		return fmt.Errorf("inspect chunk.chunker_version: %w", err)
	}
	if !hasChunkerVersion {
		if _, err := dbconn.ExecContext(ctx, `ALTER TABLE chunk ADD COLUMN chunker_version TEXT NOT NULL DEFAULT 'v1-simple-rolling'`); err != nil {
			return fmt.Errorf("add chunk.chunker_version: %w", err)
		}
	}

	if _, err := dbconn.ExecContext(ctx, `
		UPDATE chunk
		SET chunker_version = 'v1-simple-rolling'
		WHERE chunker_version IS NULL
	`); err != nil {
		return fmt.Errorf("backfill chunk.chunker_version: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		DELETE FROM schema_version WHERE version < 10
	`); err != nil {
		return fmt.Errorf("clean sqlite schema_version before 10: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_version(version) VALUES (10)
	`); err != nil {
		return fmt.Errorf("insert sqlite schema_version 10: %w", err)
	}

	return nil
}

func runSQLiteRepositoryConfigMigration(dbconn sqliteContextExecutor, ctx context.Context) error {
	if _, err := dbconn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS repository_config (
			key TEXT PRIMARY KEY CHECK (key != ''),
			value TEXT NOT NULL CHECK (value != '')
		)
	`); err != nil {
		return fmt.Errorf("create repository_config table: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO repository_config(key, value)
		VALUES ('default_chunker', 'v1-simple-rolling')
	`); err != nil {
		return fmt.Errorf("seed repository_config.default_chunker: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		DELETE FROM schema_version WHERE version < 11
	`); err != nil {
		return fmt.Errorf("clean sqlite schema_version before 11: %w", err)
	}

	if _, err := dbconn.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_version(version) VALUES (11)
	`); err != nil {
		return fmt.Errorf("insert sqlite schema_version 11: %w", err)
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

func isSQLiteSchemaApplyCompatibilityError(err error) bool {
	if err == nil {
		return false
	}
	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "no such column: parent_id") ||
		strings.Contains(errText, "no such column: path_id")
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
		if !isSQLiteSchemaApplyCompatibilityError(err) {
			return fmt.Errorf("apply sqlite schema: %w", err)
		}
		_, _ = dbconn.ExecContext(ctx, `ROLLBACK`)
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := runSQLitePhysicalFileMigration(tx, ctx); err != nil {
		return err
	}

	if err := runSQLiteSnapshotMigration(tx, ctx); err != nil {
		return err
	}

	if err := runSQLiteChunkerVersionMigration(tx, ctx); err != nil {
		return err
	}

	if err := runSQLiteChunkChunkerVersionMigration(tx, ctx); err != nil {
		return err
	}

	if err := runSQLiteRepositoryConfigMigration(tx, ctx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite migration transaction: %w", err)
	}

	return nil
}
