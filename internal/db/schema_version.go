package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const currentCatalogSchemaVersion = 17

type schemaMetadataRepresentation uint8

const (
	schemaMetadataMissing schemaMetadataRepresentation = iota
	schemaMetadataLegacy
	schemaMetadataFenced
)

type schemaMetadataState struct {
	representation schemaMetadataRepresentation
	version        int
}

type schemaMetadataQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func inspectSchemaMetadata(ctx context.Context, dbconn schemaMetadataQueryer, backend Backend) (schemaMetadataState, error) {
	switch backend {
	case BackendSQLite:
		return inspectSQLiteSchemaMetadata(ctx, dbconn)
	case BackendPostgres:
		return inspectPostgresSchemaMetadata(ctx, dbconn)
	default:
		return schemaMetadataState{}, fmt.Errorf("inspect schema_version: unsupported backend %s", backend)
	}
}

func inspectSQLiteSchemaMetadata(ctx context.Context, dbconn schemaMetadataQueryer) (schemaMetadataState, error) {
	var tableCount int
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'`).Scan(&tableCount); err != nil {
		return schemaMetadataState{}, fmt.Errorf("inspect sqlite schema_version table: %w", err)
	}
	if tableCount == 0 {
		return schemaMetadataState{representation: schemaMetadataMissing}, nil
	}

	rows, err := dbconn.QueryContext(ctx, `PRAGMA table_info(schema_version)`)
	if err != nil {
		return schemaMetadataState{}, fmt.Errorf("inspect sqlite schema_version columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columnName, columnType string
	var primaryKey int
	columnCount := 0
	for rows.Next() {
		var cid, notNull, primaryK int
		var name, dataType string
		var defaultV sql.NullString
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultV, &primaryK); err != nil {
			return schemaMetadataState{}, fmt.Errorf("scan sqlite schema_version column: %w", err)
		}
		columnCount++
		columnName, columnType, primaryKey = name, dataType, primaryK
	}
	if err := rows.Err(); err != nil {
		return schemaMetadataState{}, fmt.Errorf("iterate sqlite schema_version columns: %w", err)
	}
	if columnCount != 1 || (columnName != "version" && columnName != "catalog_version") {
		return schemaMetadataState{}, errors.New("malformed sqlite schema_version columns: require exactly one of version or catalog_version")
	}
	if !strings.EqualFold(strings.TrimSpace(columnType), "INTEGER") || primaryKey != 1 {
		return schemaMetadataState{}, fmt.Errorf("malformed sqlite schema_version.%s: require INTEGER PRIMARY KEY", columnName)
	}

	query := fmt.Sprintf(`SELECT COUNT(*), MIN(%s), MAX(%s), COALESCE(SUM(CASE WHEN typeof(%s) = 'integer' THEN 1 ELSE 0 END), 0) FROM schema_version`, columnName, columnName, columnName)
	var rowCount, integerRows int
	var minimum, maximum sql.NullInt64
	if err := dbconn.QueryRowContext(ctx, query).Scan(&rowCount, &minimum, &maximum, &integerRows); err != nil {
		return schemaMetadataState{}, fmt.Errorf("inspect sqlite schema_version rows: %w", err)
	}
	if rowCount == 0 {
		return schemaMetadataState{}, errors.New("schema_version table is empty")
	}
	if integerRows != rowCount || !minimum.Valid || !maximum.Valid {
		return schemaMetadataState{}, fmt.Errorf("malformed sqlite schema_version.%s value", columnName)
	}

	if columnName == "catalog_version" {
		if rowCount != 1 {
			return schemaMetadataState{}, fmt.Errorf("malformed fenced sqlite schema_version: have %d rows, require exactly one", rowCount)
		}
		if maximum.Int64 < currentCatalogSchemaVersion {
			return schemaMetadataState{}, fmt.Errorf("fenced sqlite catalog_version %d is below required version %d", maximum.Int64, currentCatalogSchemaVersion)
		}
		if maximum.Int64 > currentCatalogSchemaVersion {
			return schemaMetadataState{}, fmt.Errorf("fenced sqlite catalog_version %d is a future version", maximum.Int64)
		}
		return schemaMetadataState{representation: schemaMetadataFenced, version: int(maximum.Int64)}, nil
	}

	if minimum.Int64 < 1 {
		return schemaMetadataState{}, fmt.Errorf("malformed sqlite legacy schema_version: minimum version %d is below 1", minimum.Int64)
	}
	if maximum.Int64 > currentCatalogSchemaVersion {
		return schemaMetadataState{}, fmt.Errorf("sqlite legacy schema version %d is a future version", maximum.Int64)
	}
	return schemaMetadataState{representation: schemaMetadataLegacy, version: int(maximum.Int64)}, nil
}

func inspectPostgresSchemaMetadata(ctx context.Context, dbconn schemaMetadataQueryer) (schemaMetadataState, error) {
	var tableName sql.NullString
	if err := dbconn.QueryRowContext(ctx, `SELECT to_regclass('public.schema_version')`).Scan(&tableName); err != nil {
		return schemaMetadataState{}, fmt.Errorf("inspect postgres schema_version table: %w", err)
	}
	if !tableName.Valid {
		return schemaMetadataState{representation: schemaMetadataMissing}, nil
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT c.column_name, c.data_type,
		       EXISTS (
		         SELECT 1
		         FROM information_schema.table_constraints tc
		         JOIN information_schema.key_column_usage kcu
		           ON kcu.constraint_catalog = tc.constraint_catalog
		          AND kcu.constraint_schema = tc.constraint_schema
		          AND kcu.constraint_name = tc.constraint_name
		         WHERE tc.table_schema = c.table_schema
		           AND tc.table_name = c.table_name
		           AND tc.constraint_type = 'PRIMARY KEY'
		           AND kcu.column_name = c.column_name
		       )
		FROM information_schema.columns c
		WHERE c.table_schema = 'public' AND c.table_name = 'schema_version'
		ORDER BY c.ordinal_position
	`)
	if err != nil {
		return schemaMetadataState{}, fmt.Errorf("inspect postgres schema_version columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columnName, columnType string
	var primaryKey bool
	columnCount := 0
	for rows.Next() {
		if err := rows.Scan(&columnName, &columnType, &primaryKey); err != nil {
			return schemaMetadataState{}, fmt.Errorf("scan postgres schema_version column: %w", err)
		}
		columnCount++
	}
	if err := rows.Err(); err != nil {
		return schemaMetadataState{}, fmt.Errorf("iterate postgres schema_version columns: %w", err)
	}
	if columnCount != 1 || (columnName != "version" && columnName != "catalog_version") {
		return schemaMetadataState{}, errors.New("malformed postgres schema_version columns: require exactly one of version or catalog_version")
	}
	if columnType != "integer" || !primaryKey {
		return schemaMetadataState{}, fmt.Errorf("malformed postgres schema_version.%s: require INTEGER PRIMARY KEY", columnName)
	}

	query := fmt.Sprintf(`SELECT COUNT(*), MIN(%s), MAX(%s) FROM schema_version`, columnName, columnName)
	var rowCount int
	var minimum, maximum sql.NullInt64
	if err := dbconn.QueryRowContext(ctx, query).Scan(&rowCount, &minimum, &maximum); err != nil {
		return schemaMetadataState{}, fmt.Errorf("inspect postgres schema_version rows: %w", err)
	}
	if rowCount == 0 {
		return schemaMetadataState{}, errors.New("schema_version table is empty")
	}
	if rowCount != 1 || !minimum.Valid || !maximum.Valid {
		return schemaMetadataState{}, fmt.Errorf("malformed postgres schema_version: have %d rows, require exactly one", rowCount)
	}

	if columnName == "catalog_version" {
		if maximum.Int64 < currentCatalogSchemaVersion {
			return schemaMetadataState{}, fmt.Errorf("fenced postgres catalog_version %d is below required version %d", maximum.Int64, currentCatalogSchemaVersion)
		}
		if maximum.Int64 > currentCatalogSchemaVersion {
			return schemaMetadataState{}, fmt.Errorf("fenced postgres catalog_version %d is a future version", maximum.Int64)
		}
		return schemaMetadataState{representation: schemaMetadataFenced, version: int(maximum.Int64)}, nil
	}

	if minimum.Int64 < 1 {
		return schemaMetadataState{}, fmt.Errorf("malformed postgres legacy schema_version: version %d is below 1", minimum.Int64)
	}
	if maximum.Int64 > currentCatalogSchemaVersion {
		return schemaMetadataState{}, fmt.Errorf("postgres legacy schema version %d is a future version", maximum.Int64)
	}
	return schemaMetadataState{representation: schemaMetadataLegacy, version: int(maximum.Int64)}, nil
}

// CurrentSchemaVersion returns the authoritative version from either a valid
// legacy representation or the fenced schema-17 singleton.
func CurrentSchemaVersion(dbconn *sql.DB) (int64, error) {
	if dbconn == nil {
		return 0, errors.New("nil DB connection")
	}
	ctx, cancel := NewOperationContext(context.Background())
	defer cancel()
	return CurrentSchemaVersionContext(ctx, dbconn)
}

// CurrentSchemaVersionContext returns the authoritative schema version using
// the caller-owned context.
func CurrentSchemaVersionContext(ctx context.Context, dbconn *sql.DB) (int64, error) {
	if dbconn == nil {
		return 0, errors.New("nil DB connection")
	}
	backend := BackendFromDB(dbconn)
	if backend == BackendUnknown {
		var version int64
		if err := dbconn.QueryRowContext(ctx, `SELECT catalog_version FROM schema_version`).Scan(&version); err != nil {
			return 0, fmt.Errorf("query schema_version: %w", err)
		}
		return version, nil
	}
	state, err := inspectSchemaMetadata(ctx, dbconn, backend)
	if err != nil {
		return 0, fmt.Errorf("query schema_version: %w", err)
	}
	if state.representation == schemaMetadataMissing {
		return 0, errors.New("query schema_version: schema_version table is missing")
	}
	return int64(state.version), nil
}

// QueryCurrentSchemaVersion connects using runtime DB settings and returns the
// authoritative schema version.
func QueryCurrentSchemaVersion() (int64, error) {
	dbconn, err := ConnectDB()
	if err != nil {
		return 0, fmt.Errorf("connect DB for schema check: %w", err)
	}
	defer func() { _ = dbconn.Close() }()
	return CurrentSchemaVersion(dbconn)
}
