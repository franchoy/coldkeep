package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CurrentSchemaVersion returns the highest applied schema version in schema_version.
func CurrentSchemaVersion(dbconn *sql.DB) (int64, error) {
	if dbconn == nil {
		return 0, errors.New("nil DB connection")
	}

	ctx, cancel := NewOperationContext(context.Background())
	defer cancel()

	var version sql.NullInt64
	if err := dbconn.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("query schema_version: %w", err)
	}
	if !version.Valid {
		return 0, errors.New("schema_version table is empty")
	}

	return version.Int64, nil
}

// QueryCurrentSchemaVersion connects using runtime DB settings and returns MAX(schema_version.version).
func QueryCurrentSchemaVersion() (int64, error) {
	dbconn, err := ConnectDB()
	if err != nil {
		return 0, fmt.Errorf("connect DB for schema check: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	return CurrentSchemaVersion(dbconn)
}
