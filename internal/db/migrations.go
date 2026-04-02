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

const requiredPostgresSchemaVersion = 5

var postgresAutoBootstrapEnabled = loadPostgresAutoBootstrapEnabled()

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
	raw := strings.TrimSpace(strings.ToLower(strings.Trim(os.Getenv("COLDKEEP_DB_AUTO_BOOTSTRAP"), "\"'")))
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
		if !postgresAutoBootstrapEnabled {
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

	return nil
}
