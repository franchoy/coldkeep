package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/utils_env"
)

func loadSQLiteSchema() (string, error) {
	schemaPath := strings.TrimSpace(utils_env.GetenvOrDefault("COLDKEEP_SQLITE_SCHEMA_PATH", "db/schema_sqlite.sql"))
	if schemaPath != "" {
		b, err := os.ReadFile(schemaPath)
		if err == nil {
			return string(b), nil
		}

		// Keep explicit env override strict: if user configured a path, fail fast.
		if envPath := strings.TrimSpace(os.Getenv("COLDKEEP_SQLITE_SCHEMA_PATH")); envPath != "" {
			return "", fmt.Errorf("read sqlite schema from COLDKEEP_SQLITE_SCHEMA_PATH=%q: %w", envPath, err)
		}
	}

	// Resolve from cwd by walking upwards to find repo root db/schema_sqlite.sql.
	if cwd, err := os.Getwd(); err == nil {
		dir := cwd
		for i := 0; i < 8; i++ {
			candidate := filepath.Join(dir, "db", "schema_sqlite.sql")
			if b, readErr := os.ReadFile(candidate); readErr == nil {
				return string(b), nil
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	fallbacks := []string{
		"db/schema_sqlite.sql",
		"../db/schema_sqlite.sql",
		"../../db/schema_sqlite.sql",
		"/repo/db/schema_sqlite.sql",
		"/work/db/schema_sqlite.sql",
		"/db/schema_sqlite.sql",
		"/app/db/schema_sqlite.sql",
	}
	for _, p := range fallbacks {
		if b, err := os.ReadFile(p); err == nil {
			return string(b), nil
		}
	}

	return "", errors.New("could not find db/schema_sqlite.sql; set COLDKEEP_SQLITE_SCHEMA_PATH")
}

// RunMigrations applies the schema to a DB connection.
// Currently this is used for simulated sqlite storage contexts.
func RunMigrations(dbconn *sql.DB) error {
	if dbconn == nil {
		return errors.New("nil DB connection")
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
