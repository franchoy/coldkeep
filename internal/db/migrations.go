package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	dbschema "github.com/franchoy/coldkeep/db"
)

func loadSQLiteSchema() (string, error) {
	if dbschema.SQLiteSchema == "" {
		return "", errors.New("embedded sqlite schema is empty")
	}
	return dbschema.SQLiteSchema, nil
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
