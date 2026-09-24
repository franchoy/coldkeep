//go:build schema_v17_historical_probe

package db

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSchemaV17HistoricalSQLiteProbe(t *testing.T) {
	databasePath := os.Getenv("COLDKEEP_SCHEMA_V17_PROBE_DB")
	if databasePath == "" {
		t.Fatal("COLDKEEP_SCHEMA_V17_PROBE_DB is required")
	}
	action := os.Getenv("COLDKEEP_SCHEMA_V17_PROBE_ACTION")

	dbconn, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		t.Fatalf("open probe database: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	defer func() { _ = dbconn.Close() }()

	switch action {
	case "prepare-current":
		if err := RunMigrations(dbconn); err != nil {
			t.Fatalf("prepare current schema: %v", err)
		}
		var rows, version int
		if err := dbconn.QueryRow(`SELECT COUNT(*), MIN(catalog_version) FROM schema_version`).Scan(&rows, &version); err != nil {
			t.Fatalf("read current fenced metadata: %v", err)
		}
		if rows != 1 || version != 17 {
			t.Fatalf("current fenced metadata = rows:%d version:%d", rows, version)
		}
	case "historical-valid":
		if err := RunMigrations(dbconn); err != nil {
			t.Fatalf("historical runtime rejected its valid schema: %v", err)
		}
		var version int
		if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
			t.Fatalf("read historical schema metadata: %v", err)
		}
		if version != requiredPostgresSchemaVersion {
			t.Fatalf("historical schema version = %d, want %d", version, requiredPostgresSchemaVersion)
		}
	case "historical-reject":
		err := RunMigrations(dbconn)
		if err == nil {
			t.Fatal("historical runtime accepted fenced schema 17")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "version") {
			t.Fatalf("historical rejection = %q, want old-column failure", err)
		}
		var rows, version int
		if err := dbconn.QueryRow(`SELECT COUNT(*), MIN(catalog_version) FROM schema_version`).Scan(&rows, &version); err != nil {
			t.Fatalf("read fenced metadata after historical rejection: %v", err)
		}
		if rows != 1 || version != 17 {
			t.Fatalf("fenced metadata changed = rows:%d version:%d", rows, version)
		}
	default:
		t.Fatalf("unsupported COLDKEEP_SCHEMA_V17_PROBE_ACTION %q", action)
	}
}
