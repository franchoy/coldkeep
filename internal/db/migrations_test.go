package db

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

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
