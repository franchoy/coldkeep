package db

import (
	"database/sql"
	"database/sql/driver"
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
