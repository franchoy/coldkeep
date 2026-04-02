package db

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestBackendFromDBSQLite(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	backend := BackendFromDB(dbconn)
	if backend != BackendSQLite {
		t.Fatalf("expected backend %q, got %q", BackendSQLite, backend)
	}
	if SupportsSelectForUpdate(dbconn) {
		t.Fatalf("expected sqlite to report no SELECT FOR UPDATE support")
	}
}

func TestBackendFromDBNil(t *testing.T) {
	if backend := BackendFromDB(nil); backend != BackendUnknown {
		t.Fatalf("expected nil DB backend %q, got %q", BackendUnknown, backend)
	}
	if SupportsSelectForUpdate(nil) {
		t.Fatalf("expected nil DB to report no SELECT FOR UPDATE support")
	}
}
