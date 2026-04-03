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
	if SupportsSelectForUpdateSkipLocked(nil) {
		t.Fatalf("expected nil DB to report no SKIP LOCKED support")
	}
	if SupportsSelectForUpdateNowait(nil) {
		t.Fatalf("expected nil DB to report no NOWAIT support")
	}
}

func TestSupportsSelectForUpdateVariantsSQLite(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if SupportsSelectForUpdateSkipLocked(dbconn) {
		t.Fatalf("expected sqlite to report no SKIP LOCKED support")
	}
	if SupportsSelectForUpdateNowait(dbconn) {
		t.Fatalf("expected sqlite to report no NOWAIT support")
	}
}

func TestQueryWithOptionalForUpdateSQLite(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	const query = "SELECT status FROM logical_file WHERE id = $1"
	got := QueryWithOptionalForUpdate(dbconn, query)
	if got != query {
		t.Fatalf("expected sqlite query %q, got %q", query, got)
	}
}

func TestQueryWithOptionalForUpdateNil(t *testing.T) {
	const query = "SELECT status FROM logical_file WHERE id = $1"
	got := QueryWithOptionalForUpdate(nil, query)
	if got != query {
		t.Fatalf("expected nil-db query %q, got %q", query, got)
	}
}

func TestQueryWithOptionalForUpdateSkipLockedSQLite(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	const query = "SELECT id FROM container WHERE sealed = FALSE"
	got := QueryWithOptionalForUpdateSkipLocked(dbconn, query)
	if got != query {
		t.Fatalf("expected sqlite query %q, got %q", query, got)
	}
}

func TestQueryWithOptionalForUpdateNowaitSQLite(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	const query = "SELECT id FROM container WHERE id = $1"
	got := QueryWithOptionalForUpdateNowait(dbconn, query)
	if got != query {
		t.Fatalf("expected sqlite query %q, got %q", query, got)
	}
}

func TestQueryWithOptionalForUpdateSkipLockedNil(t *testing.T) {
	const query = "SELECT id FROM container WHERE sealed = FALSE"
	got := QueryWithOptionalForUpdateSkipLocked(nil, query)
	if got != query {
		t.Fatalf("expected nil-db query %q, got %q", query, got)
	}
}

func TestQueryWithOptionalForUpdateNowaitNil(t *testing.T) {
	const query = "SELECT id FROM container WHERE id = $1"
	got := QueryWithOptionalForUpdateNowait(nil, query)
	if got != query {
		t.Fatalf("expected nil-db query %q, got %q", query, got)
	}
}
