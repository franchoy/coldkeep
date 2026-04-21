package db

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
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

func TestBackendFromDBUnknownDriver(t *testing.T) {
	registerDummyDriver()
	dbconn, err := sql.Open("dummy", "")
	if err != nil {
		t.Fatalf("open dummy db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if got := BackendFromDB(dbconn); got != BackendUnknown {
		t.Fatalf("expected backend %q, got %q", BackendUnknown, got)
	}
	if SupportsSelectForUpdate(dbconn) {
		t.Fatalf("expected unknown backend to report no SELECT FOR UPDATE support")
	}
	if SupportsSelectForUpdateSkipLocked(dbconn) {
		t.Fatalf("expected unknown backend to report no SKIP LOCKED support")
	}
	if SupportsSelectForUpdateNowait(dbconn) {
		t.Fatalf("expected unknown backend to report no NOWAIT support")
	}
}

func TestQueryWithOptionalForUpdateHelpersUnknownDriverNoop(t *testing.T) {
	registerDummyDriver()
	dbconn, err := sql.Open("dummy", "")
	if err != nil {
		t.Fatalf("open dummy db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	const query = "SELECT id FROM container WHERE id = $1"
	if got := QueryWithOptionalForUpdate(dbconn, query); got != query {
		t.Fatalf("expected QueryWithOptionalForUpdate no-op for unknown backend, got %q", got)
	}
	if got := QueryWithOptionalForUpdateSkipLocked(dbconn, query); got != query {
		t.Fatalf("expected QueryWithOptionalForUpdateSkipLocked no-op for unknown backend, got %q", got)
	}
	if got := QueryWithOptionalForUpdateNowait(dbconn, query); got != query {
		t.Fatalf("expected QueryWithOptionalForUpdateNowait no-op for unknown backend, got %q", got)
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

func TestQueryWithOptionalForUpdatePostgresAppendsClause(t *testing.T) {
	dbconn, err := sql.Open("postgres", "host=127.0.0.1 port=1 user=invalid dbname=invalid sslmode=disable")
	if err != nil {
		t.Fatalf("open postgres driver handle: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	const query = "SELECT pf.path FROM physical_file pf JOIN logical_file lf ON lf.id = pf.logical_file_id ORDER BY pf.path"
	got := QueryWithOptionalForUpdate(dbconn, query)
	if got != query+" FOR UPDATE" {
		t.Fatalf("expected postgres FOR UPDATE query %q, got %q", query+" FOR UPDATE", got)
	}
}
