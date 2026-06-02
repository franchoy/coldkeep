package engine

import (
	"context"
	"database/sql"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func newEngineTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	return dbconn
}

// TestDefaultEngineVerifyUsesConfiguredDB proves DefaultEngine.Verify honors the
// engine-provided Config.DB (regression for CK-112-R001).
//
// db.ConnectDB() connects to PostgreSQL using global/environment configuration.
// If Verify reopened the global DB, this test would attempt a PostgreSQL
// connection and fail in this environment. Success against the injected
// in-memory SQLite DB proves the configured DB is used.
func TestDefaultEngineVerifyUsesConfiguredDB(t *testing.T) {
	dbconn := newEngineTestDB(t)

	eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := eng.Verify(context.Background(), VerifyRequest{Target: "system"}); err != nil {
		t.Fatalf("Verify(system) on consistent empty repo: unexpected error: %v", err)
	}
}

// TestDefaultEngineVerifyFileUsesConfiguredDB proves the file path also runs its
// existence check against the injected Config.DB rather than a reopened global
// connection. The non-existent file ID must produce an error from the injected
// DB, not a connection failure.
func TestDefaultEngineVerifyFileUsesConfiguredDB(t *testing.T) {
	dbconn := newEngineTestDB(t)

	eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := eng.Verify(context.Background(), VerifyRequest{Target: "file", FileID: 99999}); err == nil {
		t.Fatal("Verify(file) for non-existent ID: want error, got nil")
	}
}
