package storage

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func setupRepositoryConfigTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestGetDefaultChunkerVersionFallsBackToV1WhenUnset(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM repository_config WHERE key = $1`, repositoryDefaultChunkerKey); err != nil {
		t.Fatalf("delete default row: %v", err)
	}

	got, err := GetDefaultChunkerVersion(tx)
	if err != nil {
		t.Fatalf("GetDefaultChunkerVersion: %v", err)
	}
	if got != chunk.DefaultChunkerVersion {
		t.Fatalf("default chunker fallback mismatch: got %q want %q", got, chunk.DefaultChunkerVersion)
	}
}

func TestSetDefaultChunkerVersionRoundTrip(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if err := SetDefaultChunkerVersion(tx, chunk.VersionV2FastCDC); err != nil {
		t.Fatalf("SetDefaultChunkerVersion(v2): %v", err)
	}

	got, err := GetDefaultChunkerVersion(tx)
	if err != nil {
		t.Fatalf("GetDefaultChunkerVersion: %v", err)
	}
	if got != chunk.VersionV2FastCDC {
		t.Fatalf("round-trip default chunker mismatch: got %q want %q", got, chunk.VersionV2FastCDC)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	var persisted string
	if err := dbconn.QueryRow(`SELECT value FROM repository_config WHERE key = $1`, repositoryDefaultChunkerKey).Scan(&persisted); err != nil {
		t.Fatalf("read persisted default chunker: %v", err)
	}
	if persisted != string(chunk.VersionV2FastCDC) {
		t.Fatalf("persisted default chunker mismatch: got %q want %q", persisted, chunk.VersionV2FastCDC)
	}
}

func TestSetDefaultChunkerVersionRejectsUnregisteredVersion(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = SetDefaultChunkerVersion(tx, chunk.Version("v9-future-cdc"))
	if err == nil {
		t.Fatal("expected error for unregistered version, got nil")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unregistered-version error, got: %v", err)
	}
}

func TestGetDefaultChunkerVersionRejectsMalformedConfiguredValue(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`UPDATE repository_config SET value = $1 WHERE key = $2`, "future-v9", repositoryDefaultChunkerKey); err != nil {
		t.Fatalf("set malformed repository default chunker: %v", err)
	}

	_, err = GetDefaultChunkerVersion(tx)
	if err == nil {
		t.Fatal("expected error for malformed configured value, got nil")
	}
	if !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("expected malformed-value error, got: %v", err)
	}
}

func TestGetDefaultChunkerVersionRejectsUnregisteredConfiguredValue(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`UPDATE repository_config SET value = $1 WHERE key = $2`, "v9-future-cdc", repositoryDefaultChunkerKey); err != nil {
		t.Fatalf("set unregistered repository default chunker: %v", err)
	}

	_, err = GetDefaultChunkerVersion(tx)
	if err == nil {
		t.Fatal("expected error for unregistered configured value, got nil")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unregistered-value error, got: %v", err)
	}
}
