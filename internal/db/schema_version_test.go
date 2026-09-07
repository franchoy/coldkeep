package db

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestCurrentSchemaVersionReturnsMaxVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE schema_version (version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create schema_version table: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO schema_version(version) VALUES (3), (8), (11)`); err != nil {
		t.Fatalf("insert schema versions: %v", err)
	}

	version, err := CurrentSchemaVersion(dbconn)
	if err != nil {
		t.Fatalf("CurrentSchemaVersion: %v", err)
	}
	if version != 11 {
		t.Fatalf("expected version 11, got %d", version)
	}
}

func TestCurrentSchemaVersionEmptyTable(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE schema_version (version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create schema_version table: %v", err)
	}

	_, err = CurrentSchemaVersion(dbconn)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "schema_version table is empty") {
		t.Fatalf("expected empty schema_version error, got %q", got)
	}
}

func TestCurrentSchemaVersionReadsFencedSingleton(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE schema_version (catalog_version INTEGER PRIMARY KEY); INSERT INTO schema_version(catalog_version) VALUES (17)`); err != nil {
		t.Fatalf("create fenced schema_version table: %v", err)
	}
	version, err := CurrentSchemaVersion(dbconn)
	if err != nil {
		t.Fatalf("CurrentSchemaVersion: %v", err)
	}
	if version != 17 {
		t.Fatalf("fenced version = %d, want 17", version)
	}
}

func TestCurrentSchemaVersionRequiresSchemaVersionTable(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	_, err = CurrentSchemaVersion(dbconn)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "query schema_version") {
		t.Fatalf("expected query schema_version prefix, got %v", err)
	}
}
