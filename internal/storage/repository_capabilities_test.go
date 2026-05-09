package storage

import (
	"database/sql"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/repository/capabilities"
	_ "github.com/mattn/go-sqlite3"
)

func TestGetRepositoryCapabilitiesNilRepositoryFallsBackToDefaults(t *testing.T) {
	caps := GetRepositoryCapabilities(nil)

	if caps.DefaultCompression != capabilities.CompressionNone {
		t.Fatalf("expected default compression none, got %q", caps.DefaultCompression)
	}
	if caps.DefaultPacking != capabilities.PackingPackedMulti {
		t.Fatalf("expected default packing packed-multi, got %q", caps.DefaultPacking)
	}
	if !caps.ReadPathMetadataDriven {
		t.Fatalf("expected metadata-driven read path")
	}
}

func TestGetRepositoryCapabilitiesFromRepositoryDB(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	caps := GetRepositoryCapabilities(NewRepository(dbconn))
	if !caps.SupportsCompression("zstd") {
		t.Fatalf("expected zstd support from repository capabilities")
	}
	if !caps.SupportsEncryption("aes-gcm") {
		t.Fatalf("expected aes-gcm support from repository capabilities")
	}
	if !caps.SupportsPacking("packed-multi") {
		t.Fatalf("expected packed-multi support from repository capabilities")
	}
}

func TestGetRepositoryCapabilitiesWithErrorFromRepositoryDB(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	caps, err := GetRepositoryCapabilitiesWithError(NewRepository(dbconn))
	if err != nil {
		t.Fatalf("GetRepositoryCapabilitiesWithError: %v", err)
	}
	if caps.RepositoryFormatVersion == 0 {
		t.Fatalf("expected non-zero repository format version")
	}
}
