package storage

import (
	"database/sql"
	"fmt"
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

// Compression config tests

func TestIsRegisteredCompressionCodec(t *testing.T) {
	tests := []struct {
		codec    string
		expected bool
	}{
		{"none", true},
		{"zstd", true},
		{"NONE", false}, // case-sensitive
		{"gzip", false},
		{"aes-gcm", false},
		{"xz", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.codec, func(t *testing.T) {
			got := IsRegisteredCompressionCodec(tt.codec)
			if got != tt.expected {
				t.Fatalf("IsRegisteredCompressionCodec(%q) = %v, want %v", tt.codec, got, tt.expected)
			}
		})
	}
}

func TestGetDefaultCompressionFallsBackToNoneWhenUnset(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM repository_config WHERE key = $1`, repositoryDefaultCompressionKey); err != nil {
		t.Fatalf("delete default row: %v", err)
	}

	got, err := GetDefaultCompression(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompression: %v", err)
	}
	if got != defaultCompressionCodec {
		t.Fatalf("default compression fallback mismatch: got %q want %q", got, defaultCompressionCodec)
	}
}

func TestSetDefaultCompressionRoundTrip(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if err := SetDefaultCompression(tx, "zstd"); err != nil {
		t.Fatalf("SetDefaultCompression(zstd): %v", err)
	}

	got, err := GetDefaultCompression(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompression: %v", err)
	}
	if got != "zstd" {
		t.Fatalf("round-trip default compression mismatch: got %q want %q", got, "zstd")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	var persisted string
	if err := dbconn.QueryRow(`SELECT value FROM repository_config WHERE key = $1`, repositoryDefaultCompressionKey).Scan(&persisted); err != nil {
		t.Fatalf("read persisted default compression: %v", err)
	}
	if persisted != "zstd" {
		t.Fatalf("persisted default compression mismatch: got %q want %q", persisted, "zstd")
	}
}

func TestSetDefaultCompressionRejectsUnregisteredCodec(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = SetDefaultCompression(tx, "xz")
	if err == nil {
		t.Fatal("expected error for unregistered codec, got nil")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unregistered-codec error, got: %v", err)
	}
}

func TestGetDefaultCompressionLevelFallsBackTo3WhenUnset(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM repository_config WHERE key = $1`, repositoryDefaultCompressionLevelKey); err != nil {
		t.Fatalf("delete default row: %v", err)
	}

	got, err := GetDefaultCompressionLevel(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompressionLevel: %v", err)
	}
	if got != defaultCompressionLevel {
		t.Fatalf("default compression level fallback mismatch: got %d want %d", got, defaultCompressionLevel)
	}
}

func TestSetDefaultCompressionLevelRoundTrip(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if err := SetDefaultCompressionLevel(tx, 5); err != nil {
		t.Fatalf("SetDefaultCompressionLevel(5): %v", err)
	}

	got, err := GetDefaultCompressionLevel(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompressionLevel: %v", err)
	}
	if got != 5 {
		t.Fatalf("round-trip default compression level mismatch: got %d want %d", got, 5)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	var persisted string
	if err := dbconn.QueryRow(`SELECT value FROM repository_config WHERE key = $1`, repositoryDefaultCompressionLevelKey).Scan(&persisted); err != nil {
		t.Fatalf("read persisted default compression level: %v", err)
	}
	if persisted != "5" {
		t.Fatalf("persisted default compression level mismatch: got %q want %q", persisted, "5")
	}
}

func TestSetDefaultCompressionLevelRejectsOutOfRangeValues(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tests := []struct {
		name  string
		level int
	}{
		{"negative", -1},
		{"too low", -10},
		{"too high", 23},
		{"way too high", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := dbconn.Begin()
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			err = SetDefaultCompressionLevel(tx, tt.level)
			if err == nil {
				t.Fatalf("expected error for level %d, got nil", tt.level)
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Fatalf("expected out-of-range error for level %d, got: %v", tt.level, err)
			}
		})
	}
}

func TestCompressionConfigBoundaryValues(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tests := []struct {
		value   int
		success bool
	}{
		{0, true},   // minimum valid
		{1, true},   // lower range
		{5, true},   // mid range
		{22, true},  // maximum valid
		{-1, false}, // below minimum
		{23, false}, // above maximum
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("level_%d", tt.value), func(t *testing.T) {
			tx, err := dbconn.Begin()
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}

			err = SetDefaultCompressionLevel(tx, tt.value)
			if (err == nil) != tt.success {
				t.Fatalf("SetDefaultCompressionLevel(%d) success=%v, want %v (err=%v)", tt.value, err == nil, tt.success, err)
			}

			_ = tx.Rollback()
		})
	}
}
