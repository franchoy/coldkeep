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
		{"below minimum (zero)", 0},
		{"negative", -1},
		{"too low", -10},
		{"too high (10)", 10},
		{"too high (22)", 22},
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
		{1, true},   // minimum valid (Phase 5.1)
		{3, true},   // default
		{5, true},   // mid range
		{9, true},   // maximum valid (Phase 5.1)
		{0, false},  // below minimum
		{10, false}, // above maximum
		{-1, false}, // negative
		{22, false}, // zstd max (not allowed in Phase 5.1)
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

// Phase 5.1 Compression Configuration Validation Tests

func TestValidateRepositoryCompressionConfigWithCompressionNone(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Set compression to "none" (default)
	if err := SetDefaultCompression(tx, "none"); err != nil {
		t.Fatalf("SetDefaultCompression(none): %v", err)
	}

	// Validation should pass with compression="none" regardless of level config
	if err := ValidateRepositoryCompressionConfig(tx); err != nil {
		t.Fatalf("ValidateRepositoryCompressionConfig with compression=none: %v", err)
	}
}

func TestValidateRepositoryCompressionConfigWithZstdAndValidLevel(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Set compression to "zstd" with valid level
	if err := SetDefaultCompression(tx, "zstd"); err != nil {
		t.Fatalf("SetDefaultCompression(zstd): %v", err)
	}
	if err := SetDefaultCompressionLevel(tx, 5); err != nil {
		t.Fatalf("SetDefaultCompressionLevel(5): %v", err)
	}

	// Validation should pass
	if err := ValidateRepositoryCompressionConfig(tx); err != nil {
		t.Fatalf("ValidateRepositoryCompressionConfig with zstd+level5: %v", err)
	}
}

func TestValidateRepositoryCompressionConfigWithZstdAndDefaultLevel(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Set compression to "zstd" without explicitly setting level (should default to 3)
	if err := SetDefaultCompression(tx, "zstd"); err != nil {
		t.Fatalf("SetDefaultCompression(zstd): %v", err)
	}

	// Validate without setting level explicitly - should use default (3)
	if err := ValidateRepositoryCompressionConfig(tx); err != nil {
		t.Fatalf("ValidateRepositoryCompressionConfig with zstd+default_level: %v", err)
	}

	// Confirm default level is 3
	level, err := GetDefaultCompressionLevel(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompressionLevel: %v", err)
	}
	if level != 3 {
		t.Fatalf("expected default level 3, got %d", level)
	}
}

func TestValidateRepositoryCompressionConfigWithUnsetDefaults(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Delete all compression config rows to test defaults
	if _, err := tx.Exec(`DELETE FROM repository_config WHERE key IN ($1, $2)`,
		repositoryDefaultCompressionKey, repositoryDefaultCompressionLevelKey); err != nil {
		t.Fatalf("delete config rows: %v", err)
	}

	// Validation should pass with defaults (compression=none, level=3)
	if err := ValidateRepositoryCompressionConfig(tx); err != nil {
		t.Fatalf("ValidateRepositoryCompressionConfig with unset defaults: %v", err)
	}

	// Confirm defaults
	codec, err := GetDefaultCompression(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompression: %v", err)
	}
	if codec != "none" {
		t.Fatalf("expected default codec 'none', got %q", codec)
	}

	level, err := GetDefaultCompressionLevel(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompressionLevel: %v", err)
	}
	if level != 3 {
		t.Fatalf("expected default level 3, got %d", level)
	}
}

func TestCompressionConfigPhase51RequiredValidRangeForZstd(t *testing.T) {
	// Phase 5.1 contract: compression levels must be in [1, 9] for zstd
	// Test that all valid levels pass validation
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	validLevels := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, level := range validLevels {
		t.Run(fmt.Sprintf("zstd_level_%d", level), func(t *testing.T) {
			tx, err := dbconn.Begin()
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			if err := SetDefaultCompression(tx, "zstd"); err != nil {
				t.Fatalf("SetDefaultCompression(zstd): %v", err)
			}
			if err := SetDefaultCompressionLevel(tx, level); err != nil {
				t.Fatalf("SetDefaultCompressionLevel(%d): %v", level, err)
			}
			if err := ValidateRepositoryCompressionConfig(tx); err != nil {
				t.Fatalf("validation failed for level %d: %v", level, err)
			}
		})
	}
}

func TestCompressionConfigPhase51RejectsOutOfBoundsLevels(t *testing.T) {
	// Phase 5.1 contract: levels 0, 10+ must be rejected
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	invalidLevels := []int{0, 10, 11, 22, 50}
	for _, level := range invalidLevels {
		t.Run(fmt.Sprintf("invalid_level_%d", level), func(t *testing.T) {
			tx, err := dbconn.Begin()
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			// Should fail at SetDefaultCompressionLevel
			err = SetDefaultCompressionLevel(tx, level)
			if err == nil {
				t.Fatalf("expected error for level %d, got nil", level)
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Fatalf("expected out-of-range error for level %d, got: %v", level, err)
			}
		})
	}
}

func TestCompressionConfigInvalidCodecIsRejected(t *testing.T) {
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	invalidCodecs := []string{"gzip", "aes-gcm", "xz", "lz4", "unknown"}
	for _, codec := range invalidCodecs {
		t.Run(fmt.Sprintf("codec_%s", codec), func(t *testing.T) {
			tx, err := dbconn.Begin()
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			err = SetDefaultCompression(tx, codec)
			if err == nil {
				t.Fatalf("expected error for codec %q, got nil", codec)
			}
			if !strings.Contains(err.Error(), "not registered") {
				t.Fatalf("expected unregistered codec error for %q, got: %v", codec, err)
			}
		})
	}
}

func TestCompressionConfigOldConfigFilesStillParse(t *testing.T) {
	// Backward compatibility: old config files with v1.7/v1.8 lack compression config
	// and should be parsed successfully with defaults.
	dbconn := setupRepositoryConfigTestDB(t)
	defer func() { _ = dbconn.Close() }()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Simulate old config by deleting compression config rows
	if _, err := tx.Exec(`DELETE FROM repository_config WHERE key IN ($1, $2)`,
		repositoryDefaultCompressionKey, repositoryDefaultCompressionLevelKey); err != nil {
		t.Fatalf("delete config rows: %v", err)
	}

	// Verify parsing works
	codec, err := GetDefaultCompression(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompression: %v", err)
	}
	if codec != "none" {
		t.Fatalf("expected old config to parse to 'none', got %q", codec)
	}

	level, err := GetDefaultCompressionLevel(tx)
	if err != nil {
		t.Fatalf("GetDefaultCompressionLevel: %v", err)
	}
	if level != 3 {
		t.Fatalf("expected old config to parse to level 3, got %d", level)
	}

	// And validation should pass
	if err := ValidateRepositoryCompressionConfig(tx); err != nil {
		t.Fatalf("ValidateRepositoryCompressionConfig on old config: %v", err)
	}
}
