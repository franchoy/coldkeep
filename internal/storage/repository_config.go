package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

const repositoryDefaultChunkerKey = "default_chunker"

func ensureRegisteredChunkerVersion(version chunk.Version) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("chunker version %q is not registered in this binary", version)
		}
	}()

	_ = chunk.DefaultRegistry().MustGet(version)
	return nil
}

// GetDefaultChunkerVersion returns the repository-level default chunker version
// used for new writes.
//
// Behavior contract:
// - if repository_config.default_chunker is absent, it returns v1-simple-rolling
// - returned values must be both well-formed and currently registered
func GetDefaultChunkerVersion(tx *sql.Tx) (chunk.Version, error) {
	if tx == nil {
		return "", errors.New("nil transaction")
	}

	var raw string
	err := tx.QueryRow(
		`SELECT value FROM repository_config WHERE key = $1`,
		repositoryDefaultChunkerKey,
	).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return chunk.DefaultChunkerVersion, nil
		}
		return "", fmt.Errorf("read repository default chunker: %w", err)
	}

	version := chunk.Version(strings.TrimSpace(raw))
	if !chunk.IsWellFormedVersion(version) {
		return "", fmt.Errorf("repository default chunker version %q is malformed", version)
	}
	if err := ensureRegisteredChunkerVersion(version); err != nil {
		return "", fmt.Errorf("repository default chunker version validation failed: %w", err)
	}

	return version, nil
}

// SetDefaultChunkerVersion updates repository_config.default_chunker.
// The provided version must be well-formed and registered in the current binary.
func SetDefaultChunkerVersion(tx *sql.Tx, v chunk.Version) error {
	if tx == nil {
		return errors.New("nil transaction")
	}

	version := chunk.Version(strings.TrimSpace(string(v)))
	if !chunk.IsWellFormedVersion(version) {
		return fmt.Errorf("default chunker version %q is malformed", version)
	}
	if err := ensureRegisteredChunkerVersion(version); err != nil {
		return fmt.Errorf("default chunker version validation failed: %w", err)
	}

	if _, err := tx.Exec(
		`INSERT INTO repository_config(key, value)
		 VALUES($1, $2)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		repositoryDefaultChunkerKey,
		string(version),
	); err != nil {
		return fmt.Errorf("persist repository default chunker: %w", err)
	}

	return nil
}

const (
	repositoryDefaultCompressionKey      = "compression"
	repositoryDefaultCompressionLevelKey = "compression_level"
	defaultCompressionCodec              = storagecompression.CompressionNone
	defaultCompressionLevel              = 3
	// Phase 5.1: zstd levels initially 1-9 to avoid overly aggressive or minimal tuning.
	// Range [0, 11] preserves zstd capability; [1, 9] is initial contract boundary.
	minCompressionLevel = 1
	maxCompressionLevel = 9
)

// IsRegisteredCompressionCodec returns true if the codec is valid for repository use.
// Supported compression codecs: "none" (passthrough), "zstd".
func IsRegisteredCompressionCodec(codec string) bool {
	codec = strings.TrimSpace(codec)
	switch codec {
	case storagecompression.CompressionNone, storagecompression.CompressionZstd:
		return true
	default:
		return false
	}
}

// GetDefaultCompression returns the repository-level default compression codec.
//
// Behavior contract:
// - if repository_config.compression is absent, it returns "none"
// - returned values must be registered/valid
func GetDefaultCompression(tx *sql.Tx) (string, error) {
	if tx == nil {
		return "", errors.New("nil transaction")
	}

	var raw string
	err := tx.QueryRow(
		`SELECT value FROM repository_config WHERE key = $1`,
		repositoryDefaultCompressionKey,
	).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return defaultCompressionCodec, nil
		}
		return "", fmt.Errorf("read repository default compression: %w", err)
	}

	codec := strings.TrimSpace(raw)
	if !IsRegisteredCompressionCodec(codec) {
		return "", fmt.Errorf("repository default compression codec %q is not registered", codec)
	}

	return codec, nil
}

// SetDefaultCompression updates repository_config.compression.
// The provided codec must be registered/valid.
func SetDefaultCompression(tx *sql.Tx, codec string) error {
	if tx == nil {
		return errors.New("nil transaction")
	}

	codec = strings.TrimSpace(codec)
	if !IsRegisteredCompressionCodec(codec) {
		return fmt.Errorf("compression codec %q is not registered", codec)
	}

	if _, err := tx.Exec(
		`INSERT INTO repository_config(key, value)
		 VALUES($1, $2)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		repositoryDefaultCompressionKey,
		codec,
	); err != nil {
		return fmt.Errorf("persist repository default compression: %w", err)
	}

	return nil
}

// GetDefaultCompressionLevel returns the repository-level default compression level.
// Only valid when compression codec is "zstd".
//
// Behavior contract:
// - if repository_config.compression_level is absent, it returns 3
// - returned values must be in range [1, 9] for Phase 5.1
// - level is only relevant when compression = "zstd"
func GetDefaultCompressionLevel(tx *sql.Tx) (int, error) {
	if tx == nil {
		return 0, errors.New("nil transaction")
	}

	var raw string
	err := tx.QueryRow(
		`SELECT value FROM repository_config WHERE key = $1`,
		repositoryDefaultCompressionLevelKey,
	).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return defaultCompressionLevel, nil
		}
		return 0, fmt.Errorf("read repository default compression level: %w", err)
	}

	level := 0
	if _, err := fmt.Sscanf(strings.TrimSpace(raw), "%d", &level); err != nil {
		return 0, fmt.Errorf("repository default compression level %q is not a valid integer: %w", raw, err)
	}

	if level < minCompressionLevel || level > maxCompressionLevel {
		return 0, fmt.Errorf("repository default compression level %d is out of range [%d, %d]", level, minCompressionLevel, maxCompressionLevel)
	}

	return level, nil
}

// SetDefaultCompressionLevel updates repository_config.compression_level.
// The provided level must be in range [1, 9] for Phase 5.1.
func SetDefaultCompressionLevel(tx *sql.Tx, level int) error {
	if tx == nil {
		return errors.New("nil transaction")
	}

	if level < minCompressionLevel || level > maxCompressionLevel {
		return fmt.Errorf("compression level %d is out of range [%d, %d]", level, minCompressionLevel, maxCompressionLevel)
	}

	if _, err := tx.Exec(
		`INSERT INTO repository_config(key, value)
		 VALUES($1, $2)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		repositoryDefaultCompressionLevelKey,
		fmt.Sprintf("%d", level),
	); err != nil {
		return fmt.Errorf("persist repository default compression level: %w", err)
	}

	return nil
}

// ValidateRepositoryCompressionConfig validates the compression configuration during
// repository open/init.
//
// Contract (Phase 5.1):
// - compression codec must be "none" or "zstd"
// - compression_level is only relevant for "zstd" codec
// - compression_level must be in range [1, 9] when set
// - missing compression config defaults to "none" (no compression)
// - missing compression_level defaults to 3 (when compression is not "none")
func ValidateRepositoryCompressionConfig(tx *sql.Tx) error {
	if tx == nil {
		return errors.New("nil transaction")
	}

	// Get the configured compression codec (defaults to "none" if unset)
	codec, err := GetDefaultCompression(tx)
	if err != nil {
		return fmt.Errorf("validate compression config: failed to read codec: %w", err)
	}

	// If codec is "none", compression_level should be ignored / can be unset
	// (no validation needed for level when codec is "none")
	if codec == storagecompression.CompressionNone {
		return nil
	}

	// For "zstd" codec, validate that compression_level is valid
	if codec == storagecompression.CompressionZstd {
		level, err := GetDefaultCompressionLevel(tx)
		if err != nil {
			return fmt.Errorf("validate compression config: failed to read compression level for zstd: %w", err)
		}

		// Level is validated by GetDefaultCompressionLevel (range check),
		// so if we get here, it's valid. Just ensure it makes sense for zstd.
		if level < minCompressionLevel || level > maxCompressionLevel {
			return fmt.Errorf("validate compression config: compression level %d out of range [%d, %d] for zstd",
				level, minCompressionLevel, maxCompressionLevel)
		}

		return nil
	}

	// Fallback: codec was validated by GetDefaultCompression, but double-check
	return nil
}
