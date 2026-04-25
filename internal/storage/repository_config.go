package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
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
