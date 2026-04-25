package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
)

const repositoryDefaultChunkerKey = "default_chunker"

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
	if _, ok := chunk.DefaultRegistry().Get(version); !ok {
		return "", fmt.Errorf("repository default chunker version %q is not registered in this binary", version)
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
	if _, ok := chunk.DefaultRegistry().Get(version); !ok {
		return fmt.Errorf("default chunker version %q is not registered in this binary", version)
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
