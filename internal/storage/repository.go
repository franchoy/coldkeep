package storage

import (
	"database/sql"

	"github.com/franchoy/coldkeep/internal/chunk"
)

// Repository is the storage-layer persistence handle used by StoreService.
// Phase 3 keeps it intentionally small: it provides one place to hang future
// store-path repository methods without changing the service shape again.
type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) DB() *sql.DB {
	if r == nil {
		return nil
	}
	return r.db
}

// GetDefaultChunkerVersion returns the repository-level write default.
// It is transaction-backed so reads share the same persistence contract as
// other storage metadata accessors.
func (r *Repository) GetDefaultChunkerVersion() (chunk.Version, error) {
	if r == nil || r.db == nil {
		return chunk.DefaultChunkerVersion, nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	return GetDefaultChunkerVersion(tx)
}

// SetDefaultChunkerVersion persists the repository-level write default.
func (r *Repository) SetDefaultChunkerVersion(v chunk.Version) error {
	if r == nil || r.db == nil {
		return nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	if err := SetDefaultChunkerVersion(tx, v); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return nil
}
