package storage

import "database/sql"

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
