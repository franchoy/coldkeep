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

// GetDefaultCompression returns the repository-level default compression codec.
// The provided db handle is optional and allows callers to pass an explicit
// connection; when nil, the repository's configured DB is used.
func (r *Repository) GetDefaultCompression(dbconn *sql.DB) (string, error) {
	if dbconn == nil {
		if r == nil || r.db == nil {
			return defaultCompressionCodec, nil
		}
		dbconn = r.db
	}

	tx, err := dbconn.Begin()
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	return GetDefaultCompression(tx)
}

// SetDefaultCompression persists the repository-level default compression codec.
// The provided db handle is optional and allows callers to pass an explicit
// connection; when nil, the repository's configured DB is used.
func (r *Repository) SetDefaultCompression(dbconn *sql.DB, codec string) error {
	if dbconn == nil {
		if r == nil || r.db == nil {
			return nil
		}
		dbconn = r.db
	}

	tx, err := dbconn.Begin()
	if err != nil {
		return err
	}

	if err := SetDefaultCompression(tx, codec); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return nil
}

// GetDefaultCompressionLevel returns the repository-level default compression
// level. The provided db handle is optional and allows callers to pass an
// explicit connection; when nil, the repository's configured DB is used.
func (r *Repository) GetDefaultCompressionLevel(dbconn *sql.DB) (int, error) {
	if dbconn == nil {
		if r == nil || r.db == nil {
			return defaultCompressionLevel, nil
		}
		dbconn = r.db
	}

	tx, err := dbconn.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	return GetDefaultCompressionLevel(tx)
}

// SetDefaultCompressionLevel persists the repository-level default compression
// level. The provided db handle is optional and allows callers to pass an
// explicit connection; when nil, the repository's configured DB is used.
func (r *Repository) SetDefaultCompressionLevel(dbconn *sql.DB, level int) error {
	if dbconn == nil {
		if r == nil || r.db == nil {
			return nil
		}
		dbconn = r.db
	}

	tx, err := dbconn.Begin()
	if err != nil {
		return err
	}

	if err := SetDefaultCompressionLevel(tx, level); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return nil
}
