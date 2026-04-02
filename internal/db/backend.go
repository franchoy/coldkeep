package db

import (
	"database/sql"

	"github.com/lib/pq"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// Backend identifies the SQL backend in use for a given *sql.DB.
type Backend string

const (
	BackendUnknown  Backend = "unknown"
	BackendPostgres Backend = "postgres"
	BackendSQLite   Backend = "sqlite"
)

// BackendFromDB detects the configured backend from the DB driver.
func BackendFromDB(dbconn *sql.DB) Backend {
	if dbconn == nil {
		return BackendUnknown
	}

	switch dbconn.Driver().(type) {
	case *pq.Driver:
		return BackendPostgres
	case *sqlite3.SQLiteDriver:
		return BackendSQLite
	default:
		return BackendUnknown
	}
}

// SupportsSelectForUpdate reports whether SELECT ... FOR UPDATE is supported.
func SupportsSelectForUpdate(dbconn *sql.DB) bool {
	return BackendFromDB(dbconn) == BackendPostgres
}
