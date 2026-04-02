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

// SupportsSelectForUpdateSkipLocked reports whether FOR UPDATE SKIP LOCKED is supported.
func SupportsSelectForUpdateSkipLocked(dbconn *sql.DB) bool {
	return SupportsSelectForUpdate(dbconn)
}

// SupportsSelectForUpdateNowait reports whether FOR UPDATE NOWAIT is supported.
func SupportsSelectForUpdateNowait(dbconn *sql.DB) bool {
	return SupportsSelectForUpdate(dbconn)
}

// QueryWithOptionalForUpdate appends FOR UPDATE only for backends that support it.
func QueryWithOptionalForUpdate(dbconn *sql.DB, query string) string {
	if SupportsSelectForUpdate(dbconn) {
		return query + " FOR UPDATE"
	}
	return query
}

// QueryWithOptionalForUpdateSkipLocked appends FOR UPDATE SKIP LOCKED when supported.
func QueryWithOptionalForUpdateSkipLocked(dbconn *sql.DB, query string) string {
	if SupportsSelectForUpdateSkipLocked(dbconn) {
		return query + " FOR UPDATE SKIP LOCKED"
	}
	return query
}

// QueryWithOptionalForUpdateNowait appends FOR UPDATE NOWAIT when supported.
func QueryWithOptionalForUpdateNowait(dbconn *sql.DB, query string) string {
	if SupportsSelectForUpdateNowait(dbconn) {
		return query + " FOR UPDATE NOWAIT"
	}
	return query
}
