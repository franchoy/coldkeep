package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/utils_env"
	_ "github.com/lib/pq"
)

var connectTimeout = loadConnectTimeout()
var operationTimeout = loadOperationTimeout()
var statementTimeout = loadStatementTimeout()
var lockTimeout = loadLockTimeout()
var idleInTransactionTimeout = loadIdleInTransactionTimeout()
var maxOpenConns = loadMaxOpenConns()
var maxIdleConns = loadMaxIdleConns()
var connMaxLifetime = loadConnMaxLifetime()
var connMaxIdleTime = loadConnMaxIdleTime()

func loadConnectTimeout() time.Duration {
	const defaultTimeout = 5 * time.Second
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_CONNECT_TIMEOUT_MS", int64(defaultTimeout/time.Millisecond))
	if valueMs <= 0 {
		return defaultTimeout
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadOperationTimeout() time.Duration {
	const defaultTimeout = 30 * time.Second
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_OPERATION_TIMEOUT_MS", int64(defaultTimeout/time.Millisecond))
	if valueMs <= 0 {
		return defaultTimeout
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadStatementTimeout() time.Duration {
	return loadSessionTimeout("COLDKEEP_DB_STATEMENT_TIMEOUT_MS", operationTimeout)
}

func loadLockTimeout() time.Duration {
	const defaultTimeout = 5 * time.Second
	return loadSessionTimeout("COLDKEEP_DB_LOCK_TIMEOUT_MS", defaultTimeout)
}

func loadIdleInTransactionTimeout() time.Duration {
	const defaultTimeout = 60 * time.Second
	return loadSessionTimeout("COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS", defaultTimeout)
}

func loadMaxOpenConns() int {
	const defaultMaxOpenConns = 25
	value := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_MAX_OPEN_CONNS", defaultMaxOpenConns)
	if value <= 0 {
		return defaultMaxOpenConns
	}
	return int(value)
}

func loadMaxIdleConns() int {
	const defaultMaxIdleConns = 5
	value := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_MAX_IDLE_CONNS", defaultMaxIdleConns)
	if value < 0 {
		return defaultMaxIdleConns
	}
	return int(value)
}

func loadConnMaxLifetime() time.Duration {
	const defaultLifetime = 30 * time.Minute
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_CONN_MAX_LIFETIME_MS", int64(defaultLifetime/time.Millisecond))
	if valueMs < 0 {
		return defaultLifetime
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadConnMaxIdleTime() time.Duration {
	const defaultIdleTime = 5 * time.Minute
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS", int64(defaultIdleTime/time.Millisecond))
	if valueMs < 0 {
		return defaultIdleTime
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadSessionTimeout(envVar string, defaultTimeout time.Duration) time.Duration {
	valueMs := utils_env.GetenvOrDefaultInt64(envVar, int64(defaultTimeout/time.Millisecond))
	if valueMs <= 0 {
		return defaultTimeout
	}
	return time.Duration(valueMs) * time.Millisecond
}

func DefaultOperationTimeout() time.Duration {
	return operationTimeout
}

func NewOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, operationTimeout)
}

func ApplySQLiteSessionPragmas(db *sql.DB) error {
	busyTimeoutMs := DefaultOperationTimeout() / time.Millisecond
	if busyTimeoutMs <= 0 {
		busyTimeoutMs = 1
	}
	_, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMs))
	return err
}

func buildConnectionOptions() string {
	return strings.Join([]string{
		fmt.Sprintf("-c statement_timeout=%d", statementTimeout/time.Millisecond),
		fmt.Sprintf("-c lock_timeout=%d", lockTimeout/time.Millisecond),
		fmt.Sprintf("-c idle_in_transaction_session_timeout=%d", idleInTransactionTimeout/time.Millisecond),
	}, " ")
}

func ConnectDB() (*sql.DB, error) {
	connStr := "host=" + os.Getenv("DB_HOST") +
		" port=" + os.Getenv("DB_PORT") +
		" user=" + os.Getenv("DB_USER") +
		" password=" + os.Getenv("DB_PASSWORD") +
		" dbname=" + os.Getenv("DB_NAME") +
		" sslmode=" + utils_env.GetenvOrDefault("DB_SSLMODE", "disable") +
		fmt.Sprintf(" connect_timeout=%d", max(1, int(connectTimeout/time.Second))) +
		fmt.Sprintf(" options='%s'", buildConnectionOptions())

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// DBTX is implemented by *sql.DB and *sql.Tx (so we can reuse helpers inside a tx).
type DBTX interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}
