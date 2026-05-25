package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
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
	const defaultTimeout = 5 * time.Minute
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_DB_OPERATION_TIMEOUT_MS", int64(defaultTimeout/time.Millisecond))
	if valueMs <= 0 {
		return defaultTimeout
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadStatementTimeout() time.Duration {
	const defaultStatementTimeout = 30 * time.Second
	return loadSessionTimeout("COLDKEEP_DB_STATEMENT_TIMEOUT_MS", defaultStatementTimeout)
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

func DefaultStatementTimeout() time.Duration {
	return statementTimeout
}

func NewOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, operationTimeout)
}

func ApplySQLiteSessionPragmas(db *sql.DB) error {
	// Use the statement timeout (not the much larger operation timeout) so that
	// SQLite lock contention does not wait for minutes.
	busyTimeoutMillis := int(DefaultStatementTimeout() / time.Millisecond)
	if busyTimeoutMillis <= 0 {
		busyTimeoutMillis = 1
	}
	_, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMillis))
	return err
}

func buildConnectionOptions() string {
	return strings.Join([]string{
		fmt.Sprintf("-c statement_timeout=%d", statementTimeout/time.Millisecond),
		fmt.Sprintf("-c lock_timeout=%d", lockTimeout/time.Millisecond),
		fmt.Sprintf("-c idle_in_transaction_session_timeout=%d", idleInTransactionTimeout/time.Millisecond),
	}, " ")
}

func dbEnvOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func validateDBConnComponent(name, value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if strings.ContainsRune(trimmed, '\x00') {
		return "", fmt.Errorf("%s must not contain NUL", name)
	}
	if trimmed == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	return trimmed, nil
}

func validateDBPort(value string) (string, error) {
	trimmed, err := validateDBConnComponent("DB_PORT", value)
	if err != nil {
		return "", err
	}
	port, err := strconv.Atoi(trimmed)
	if err != nil {
		return "", fmt.Errorf("DB_PORT must be a base-10 integer: %w", err)
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("DB_PORT must be in range 1..65535")
	}
	return trimmed, nil
}

func validateDBSSLMode(value string) (string, error) {
	trimmed, err := validateDBConnComponent("DB_SSLMODE", value)
	if err != nil {
		return "", err
	}
	switch trimmed {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return trimmed, nil
	default:
		return "", fmt.Errorf("DB_SSLMODE must be one of disable|allow|prefer|require|verify-ca|verify-full")
	}
}

func escapePostgresConnValue(value string) string {
	escaped := strings.ReplaceAll(value, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return "'" + escaped + "'"
}

type postgresConnParams struct {
	host     string
	port     string
	user     string
	password string
	dbName   string
	sslMode  string
	options  string
}

func resolvePostgresConnParams(dbName string) (postgresConnParams, error) {
	host, err := validateDBConnComponent("DB_HOST", dbEnvOrDefault("DB_HOST", "127.0.0.1"))
	if err != nil {
		return postgresConnParams{}, err
	}
	port, err := validateDBPort(dbEnvOrDefault("DB_PORT", "5432"))
	if err != nil {
		return postgresConnParams{}, err
	}
	user, err := validateDBConnComponent("DB_USER", dbEnvOrDefault("DB_USER", "coldkeep"))
	if err != nil {
		return postgresConnParams{}, err
	}
	password, err := resolveDBPassword()
	if err != nil {
		return postgresConnParams{}, err
	}
	resolvedDBName, err := resolveDBName(dbName)
	if err != nil {
		return postgresConnParams{}, err
	}
	sslMode, err := validateDBSSLMode(dbEnvOrDefault("DB_SSLMODE", "disable"))
	if err != nil {
		return postgresConnParams{}, err
	}
	options, err := resolvePostgresOptions()
	if err != nil {
		return postgresConnParams{}, err
	}

	return postgresConnParams{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		dbName:   resolvedDBName,
		sslMode:  sslMode,
		options:  options,
	}, nil
}

func resolveDBPassword() (string, error) {
	password := os.Getenv("DB_PASSWORD")
	if strings.ContainsRune(password, '\x00') {
		return "", fmt.Errorf("DB_PASSWORD must not contain NUL")
	}
	return password, nil
}

func resolveDBName(dbName string) (string, error) {
	resolvedDBName := strings.TrimSpace(dbName)
	if resolvedDBName == "" {
		resolvedDBName = dbEnvOrDefault("DB_NAME", "coldkeep")
	}
	return validateDBConnComponent("DB_NAME", resolvedDBName)
}

func resolvePostgresOptions() (string, error) {
	options := buildConnectionOptions()
	if strings.ContainsRune(options, '\x00') {
		return "", fmt.Errorf("postgres options must not contain NUL")
	}
	return options, nil
}

func buildPostgresConnStringForDatabase(dbName string) (string, error) {
	params, err := resolvePostgresConnParams(dbName)
	if err != nil {
		return "", err
	}

	connStr := strings.Join([]string{
		"host=" + escapePostgresConnValue(params.host),
		"port=" + escapePostgresConnValue(params.port),
		"user=" + escapePostgresConnValue(params.user),
		"password=" + escapePostgresConnValue(params.password),
		"dbname=" + escapePostgresConnValue(params.dbName),
		"sslmode=" + escapePostgresConnValue(params.sslMode),
		fmt.Sprintf("connect_timeout=%d", max(1, int(connectTimeout/time.Second))),
		"options=" + escapePostgresConnValue(params.options),
	}, " ")

	return connStr, nil
}

func BuildPostgresConnStringFromEnv(databaseName string) (string, error) {
	return buildPostgresConnStringForDatabase(databaseName)
}

func ConnectDB() (*sql.DB, error) {
	connStr, err := buildPostgresConnStringForDatabase("")
	if err != nil {
		return nil, err
	}

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

	if err := EnsurePostgresSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// DBTX is implemented by *sql.DB and *sql.Tx (so we can reuse helpers inside a tx).
type DBTX interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
