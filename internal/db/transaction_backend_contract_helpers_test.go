package db_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
)

const (
	phase10ObserverTimeout  = 2 * time.Second
	phase10OperationTimeout = 5 * time.Second
)

type phase10QueryResult struct {
	id    int64
	value string
	err   error
}

type phase10PostgresConnections struct {
	a        *sql.Conn
	b        *sql.Conn
	observer *sql.Conn
}

func setupPhase10TransactionTable(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	phase10Exec(t, dbconn, `
		CREATE TABLE phase10_txn_contract (
			id INTEGER PRIMARY KEY,
			value TEXT NOT NULL UNIQUE
		)
	`)
	phase10Exec(t, dbconn, `INSERT INTO phase10_txn_contract (id, value) VALUES ($1, $2)`, 1, "baseline")
}

func setupPhase10LockTable(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	phase10Exec(t, dbconn, `
		CREATE TABLE phase10_lock_contract (
			id INTEGER PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	phase10Exec(t, dbconn, `INSERT INTO phase10_lock_contract (id, value) VALUES ($1, $2), ($3, $4)`,
		1, "first", 2, "second")
}

func phase10Exec(t *testing.T, dbconn *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := dbconn.Exec(query, args...); err != nil {
		t.Fatalf("execute Phase 10 fixture query: %v", err)
	}
}

func openPhase10PostgresConnections(t *testing.T, dbconn *sql.DB) phase10PostgresConnections {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
	defer cancel()

	open := func(role string) *sql.Conn {
		t.Helper()
		conn, err := dbconn.Conn(ctx)
		if err != nil {
			t.Fatalf("reserve PostgreSQL %s connection: %v", role, err)
		}
		t.Cleanup(func() {
			if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
				t.Errorf("close PostgreSQL %s connection: %v", role, err)
			}
		})
		return conn
	}

	return phase10PostgresConnections{
		a:        open("locker"),
		b:        open("contender"),
		observer: open("observer"),
	}
}

func postgresBackendPID(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
	defer cancel()

	var pid int
	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatalf("query PostgreSQL backend PID: %v", err)
	}
	return pid
}

func waitForPostgresLockWait(t *testing.T, observer *sql.Conn, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), phase10ObserverTimeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var state string
		var waitEventType sql.NullString
		err := observer.QueryRowContext(ctx, `
			SELECT state, wait_event_type
			FROM pg_stat_activity
			WHERE pid = $1
		`, pid).Scan(&state, &waitEventType)
		if err == nil && state == "active" && waitEventType.Valid && waitEventType.String == "Lock" {
			return
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
			t.Fatalf("observe PostgreSQL lock wait for pid %d: %v", pid, err)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("PostgreSQL pid %d did not enter a server-observed lock wait: %v", pid, ctx.Err())
		case <-ticker.C:
		}
	}
}

func startPhase10BlockingQuery(
	ctx context.Context,
	tx *sql.Tx,
	query string,
	args ...any,
) (<-chan phase10QueryResult, *sync.WaitGroup) {
	results := make(chan phase10QueryResult, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var id int64
		var value string
		err := tx.QueryRowContext(ctx, query, args...).Scan(&id, &value)
		results <- phase10QueryResult{id: id, value: value, err: err}
	}()
	return results, &wg
}

func awaitPhase10QueryResult(t *testing.T, results <-chan phase10QueryResult, wg *sync.WaitGroup) phase10QueryResult {
	t.Helper()
	timer := time.NewTimer(phase10OperationTimeout)
	defer timer.Stop()

	select {
	case result := <-results:
		wg.Wait()
		return result
	case <-timer.C:
		t.Fatal("timed out waiting for Phase 10 blocked query to finish")
		return phase10QueryResult{}
	}
}

func assertPhase10PostgresCode(t *testing.T, err error, code string) {
	t.Helper()
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		t.Fatalf("expected PostgreSQL SQLSTATE %s, got %T: %v", code, err, err)
	}
	if got := string(pqErr.Code); got != code {
		t.Fatalf("expected PostgreSQL SQLSTATE %s, got %s: %v", code, got, err)
	}
}

func assertPhase10Cancellation(t *testing.T, ctx context.Context, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected blocked PostgreSQL query cancellation error")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("expected cancelled query context, got %v", ctx.Err())
	}
	if errors.Is(err, context.Canceled) {
		return
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) && string(pqErr.Code) == "57014" {
		return
	}
	t.Fatalf("expected context cancellation or PostgreSQL SQLSTATE 57014, got %T: %v", err, err)
}

func assertPhase10ConnectionReusable(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
	defer cancel()

	var one int
	if err := dbconn.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("final connection reuse query: %v", err)
	}
	if one != 1 {
		t.Fatalf("final connection reuse query returned %d", one)
	}
}

func assertPhase10LockRowsUnchanged(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
	defer cancel()

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, value
		FROM phase10_lock_contract
		ORDER BY id
	`)
	if err != nil {
		t.Fatalf("query final Phase 10 lock rows: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close final Phase 10 lock rows: %v", err)
		}
	}()

	want := []phase10QueryResult{
		{id: 1, value: "first"},
		{id: 2, value: "second"},
	}
	for index, expected := range want {
		if !rows.Next() {
			t.Fatalf("missing final Phase 10 lock row %d", expected.id)
		}
		var id int64
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("scan final Phase 10 lock row %d: %v", expected.id, err)
		}
		if id != expected.id || value != expected.value {
			t.Fatalf(
				"final Phase 10 lock row %d = (%d, %q), want (%d, %q)",
				index,
				id,
				value,
				expected.id,
				expected.value,
			)
		}
	}
	if rows.Next() {
		t.Fatal("unexpected extra final Phase 10 lock row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate final Phase 10 lock rows: %v", err)
	}
}

func rollbackPhase10Tx(t *testing.T, tx **sql.Tx, role string) {
	t.Helper()
	if tx == nil || *tx == nil {
		return
	}
	if err := (*tx).Rollback(); err != nil &&
		!errors.Is(err, sql.ErrTxDone) &&
		!errors.Is(err, driver.ErrBadConn) {
		t.Errorf("rollback Phase 10 %s transaction: %v", role, err)
	}
	*tx = nil
}

func phase10LockQuery(base string, clause string) string {
	return fmt.Sprintf("%s %s", base, clause)
}
