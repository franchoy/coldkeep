package container

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

const phase10ContainerOperationTimeout = 5 * time.Second

func TestContainerRowLockIntegrationAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		ctx, cancel := context.WithTimeout(context.Background(), phase10ContainerOperationTimeout)
		defer cancel()

		seedPhase10ContainerRows(t, ctx, backend.DB)

		if backend.Kind == db.BackendSQLite {
			testPhase10SQLiteContainerLockBoundary(t, ctx, backend.DB)
			assertPhase10ContainerRowsReusable(t, ctx, backend.DB)
			return
		}

		t.Run("nowait_savepoint_recovery", func(t *testing.T) {
			testPhase10PostgresContainerNowait(t, ctx, backend.DB)
		})
		t.Run("skip_locked_candidate_order", func(t *testing.T) {
			testPhase10PostgresContainerSkipLocked(t, ctx, backend.DB)
		})

		assertPhase10ContainerRowsReusable(t, ctx, backend.DB)
	})
}

func seedPhase10ContainerRows(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()

	_, err := database.ExecContext(ctx, `
		INSERT INTO container (
			id, filename, current_size, max_size, sealed, sealing, quarantine
		) VALUES
			(1001, 'phase10-container-1.bin', 64, 4096, FALSE, FALSE, FALSE),
			(1002, 'phase10-container-2.bin', 64, 4096, FALSE, FALSE, FALSE)
	`)
	if err != nil {
		t.Fatalf("seed Phase 10 container rows: %v", err)
	}
}

func testPhase10SQLiteContainerLockBoundary(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()

	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin SQLite container transaction: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, tx)

	if err := lockContainerRowNowaitWithRetry(tx, database, 1001, 1, time.Millisecond); err != nil {
		t.Fatalf("SQLite lock helper should execute its clause-free lookup: %v", err)
	}

	id, _, _, err := selectOpenContainerExcluding(tx, database, 0)
	if err != nil {
		t.Fatalf("select first SQLite open container: %v", err)
	}
	if id != 1001 {
		t.Fatalf("SQLite ordered lookup returned container %d, want 1001", id)
	}
	id, _, _, err = selectOpenContainerExcluding(tx, database, 1001)
	if err != nil {
		t.Fatalf("select SQLite open container excluding 1001: %v", err)
	}
	if id != 1002 {
		t.Fatalf("SQLite exclusion lookup returned container %d, want 1002", id)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("roll back SQLite container transaction: %v", err)
	}
}

func testPhase10PostgresContainerNowait(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()

	connA := phase10ContainerConn(t, ctx, database)
	defer closePhase10ContainerConn(t, connA, "NOWAIT locker")
	connB := phase10ContainerConn(t, ctx, database)
	defer closePhase10ContainerConn(t, connB, "NOWAIT contender")

	txA, err := connA.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL container lock holder: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, txA)

	lockQuery := db.QueryWithOptionalForUpdate(database, "SELECT id FROM container WHERE id = $1")
	var lockedID int64
	if err := txA.QueryRowContext(ctx, lockQuery, 1001).Scan(&lockedID); err != nil {
		t.Fatalf("lock PostgreSQL container row: %v", err)
	}
	if lockedID != 1001 {
		t.Fatalf("NOWAIT locker selected container %d, want 1001", lockedID)
	}

	txB, err := connB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL NOWAIT contender: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, txB)

	err = lockContainerRowNowaitWithRetry(txB, database, 1001, 1, time.Millisecond)
	if !errors.Is(err, ErrContainerLockContention) {
		t.Fatalf("NOWAIT helper error = %v, want ErrContainerLockContention", err)
	}

	var one int
	if err := txB.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("NOWAIT savepoint should leave transaction usable: %v", err)
	}
	if one != 1 {
		t.Fatalf("transaction reuse query returned %d, want 1", one)
	}

	if err := txB.Rollback(); err != nil {
		t.Fatalf("roll back PostgreSQL NOWAIT contender: %v", err)
	}
	if err := txA.Commit(); err != nil {
		t.Fatalf("commit PostgreSQL container lock holder: %v", err)
	}

	txAfter, err := connB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL post-release transaction: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, txAfter)

	if err := lockContainerRowNowaitWithRetry(
		txAfter,
		database,
		1001,
		1,
		time.Millisecond,
	); err != nil {
		t.Fatalf("NOWAIT helper after release: %v", err)
	}
	if err := txAfter.Rollback(); err != nil {
		t.Fatalf("roll back PostgreSQL post-release transaction: %v", err)
	}
}

func testPhase10PostgresContainerSkipLocked(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()

	connA := phase10ContainerConn(t, ctx, database)
	defer closePhase10ContainerConn(t, connA, "SKIP LOCKED locker")
	connB := phase10ContainerConn(t, ctx, database)
	defer closePhase10ContainerConn(t, connB, "SKIP LOCKED selector")

	txA, err := connA.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL SKIP LOCKED holder: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, txA)

	lockQuery := db.QueryWithOptionalForUpdate(database, "SELECT id FROM container WHERE id = $1")
	var lockedID int64
	if err := txA.QueryRowContext(ctx, lockQuery, 1001).Scan(&lockedID); err != nil {
		t.Fatalf("lock lower PostgreSQL container row: %v", err)
	}
	if lockedID != 1001 {
		t.Fatalf("SKIP LOCKED locker selected container %d, want 1001", lockedID)
	}

	txB, err := connB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL SKIP LOCKED selector: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, txB)

	selectedID, _, _, err := selectOpenContainerExcluding(txB, database, 0)
	if err != nil {
		t.Fatalf("select unlocked PostgreSQL container: %v", err)
	}
	if selectedID != 1002 {
		t.Fatalf("SKIP LOCKED selected container %d, want 1002", selectedID)
	}

	if err := txB.Rollback(); err != nil {
		t.Fatalf("roll back PostgreSQL SKIP LOCKED selector: %v", err)
	}
	if err := txA.Rollback(); err != nil {
		t.Fatalf("release lower PostgreSQL container row: %v", err)
	}

	txAfter, err := connB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL post-release selector: %v", err)
	}
	defer rollbackPhase10ContainerTx(t, txAfter)

	selectedID, _, _, err = selectOpenContainerExcluding(txAfter, database, 0)
	if err != nil {
		t.Fatalf("select PostgreSQL container after release: %v", err)
	}
	if selectedID != 1001 {
		t.Fatalf("post-release selector returned container %d, want 1001", selectedID)
	}
	selectedID, _, _, err = selectOpenContainerExcluding(txAfter, database, 1001)
	if err != nil {
		t.Fatalf("select PostgreSQL container excluding 1001: %v", err)
	}
	if selectedID != 1002 {
		t.Fatalf("PostgreSQL exclusion selector returned container %d, want 1002", selectedID)
	}
	if err := txAfter.Rollback(); err != nil {
		t.Fatalf("roll back PostgreSQL post-release selector: %v", err)
	}
}

func phase10ContainerConn(t *testing.T, ctx context.Context, database *sql.DB) *sql.Conn {
	t.Helper()

	conn, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve PostgreSQL container connection: %v", err)
	}
	return conn
}

func closePhase10ContainerConn(t *testing.T, conn *sql.Conn, role string) {
	t.Helper()

	if err := conn.Close(); err != nil {
		t.Errorf("close PostgreSQL container %s connection: %v", role, err)
	}
}

func rollbackPhase10ContainerTx(t *testing.T, tx *sql.Tx) {
	t.Helper()

	if tx == nil {
		return
	}
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("clean up Phase 10 container transaction: %v", err)
	}
}

func assertPhase10ContainerRowsReusable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()

	var one int
	if err := database.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("final container connection reuse query: %v", err)
	}
	if one != 1 {
		t.Fatalf("final container reuse query returned %d, want 1", one)
	}

	var count int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM container WHERE id IN (1001, 1002)",
	).Scan(&count); err != nil {
		t.Fatalf("verify Phase 10 container fixture rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("Phase 10 container row count = %d, want 2", count)
	}
}
