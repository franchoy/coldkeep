package db_test

import (
	"context"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestBackendTransactionCommitRollbackAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		setupPhase10TransactionTable(t, backend.DB)

		if got := db.BackendFromDB(backend.DB); got != backend.Kind {
			t.Fatalf("backend detection mismatch: got %q want %q", got, backend.Kind)
		}
		wantLocks := backend.Kind == db.BackendPostgres
		if backend.Capabilities.SelectForUpdate != wantLocks ||
			backend.Capabilities.Nowait != wantLocks ||
			backend.Capabilities.SkipLocked != wantLocks {
			t.Fatalf("unexpected lock capabilities for %s: %+v", backend.Name, backend.Capabilities)
		}
		const capabilityQuery = `SELECT id FROM phase10_txn_contract WHERE id = $1`
		wantForUpdate := capabilityQuery
		wantNowait := capabilityQuery
		wantSkipLocked := capabilityQuery
		if wantLocks {
			wantForUpdate = phase10LockQuery(capabilityQuery, "FOR UPDATE")
			wantNowait = phase10LockQuery(capabilityQuery, "FOR UPDATE NOWAIT")
			wantSkipLocked = phase10LockQuery(capabilityQuery, "FOR UPDATE SKIP LOCKED")
		}
		if got := db.QueryWithOptionalForUpdate(backend.DB, capabilityQuery); got != wantForUpdate {
			t.Fatalf("%s FOR UPDATE query=%q want %q", backend.Name, got, wantForUpdate)
		}
		if got := db.QueryWithOptionalForUpdateNowait(backend.DB, capabilityQuery); got != wantNowait {
			t.Fatalf("%s FOR UPDATE NOWAIT query=%q want %q", backend.Name, got, wantNowait)
		}
		if got := db.QueryWithOptionalForUpdateSkipLocked(backend.DB, capabilityQuery); got != wantSkipLocked {
			t.Fatalf("%s FOR UPDATE SKIP LOCKED query=%q want %q", backend.Name, got, wantSkipLocked)
		}

		ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
		defer cancel()

		commitTx, err := backend.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin commit transaction: %v", err)
		}
		if _, err := commitTx.ExecContext(ctx,
			`INSERT INTO phase10_txn_contract (id, value) VALUES ($1, $2)`,
			2, "committed",
		); err != nil {
			_ = commitTx.Rollback()
			t.Fatalf("insert committed fixture row: %v", err)
		}
		var ownWrite string
		if err := commitTx.QueryRowContext(ctx,
			`SELECT value FROM phase10_txn_contract WHERE id = $1`, 2,
		).Scan(&ownWrite); err != nil {
			_ = commitTx.Rollback()
			t.Fatalf("read own insert: %v", err)
		}
		if ownWrite != "committed" {
			_ = commitTx.Rollback()
			t.Fatalf("read own insert value=%q", ownWrite)
		}
		if err := commitTx.Commit(); err != nil {
			t.Fatalf("commit insertion: %v", err)
		}

		var committed string
		if err := backend.DB.QueryRowContext(ctx,
			`SELECT value FROM phase10_txn_contract WHERE id = $1`, 2,
		).Scan(&committed); err != nil {
			t.Fatalf("read committed insertion: %v", err)
		}
		if committed != "committed" {
			t.Fatalf("committed value=%q", committed)
		}

		rollbackTx, err := backend.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin rollback transaction: %v", err)
		}
		if _, err := rollbackTx.ExecContext(ctx,
			`UPDATE phase10_txn_contract SET value = $1 WHERE id = $2`,
			"rolled-back", 1,
		); err != nil {
			_ = rollbackTx.Rollback()
			t.Fatalf("update rollback fixture row: %v", err)
		}
		if err := rollbackTx.QueryRowContext(ctx,
			`SELECT value FROM phase10_txn_contract WHERE id = $1`, 1,
		).Scan(&ownWrite); err != nil {
			_ = rollbackTx.Rollback()
			t.Fatalf("read own update: %v", err)
		}
		if ownWrite != "rolled-back" {
			_ = rollbackTx.Rollback()
			t.Fatalf("read own update value=%q", ownWrite)
		}
		if err := rollbackTx.Rollback(); err != nil {
			t.Fatalf("rollback update: %v", err)
		}

		var baseline string
		if err := backend.DB.QueryRowContext(ctx,
			`SELECT value FROM phase10_txn_contract WHERE id = $1`, 1,
		).Scan(&baseline); err != nil {
			t.Fatalf("read row after rollback: %v", err)
		}
		if baseline != "baseline" {
			t.Fatalf("rollback did not restore baseline: %q", baseline)
		}

		conflictTx, err := backend.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin constraint transaction: %v", err)
		}
		if _, err := conflictTx.ExecContext(ctx,
			`INSERT INTO phase10_txn_contract (id, value) VALUES ($1, $2)`,
			3, "temporary",
		); err != nil {
			_ = conflictTx.Rollback()
			t.Fatalf("insert pre-conflict row: %v", err)
		}
		if _, err := conflictTx.ExecContext(ctx,
			`INSERT INTO phase10_txn_contract (id, value) VALUES ($1, $2)`,
			4, "baseline",
		); err == nil {
			_ = conflictTx.Rollback()
			t.Fatal("expected uniqueness conflict")
		}
		if err := conflictTx.Rollback(); err != nil {
			t.Fatalf("rollback uniqueness conflict: %v", err)
		}

		var temporaryCount int
		if err := backend.DB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM phase10_txn_contract WHERE id = $1`, 3,
		).Scan(&temporaryCount); err != nil {
			t.Fatalf("count pre-conflict row after rollback: %v", err)
		}
		if temporaryCount != 0 {
			t.Fatalf("constraint rollback retained %d pre-conflict rows", temporaryCount)
		}

		affectedTx, err := backend.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin affected-row transaction: %v", err)
		}
		existingResult, err := affectedTx.ExecContext(ctx,
			`UPDATE phase10_txn_contract SET value = $1 WHERE id = $2`,
			"affected", 1,
		)
		if err != nil {
			_ = affectedTx.Rollback()
			t.Fatalf("update existing affected-row fixture: %v", err)
		}
		existingRows, err := existingResult.RowsAffected()
		if err != nil {
			_ = affectedTx.Rollback()
			t.Fatalf("existing RowsAffected: %v", err)
		}
		if existingRows != 1 {
			_ = affectedTx.Rollback()
			t.Fatalf("existing RowsAffected=%d want 1", existingRows)
		}
		missingResult, err := affectedTx.ExecContext(ctx,
			`UPDATE phase10_txn_contract SET value = $1 WHERE id = $2`,
			"missing", 999,
		)
		if err != nil {
			_ = affectedTx.Rollback()
			t.Fatalf("update missing affected-row fixture: %v", err)
		}
		missingRows, err := missingResult.RowsAffected()
		if err != nil {
			_ = affectedTx.Rollback()
			t.Fatalf("missing RowsAffected: %v", err)
		}
		if missingRows != 0 {
			_ = affectedTx.Rollback()
			t.Fatalf("missing RowsAffected=%d want 0", missingRows)
		}
		if err := affectedTx.Rollback(); err != nil {
			t.Fatalf("rollback affected-row transaction: %v", err)
		}

		cancelled, cancelNow := context.WithCancel(context.Background())
		cancelNow()
		cancelledTx, err := backend.DB.BeginTx(cancelled, nil)
		if cancelledTx != nil {
			_ = cancelledTx.Rollback()
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled BeginTx error=%v", err)
		}
		if _, err := backend.DB.ExecContext(cancelled,
			`UPDATE phase10_txn_contract SET value = $1 WHERE id = $2`,
			"cancelled", 1,
		); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled update error=%v", err)
		}

		if err := backend.DB.QueryRowContext(ctx,
			`SELECT value FROM phase10_txn_contract WHERE id = $1`, 1,
		).Scan(&baseline); err != nil {
			t.Fatalf("read final baseline: %v", err)
		}
		if baseline != "baseline" {
			t.Fatalf("final baseline=%q", baseline)
		}
		assertPhase10ConnectionReusable(t, backend.DB)
	})
}

func TestBackendForUpdateLockReleaseAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		setupPhase10LockTable(t, backend.DB)
		const baseQuery = `SELECT id, value FROM phase10_lock_contract WHERE id = $1`
		lockQuery := db.QueryWithOptionalForUpdate(backend.DB, baseQuery)

		if backend.Kind == db.BackendSQLite {
			if lockQuery != baseQuery {
				t.Fatalf("SQLite FOR UPDATE helper emitted %q", lockQuery)
			}
			ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
			defer cancel()
			tx, err := backend.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin SQLite boundary transaction: %v", err)
			}
			var id int64
			var value string
			if err := tx.QueryRowContext(ctx, lockQuery, 1).Scan(&id, &value); err != nil {
				_ = tx.Rollback()
				t.Fatalf("execute SQLite lock-clause boundary query: %v", err)
			}
			if id != 1 || value != "first" {
				_ = tx.Rollback()
				t.Fatalf("SQLite boundary selected (%d, %q), want (1, %q)", id, value, "first")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback SQLite boundary transaction: %v", err)
			}
			assertPhase10LockRowsUnchanged(t, backend.DB)
			assertPhase10ConnectionReusable(t, backend.DB)
			return
		}
		if lockQuery != phase10LockQuery(baseQuery, "FOR UPDATE") {
			t.Fatalf("PostgreSQL FOR UPDATE helper emitted %q", lockQuery)
		}

		for _, release := range []string{"commit", "rollback"} {
			t.Run(release+"_release", func(t *testing.T) {
				conns := openPhase10PostgresConnections(t, backend.DB)
				ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
				defer cancel()

				txA, err := conns.a.BeginTx(ctx, nil)
				if err != nil {
					t.Fatalf("begin locker transaction: %v", err)
				}
				defer rollbackPhase10Tx(t, &txA, "locker")
				var lockedID int64
				var lockedValue string
				if err := txA.QueryRowContext(ctx, lockQuery, 1).Scan(&lockedID, &lockedValue); err != nil {
					t.Fatalf("acquire PostgreSQL row lock: %v", err)
				}
				if lockedID != 1 || lockedValue != "first" {
					t.Fatalf("locker selected (%d, %q), want (1, %q)", lockedID, lockedValue, "first")
				}

				pidB := postgresBackendPID(t, conns.b)
				txB, err := conns.b.BeginTx(ctx, nil)
				if err != nil {
					t.Fatalf("begin contender transaction: %v", err)
				}
				defer rollbackPhase10Tx(t, &txB, "contender")
				results, wg := startPhase10BlockingQuery(ctx, txB, lockQuery, 1)
				waitForPostgresLockWait(t, conns.observer, pidB)

				if release == "commit" {
					if err := txA.Commit(); err != nil {
						t.Fatalf("commit locker transaction: %v", err)
					}
				} else {
					if err := txA.Rollback(); err != nil {
						t.Fatalf("rollback locker transaction: %v", err)
					}
				}
				txA = nil

				result := awaitPhase10QueryResult(t, results, wg)
				if result.err != nil {
					t.Fatalf("contender acquire after %s: %v", release, result.err)
				}
				if result.id != 1 || result.value != "first" {
					t.Fatalf(
						"contender selected (%d, %q) after %s, want (1, %q)",
						result.id,
						result.value,
						release,
						"first",
					)
				}
				if err := txB.Commit(); err != nil {
					t.Fatalf("commit contender after %s: %v", release, err)
				}
				txB = nil
			})
		}

		assertPhase10LockRowsUnchanged(t, backend.DB)
		assertPhase10ConnectionReusable(t, backend.DB)
	})
}

func TestBackendNowaitAndSkipLockedAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		setupPhase10LockTable(t, backend.DB)
		const rowQuery = `SELECT id FROM phase10_lock_contract WHERE id = $1`
		const candidateQuery = `SELECT id FROM phase10_lock_contract ORDER BY id ASC LIMIT 1`
		nowaitQuery := db.QueryWithOptionalForUpdateNowait(backend.DB, rowQuery)
		skipLockedQuery := db.QueryWithOptionalForUpdateSkipLocked(backend.DB, candidateQuery)

		if backend.Kind == db.BackendSQLite {
			if nowaitQuery != rowQuery || skipLockedQuery != candidateQuery {
				t.Fatalf("SQLite emitted PostgreSQL lock clauses: nowait=%q skip=%q", nowaitQuery, skipLockedQuery)
			}
			ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
			defer cancel()
			tx, err := backend.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin SQLite lock boundary transaction: %v", err)
			}
			var id int64
			if err := tx.QueryRowContext(ctx, skipLockedQuery).Scan(&id); err != nil {
				_ = tx.Rollback()
				t.Fatalf("execute SQLite ordered candidate query: %v", err)
			}
			if id != 1 {
				_ = tx.Rollback()
				t.Fatalf("SQLite ordered candidate id=%d want 1", id)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback SQLite lock boundary transaction: %v", err)
			}
			assertPhase10ConnectionReusable(t, backend.DB)
			return
		}

		t.Run("nowait", func(t *testing.T) {
			conns := openPhase10PostgresConnections(t, backend.DB)
			ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
			defer cancel()

			txA, err := conns.a.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin NOWAIT locker transaction: %v", err)
			}
			defer rollbackPhase10Tx(t, &txA, "NOWAIT locker")
			var id int64
			if err := txA.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(backend.DB, rowQuery), 1).Scan(&id); err != nil {
				t.Fatalf("acquire NOWAIT fixture lock: %v", err)
			}
			if id != 1 {
				t.Fatalf("NOWAIT locker selected id=%d want 1", id)
			}

			txB, err := conns.b.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin NOWAIT contender transaction: %v", err)
			}
			defer rollbackPhase10Tx(t, &txB, "NOWAIT contender")
			nowaitCtx, nowaitCancel := context.WithTimeout(context.Background(), phase10ObserverTimeout)
			defer nowaitCancel()
			err = txB.QueryRowContext(nowaitCtx, nowaitQuery, 1).Scan(&id)
			if err == nil {
				t.Fatal("expected NOWAIT lock conflict")
			}
			assertPhase10PostgresCode(t, err, "55P03")
			if err := txB.Rollback(); err != nil {
				t.Fatalf("rollback NOWAIT contender: %v", err)
			}
			txB = nil

			if err := txA.QueryRowContext(ctx, `SELECT id FROM phase10_lock_contract WHERE id = $1`, 1).Scan(&id); err != nil {
				t.Fatalf("locker transaction invalid after contender NOWAIT failure: %v", err)
			}
			if id != 1 {
				t.Fatalf("locker transaction reuse selected id=%d want 1", id)
			}
			if err := txA.Rollback(); err != nil {
				t.Fatalf("release NOWAIT locker: %v", err)
			}
			txA = nil

			txC, err := conns.b.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin post-release NOWAIT transaction: %v", err)
			}
			defer rollbackPhase10Tx(t, &txC, "post-release NOWAIT")
			if err := txC.QueryRowContext(ctx, nowaitQuery, 1).Scan(&id); err != nil {
				t.Fatalf("NOWAIT acquisition after release: %v", err)
			}
			if id != 1 {
				t.Fatalf("post-release NOWAIT selected id=%d", id)
			}
			if err := txC.Rollback(); err != nil {
				t.Fatalf("rollback post-release NOWAIT transaction: %v", err)
			}
			txC = nil
		})

		t.Run("skip_locked", func(t *testing.T) {
			conns := openPhase10PostgresConnections(t, backend.DB)
			ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
			defer cancel()

			txA, err := conns.a.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin SKIP LOCKED locker transaction: %v", err)
			}
			defer rollbackPhase10Tx(t, &txA, "SKIP LOCKED locker")
			var id int64
			if err := txA.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(backend.DB, rowQuery), 1).Scan(&id); err != nil {
				t.Fatalf("acquire SKIP LOCKED fixture lock: %v", err)
			}
			if id != 1 {
				t.Fatalf("SKIP LOCKED locker selected id=%d want 1", id)
			}

			txB, err := conns.b.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin SKIP LOCKED contender transaction: %v", err)
			}
			defer rollbackPhase10Tx(t, &txB, "SKIP LOCKED contender")
			if err := txB.QueryRowContext(ctx, skipLockedQuery).Scan(&id); err != nil {
				t.Fatalf("select unlocked candidate: %v", err)
			}
			if id != 2 {
				t.Fatalf("SKIP LOCKED selected id=%d want 2", id)
			}
			if err := txB.Commit(); err != nil {
				t.Fatalf("commit SKIP LOCKED contender: %v", err)
			}
			txB = nil

			if err := txA.Rollback(); err != nil {
				t.Fatalf("release SKIP LOCKED locker: %v", err)
			}
			txA = nil

			txC, err := conns.b.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin post-release SKIP LOCKED transaction: %v", err)
			}
			defer rollbackPhase10Tx(t, &txC, "post-release SKIP LOCKED")
			if err := txC.QueryRowContext(ctx, skipLockedQuery).Scan(&id); err != nil {
				t.Fatalf("select candidate after lock release: %v", err)
			}
			if id != 1 {
				t.Fatalf("post-release candidate id=%d want 1", id)
			}
			if err := txC.Rollback(); err != nil {
				t.Fatalf("rollback post-release SKIP LOCKED transaction: %v", err)
			}
			txC = nil
		})

		assertPhase10LockRowsUnchanged(t, backend.DB)
		assertPhase10ConnectionReusable(t, backend.DB)
	})
}

func TestBackendBlockedLockCancellationAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		setupPhase10LockTable(t, backend.DB)
		const baseQuery = `SELECT id, value FROM phase10_lock_contract WHERE id = $1`
		lockQuery := db.QueryWithOptionalForUpdate(backend.DB, baseQuery)

		if backend.Kind == db.BackendSQLite {
			if lockQuery != baseQuery {
				t.Fatalf("SQLite blocked-lock boundary emitted %q", lockQuery)
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			var id int64
			var value string
			if err := backend.DB.QueryRowContext(cancelled, lockQuery, 1).Scan(&id, &value); !errors.Is(err, context.Canceled) {
				t.Fatalf("SQLite cancelled lock-boundary query error=%v", err)
			}
			assertPhase10LockRowsUnchanged(t, backend.DB)
			assertPhase10ConnectionReusable(t, backend.DB)
			return
		}

		conns := openPhase10PostgresConnections(t, backend.DB)
		ctx, cancel := context.WithTimeout(context.Background(), phase10OperationTimeout)
		defer cancel()

		txA, err := conns.a.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin cancellation locker transaction: %v", err)
		}
		defer rollbackPhase10Tx(t, &txA, "cancellation locker")
		var id int64
		var value string
		if err := txA.QueryRowContext(ctx, lockQuery, 1).Scan(&id, &value); err != nil {
			t.Fatalf("acquire cancellation fixture lock: %v", err)
		}
		if id != 1 || value != "first" {
			t.Fatalf("cancellation locker selected (%d, %q), want (1, %q)", id, value, "first")
		}

		pidB := postgresBackendPID(t, conns.b)
		txB, err := conns.b.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin cancellation contender transaction: %v", err)
		}
		defer rollbackPhase10Tx(t, &txB, "cancellation contender")

		queryCtx, queryCancel := context.WithCancel(context.Background())
		results, wg := startPhase10BlockingQuery(queryCtx, txB, lockQuery, 1)
		waitForPostgresLockWait(t, conns.observer, pidB)
		queryCancel()

		result := awaitPhase10QueryResult(t, results, wg)
		assertPhase10Cancellation(t, queryCtx, result.err)
		// A cancelled lib/pq query can make database/sql discard B's physical
		// connection. rollbackPhase10Tx accepts that terminal cleanup outcome
		// while still reporting any other rollback failure.
		rollbackPhase10Tx(t, &txB, "cancelled contender")
		if err := txA.Rollback(); err != nil {
			t.Fatalf("release cancellation locker: %v", err)
		}
		txA = nil

		// Cancellation can retire B's dedicated connection, so the later
		// acquisition must come from the pool rather than that closed handle.
		txAfter, err := backend.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin post-cancellation transaction: %v", err)
		}
		defer rollbackPhase10Tx(t, &txAfter, "post-cancellation")
		if err := txAfter.QueryRowContext(ctx, lockQuery, 1).Scan(&id, &value); err != nil {
			t.Fatalf("acquire row after cancellation cleanup: %v", err)
		}
		if id != 1 || value != "first" {
			t.Fatalf("post-cancellation query selected (%d, %q), want (1, %q)", id, value, "first")
		}
		if err := txAfter.Rollback(); err != nil {
			t.Fatalf("rollback post-cancellation transaction: %v", err)
		}
		txAfter = nil

		assertPhase10LockRowsUnchanged(t, backend.DB)
		assertPhase10ConnectionReusable(t, backend.DB)
	})
}
