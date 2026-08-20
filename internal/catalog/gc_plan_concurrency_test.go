package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
	_ "github.com/mattn/go-sqlite3"
)

type gcPlanRaceResult struct {
	plan *GCPlanMetadata
	err  error
}

type gcPlanRaceCase struct {
	name         string
	input        GCPlanInput
	mutate       func(context.Context, *sql.Tx) error
	assertDuring func(*testing.T, *GCPlanMetadata, error)
	assertAfter  func(*testing.T, *GCPlanMetadata, error)
}

func TestGCPlanCompoundReadIsStatementCoherentAcrossBackends(t *testing.T) {
	cases := []gcPlanRaceCase{
		{
			name: "snapshot creation",
			mutate: func(ctx context.Context, tx *sql.Tx) error {
				pathID, err := insertGCRaceSnapshotPath(ctx, tx, "/race/create")
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)",
					"snap-create", time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC), "full", "created"); err != nil {
					return err
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)",
					"snap-create", pathID, int64(101))
				return err
			},
			assertDuring: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceSnapshot(t, plan, "snap-create", false)
				assertGCRaceRootSnapshot(t, plan, 101, "snap-create", false)
			},
			assertAfter: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceSnapshot(t, plan, "snap-create", true)
				assertGCRaceRootSnapshot(t, plan, 101, "snap-create", true)
			},
		},
		{
			name: "snapshot deletion",
			mutate: func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, "DELETE FROM snapshot_file WHERE snapshot_id = $1", "snap-delete"); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, "DELETE FROM snapshot WHERE id = $1", "snap-delete")
				return err
			},
			assertDuring: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceSnapshot(t, plan, "snap-delete", true)
				assertGCRaceRootSnapshot(t, plan, 101, "snap-delete", true)
			},
			assertAfter: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceSnapshot(t, plan, "snap-delete", false)
				assertGCRaceRootSnapshot(t, plan, 101, "snap-delete", false)
			},
		},
		{
			name:  "excluded snapshot deletion",
			input: GCPlanInput{ExcludeSnapshotIDs: []string{"snap-excluded"}},
			mutate: func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, "DELETE FROM snapshot_file WHERE snapshot_id = $1", "snap-excluded"); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, "DELETE FROM snapshot WHERE id = $1", "snap-excluded")
				return err
			},
			assertDuring: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceSnapshot(t, plan, "snap-excluded", false)
				assertGCRaceRootSnapshot(t, plan, 101, "snap-excluded", false)
			},
			assertAfter: func(t *testing.T, plan *GCPlanMetadata, err error) {
				t.Helper()
				if plan != nil || !IsCode(err, ErrorNotFound) {
					t.Fatalf("post-delete excluded plan = %+v, err = %v; want not_found", plan, err)
				}
			},
		},
		{
			name: "current root insertion",
			mutate: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2)", "/current/insert", int64(101))
				return err
			},
			assertDuring: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceCurrentRoot(t, plan, 101, false)
			},
			assertAfter: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceCurrentRoot(t, plan, 101, true)
			},
		},
		{
			name: "current root removal",
			mutate: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "DELETE FROM physical_file WHERE path = $1", "/current/remove")
				return err
			},
			assertDuring: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceCurrentRoot(t, plan, 102, true)
			},
			assertAfter: func(t *testing.T, plan *GCPlanMetadata, err error) {
				plan = requireGCRacePlan(t, plan, err)
				assertGCRaceCurrentRoot(t, plan, 102, false)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backendtest.ForEach(t, backendtest.Options{Postgres: backendtest.PostgresOptional}, func(t *testing.T, backend backendtest.Backend) {
				prepareGCRaceBackend(t, backend)
				seedGCRaceFixture(t, backend.DB)
				runGCRaceInterleaving(t, backend.DB, tc)
			})
		})
	}
}

func prepareGCRaceBackend(t *testing.T, backend backendtest.Backend) {
	t.Helper()
	backend.DB.SetMaxOpenConns(4)
	if backend.Kind != db.BackendSQLite {
		return
	}
	var mode string
	if err := backend.DB.QueryRow("PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		t.Fatalf("enable SQLite WAL: %v", err)
	}
	if strings.ToLower(strings.TrimSpace(mode)) != "wal" {
		t.Fatalf("enable SQLite WAL: journal_mode=%q, want wal", mode)
	}
	if err := backend.DB.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("verify SQLite WAL: %v", err)
	}
	if strings.ToLower(strings.TrimSpace(mode)) != "wal" {
		t.Fatalf("verify SQLite WAL: journal_mode=%q, want wal", mode)
	}
}

func seedGCRaceFixture(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for _, logicalID := range []int64{101, 102} {
		if _, err := dbconn.ExecContext(ctx,
			"INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, $4, $5, $6)",
			logicalID, fmt.Sprintf("race-%d", logicalID), logicalID, fmt.Sprintf("race-hash-%d", logicalID), 1, "COMPLETED"); err != nil {
			t.Fatalf("seed logical file %d: %v", logicalID, err)
		}
	}
	seedGCRaceSnapshot(t, dbconn, "snap-zero", "/race/zero", 0, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	seedGCRaceSnapshot(t, dbconn, "snap-delete", "/race/delete", 101, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	seedGCRaceSnapshot(t, dbconn, "snap-excluded", "/race/excluded", 101, time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC))
	if _, err := dbconn.ExecContext(ctx, "INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2)", "/current/remove", int64(102)); err != nil {
		t.Fatalf("seed current root: %v", err)
	}
}

func seedGCRaceSnapshot(t *testing.T, dbconn *sql.DB, snapshotID, path string, logicalID int64, createdAt time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := dbconn.ExecContext(ctx, "INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)",
		snapshotID, createdAt, "full", snapshotID); err != nil {
		t.Fatalf("seed snapshot %q: %v", snapshotID, err)
	}
	if logicalID == 0 {
		return
	}
	pathID, err := insertGCRaceSnapshotPath(ctx, dbconn, path)
	if err != nil {
		t.Fatalf("seed snapshot path %q: %v", path, err)
	}
	if _, err := dbconn.ExecContext(ctx, "INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)",
		snapshotID, pathID, logicalID); err != nil {
		t.Fatalf("seed snapshot root %q: %v", snapshotID, err)
	}
}

type gcPlanRaceDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func insertGCRaceSnapshotPath(ctx context.Context, dbconn gcPlanRaceDB, path string) (int64, error) {
	if _, err := dbconn.ExecContext(ctx, "INSERT INTO snapshot_path (path) VALUES ($1)", path); err != nil {
		return 0, err
	}
	var pathID int64
	if err := dbconn.QueryRowContext(ctx, "SELECT id FROM snapshot_path WHERE path = $1", path).Scan(&pathID); err != nil {
		return 0, err
	}
	return pathID, nil
}

func runGCRaceInterleaving(t *testing.T, dbconn *sql.DB, tc gcPlanRaceCase) {
	t.Helper()
	reached := make(chan struct{})
	resume := make(chan struct{})
	resultCh := make(chan gcPlanRaceResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	svc := NewServiceFromSQL(dbconn)
	go func() {
		plan, err := svc.loadGCPlanMetadata(ctx, tc.input, func() {
			close(reached)
			<-resume
		})
		resultCh <- gcPlanRaceResult{plan: plan, err: err}
	}()

	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatalf("GC read did not reach scan barrier: %v", ctx.Err())
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		close(resume)
		t.Fatalf("begin concurrent mutation: %v", err)
	}
	if err := tc.mutate(ctx, tx); err != nil {
		_ = tx.Rollback()
		close(resume)
		t.Fatalf("execute concurrent mutation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		close(resume)
		t.Fatalf("commit concurrent mutation: %v", err)
	}
	close(resume)

	var during gcPlanRaceResult
	select {
	case during = <-resultCh:
	case <-ctx.Done():
		t.Fatalf("GC read did not complete after barrier release: %v", ctx.Err())
	}
	tc.assertDuring(t, during.plan, during.err)

	after, afterErr := svc.LoadGCPlanMetadata(context.Background(), tc.input)
	tc.assertAfter(t, after, afterErr)
}

func requireGCRacePlan(t *testing.T, plan *GCPlanMetadata, err error) *GCPlanMetadata {
	t.Helper()
	if err != nil || plan == nil {
		t.Fatalf("GC plan = %+v, err = %v", plan, err)
	}
	assertGCRaceSnapshot(t, plan, "snap-zero", true)
	return plan
}

func assertGCRaceSnapshot(t *testing.T, plan *GCPlanMetadata, snapshotID string, want bool) {
	t.Helper()
	found := false
	for _, snapshot := range plan.ProtectedSnapshots {
		if snapshot.ID == snapshotID {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("protected snapshot %q present=%t, want %t: %+v", snapshotID, found, want, plan.ProtectedSnapshots)
	}
}

func assertGCRaceRootSnapshot(t *testing.T, plan *GCPlanMetadata, logicalID int64, snapshotID string, want bool) {
	t.Helper()
	found := false
	if root := findGCRaceRoot(plan, logicalID); root != nil {
		for _, id := range root.SnapshotIDs {
			if id == snapshotID {
				found = true
				break
			}
		}
	}
	if found != want {
		t.Fatalf("root %d snapshot %q present=%t, want %t: %+v", logicalID, snapshotID, found, want, plan.Roots)
	}
}

func assertGCRaceCurrentRoot(t *testing.T, plan *GCPlanMetadata, logicalID int64, want bool) {
	t.Helper()
	root := findGCRaceRoot(plan, logicalID)
	found := root != nil && root.Current
	if found != want {
		t.Fatalf("root %d current=%t, want %t: %+v", logicalID, found, want, plan.Roots)
	}
}

func findGCRaceRoot(plan *GCPlanMetadata, logicalID int64) *GCReachabilityRoot {
	for i := range plan.Roots {
		if plan.Roots[i].LogicalFileID == logicalID {
			return &plan.Roots[i]
		}
	}
	return nil
}

func TestGCPlanCompoundReadPreservesOrphanDetection(t *testing.T) {
	path := t.TempDir() + "/orphan.sqlite"
	dbconn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	dbconn.SetMaxOpenConns(1)
	if err := db.EnsureSchema(dbconn); err != nil {
		t.Fatalf("bootstrap SQLite: %v", err)
	}
	if _, err := dbconn.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("disable fixture foreign keys: %v", err)
	}
	if _, err := dbconn.Exec("INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)",
		"orphan-snapshot", int64(999), int64(999)); err != nil {
		t.Fatalf("insert orphan fixture: %v", err)
	}

	plan, err := NewServiceFromSQL(dbconn).LoadGCPlanMetadata(context.Background(), GCPlanInput{})
	if plan != nil || !IsCode(err, ErrorInvariantViolation) || !strings.Contains(err.Error(), "orphan-snapshot") {
		t.Fatalf("orphan plan = %+v, err = %v; want invariant_violation naming orphan", plan, err)
	}
}
