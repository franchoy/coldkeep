package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/container"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"github.com/mattn/go-sqlite3"
)

type callerContextProbeKey struct{}

var errCallerContextProbeStop = errors.New("caller context probe stop")

type callerContextProbe struct {
	observed chan bool
	onQuery  func(context.Context)
}

func (p *callerContextProbe) record(ctx context.Context) {
	select {
	case p.observed <- ctx != nil && ctx.Value(callerContextProbeKey{}) == "caller":
	default:
	}
}

type callerContextProbeConnector struct {
	probe *callerContextProbe
}

func (c *callerContextProbeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.probe.record(ctx)
	return &callerContextProbeConn{probe: c.probe}, nil
}

func (c *callerContextProbeConnector) Driver() driver.Driver { return callerContextProbeDriver{} }

type callerContextProbeDriver struct{}

func (callerContextProbeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("caller context probe requires connector")
}

type callerContextProbeConn struct {
	probe *callerContextProbe
}

func (c *callerContextProbeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errCallerContextProbeStop
}
func (c *callerContextProbeConn) Close() error              { return nil }
func (c *callerContextProbeConn) Begin() (driver.Tx, error) { return nil, errCallerContextProbeStop }
func (c *callerContextProbeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.probe.record(ctx)
	return nil, errCallerContextProbeStop
}
func (c *callerContextProbeConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.probe.record(ctx)
	if c.probe.onQuery != nil {
		c.probe.onQuery(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return nil, errCallerContextProbeStop
}
func (c *callerContextProbeConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.probe.record(ctx)
	return nil, errCallerContextProbeStop
}
func (c *callerContextProbeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

type cancellationFinalizeWriter struct {
	*container.SimulatedWriter
	err   error
	calls int
}

func (w *cancellationFinalizeWriter) FinalizeContainer() error {
	w.calls++
	return w.err
}

func newCallerContextProbeDB(t *testing.T) (*sql.DB, *callerContextProbe) {
	t.Helper()
	probe := &callerContextProbe{observed: make(chan bool, 8)}
	dbconn := sql.OpenDB(&callerContextProbeConnector{probe: probe})
	t.Cleanup(func() { _ = dbconn.Close() })
	return dbconn, probe
}

func assertCallerContextObserved(t *testing.T, probe *callerContextProbe) {
	t.Helper()
	select {
	case observed := <-probe.observed:
		if !observed {
			t.Fatal("ordinary lower-layer work did not receive the Engine caller context")
		}
	default:
		t.Fatal("ordinary lower-layer work did not execute through the context probe")
	}
}

func TestEngineCallerContextReachesAffectedOperationWork(t *testing.T) {
	t.Run("Store", func(t *testing.T) {
		dbconn := newEngineTestDB(t)
		containersDir := t.TempDir()
		sgctx := storage.StorageContext{
			DB:           dbconn,
			Writer:       container.NewSimulatedWriter(1 << 20),
			ContainerDir: containersDir,
		}
		observed := false
		storage.InstallTestStoreInterleavingHooks(&sgctx, func(ctx context.Context, _ storage.TestStoreInterleavingHookEvent) error {
			observed = ctx.Value(callerContextProbeKey{}) == "caller"
			return errCallerContextProbeStop
		})
		eng, err := New(Config{DB: dbconn, ContainerDir: containersDir, StoreContext: &sgctx})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		input := filepath.Join(t.TempDir(), "store-context.txt")
		if err := os.WriteFile(input, []byte("caller context"), 0o600); err != nil {
			t.Fatalf("write input: %v", err)
		}
		ctx := context.WithValue(context.Background(), callerContextProbeKey{}, "caller")
		_, _ = eng.Store(ctx, StoreRequest{SourcePath: input, Codec: "plain"})
		if !observed {
			t.Fatal("Store ordinary work did not receive the Engine caller context")
		}
	})

	for _, tc := range []struct {
		name string
		run  func(context.Context, *DefaultEngine)
	}{
		{name: "Remove", run: func(ctx context.Context, eng *DefaultEngine) {
			_, _ = eng.Remove(ctx, RemoveRequest{FileIDs: []int64{1}})
		}},
		{name: "RemoveStoredPaths", run: func(ctx context.Context, eng *DefaultEngine) {
			_, _ = eng.RemoveStoredPaths(ctx, RemoveStoredPathsRequest{StoredPaths: []string{"file.txt"}})
		}},
		{name: "Restore", run: func(ctx context.Context, eng *DefaultEngine) {
			_, _ = eng.Restore(ctx, RestoreRequest{FileIDs: []int64{1}, DestinationRoot: t.TempDir()})
		}},
		{name: "RestoreStoredPath", run: func(ctx context.Context, eng *DefaultEngine) {
			_, _ = eng.RestoreStoredPath(ctx, RestoreStoredPathRequest{StoredPath: "file.txt", Overwrite: true})
		}},
		{name: "Verify", run: func(ctx context.Context, eng *DefaultEngine) {
			_, _ = eng.Verify(ctx, VerifyRequest{Target: "file", FileID: 1, Level: "standard"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbconn, probe := newCallerContextProbeDB(t)
			eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			ctx := context.WithValue(context.Background(), callerContextProbeKey{}, "caller")
			tc.run(ctx, eng)
			assertCallerContextObserved(t, probe)
		})
	}

	t.Run("DoctorSchema", func(t *testing.T) {
		dbconn, probe := newCallerContextProbeDB(t)
		eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		eng.doctorRecover = func(context.Context) (RecoverResult, error) { return RecoverResult{}, nil }
		ctx := context.WithValue(context.Background(), callerContextProbeKey{}, "caller")
		_, _ = eng.Doctor(ctx, DoctorRequest{})
		assertCallerContextObserved(t, probe)
	})

	t.Run("DoctorAudit", func(t *testing.T) {
		dbconn, probe := newCallerContextProbeDB(t)
		eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		eng.doctorRecover = func(context.Context) (RecoverResult, error) { return RecoverResult{}, nil }
		eng.doctorSchema = func(context.Context, *sql.DB) (int64, error) { return 16, nil }
		eng.doctorVerify = func(context.Context, string) error { return nil }
		ctx := context.WithValue(context.Background(), callerContextProbeKey{}, "caller")
		_, _ = eng.Doctor(ctx, DoctorRequest{})
		assertCallerContextObserved(t, probe)
	})
}

// The callbacks observe real public API operations; they do not replace item work.
// SQLite read observations additionally prove that cancellation does not reach a
// second target, while the uncancelled control exercises both seeded targets.
func TestEngineBatchCancellationStopsNewDispatchWithPartialResult(t *testing.T) {
	type outcome struct {
		summary  BatchSummary
		statuses []BatchItemStatus
		err      error
	}
	operations := []struct {
		name string
		run  func(context.Context, *DefaultEngine, bool, string) outcome
	}{
		{"Remove", func(ctx context.Context, eng *DefaultEngine, failFast bool, output string) outcome {
			result, err := eng.Remove(ctx, RemoveRequest{FileIDs: []int64{1, 2}, DryRun: true, FailFast: failFast})
			out := outcome{summary: result.Summary, err: err}
			for _, item := range result.Items {
				out.statuses = append(out.statuses, item.Status)
			}
			return out
		}},
		{"Restore", func(ctx context.Context, eng *DefaultEngine, failFast bool, output string) outcome {
			result, err := eng.Restore(ctx, RestoreRequest{FileIDs: []int64{1, 2}, DestinationRoot: output, DryRun: true, FailFast: failFast})
			out := outcome{summary: result.Summary, err: err}
			for _, item := range result.Items {
				out.statuses = append(out.statuses, item.Status)
			}
			return out
		}},
		{"RemoveStoredPaths", func(ctx context.Context, eng *DefaultEngine, failFast bool, output string) outcome {
			result, err := eng.RemoveStoredPaths(ctx, RemoveStoredPathsRequest{StoredPaths: []string{"first.txt", "second.txt"}, DryRun: true, FailFast: failFast})
			out := outcome{summary: result.Summary, err: err}
			for _, item := range result.Items {
				out.statuses = append(out.statuses, item.Status)
			}
			return out
		}},
	}
	for _, operation := range operations {
		for _, failFast := range []bool{false, true} {
			for _, point := range []string{"before", "after_first", "after_first_extra_reads", "during_first", "uncancelled"} {
				t.Run(fmt.Sprintf("%s/fail_fast_%t/%s", operation.name, failFast, point), func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var eng *DefaultEngine
					var reads atomic.Int64
					if point == "during_first" {
						conn, probe := newCallerContextProbeDB(t)
						var err error
						eng, err = New(Config{DB: conn, ContainerDir: t.TempDir()})
						if err != nil {
							t.Fatal(err)
						}
						probe.onQuery = func(context.Context) { reads.Add(1); cancel() }
					} else {
						conn := newEngineTestDB(t)
						for i, name := range []string{"first.txt", "second.txt"} {
							_, err := conn.Exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status, chunker_version) VALUES ($1, $2, 0, $3, 1, 'COMPLETED', 'v1-simple-rolling')`, i+1, name, fmt.Sprintf("%064d", i+1))
							if err != nil {
								t.Fatal(err)
							}
							_, err = conn.Exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete) VALUES ($1, $2, 0, CURRENT_TIMESTAMP, 0, 0, TRUE)`, name, i+1)
							if err != nil {
								t.Fatal(err)
							}
						}
						var err error
						eng, err = New(Config{DB: conn, ContainerDir: t.TempDir()})
						if err != nil {
							t.Fatal(err)
						}
						raw, err := conn.Conn(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						err = raw.Raw(func(value any) error {
							value.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(action int, _, _, _ string) int {
								if action == sqlite3.SQLITE_READ {
									reads.Add(1)
								}
								return sqlite3.SQLITE_OK
							})
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
						if err = raw.Close(); err != nil {
							t.Fatal(err)
						}
					}
					completed := 0
					var firstReads int64
					eng.batchItemCompleted = func() {
						completed++
						if completed == 1 {
							firstReads = reads.Load()
						}
						if point == "after_first_extra_reads" {
							// Harmless context observation is deliberately independent of cancellation.
							if err := ctx.Err(); err != nil {
								t.Fatalf("first item unexpectedly cancelled: %v", err)
							}
							_ = ctx.Done()
							_ = ctx.Err()
						}
						if point == "after_first" || point == "after_first_extra_reads" {
							cancel()
						}
					}
					if point == "before" {
						cancel()
					}
					out := operation.run(ctx, eng, failFast, t.TempDir())
					if point == "uncancelled" {
						if out.err != nil || completed != 2 || len(out.statuses) != 2 || out.summary.OK != 2 || out.summary.Failed != 0 || out.summary.Skipped != 0 {
							t.Fatalf("uncancelled outcome=%+v completed=%d", out, completed)
						}
						return
					}
					if !errors.Is(out.err, context.Canceled) || !IsCode(out.err, ErrorCancelled) {
						t.Fatalf("error=%v, want cancelled cause and code", out.err)
					}
					if point == "before" {
						if completed != 0 || len(out.statuses) != 0 || reads.Load() != 0 {
							t.Fatalf("work before dispatch: outcome=%+v completed=%d reads=%d", out, completed, reads.Load())
						}
						return
					}
					if completed != 1 || len(out.statuses) != 1 || reads.Load() != firstReads || firstReads == 0 {
						t.Fatalf("second target reached or first work missing: outcome=%+v completed=%d first_reads=%d final_reads=%d", out, completed, firstReads, reads.Load())
					}
					if point == "during_first" {
						if out.summary.Failed != 1 || out.summary.OK != 0 || out.statuses[0] != BatchItemFailed {
							t.Fatalf("first failure lost: %+v", out)
						}
					} else if out.summary.OK != 1 || out.summary.Failed != 0 || (out.statuses[0] != BatchItemOK && out.statuses[0] != BatchItemPlanned) {
						t.Fatalf("successful partial result lost: %+v", out)
					}
					if operation.name != "RemoveStoredPaths" && out.summary.Skipped != 1 {
						t.Fatalf("undispatched item lost: %+v", out)
					}
				})
			}
		}
	}
}

func TestEngineStoreCancellationJoinsFinalizationFailure(t *testing.T) {
	dbconn := newEngineTestDB(t)
	finalizeErr := errors.New("forced finalization failure")
	writer := &cancellationFinalizeWriter{SimulatedWriter: container.NewSimulatedWriter(1 << 20), err: finalizeErr}
	sgctx := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: t.TempDir()}
	ctx, cancel := context.WithCancel(context.Background())
	storage.InstallTestStoreInterleavingHooks(&sgctx, func(hookCtx context.Context, _ storage.TestStoreInterleavingHookEvent) error {
		cancel()
		return hookCtx.Err()
	})
	eng, err := New(Config{DB: dbconn, ContainerDir: sgctx.ContainerDir, StoreContext: &sgctx})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	input := filepath.Join(t.TempDir(), "cancel-store.txt")
	if err := os.WriteFile(input, []byte("cancel me"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	result, err := eng.Store(ctx, StoreRequest{SourcePath: input, Codec: "plain"})
	if result != (StoreResult{}) {
		t.Fatalf("Store result = %+v, want zero", result)
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, finalizeErr) || !IsCode(err, ErrorCancelled) {
		t.Fatalf("Store error = %v, want joined cancellation and finalization failure", err)
	}
	if writer.calls != 1 {
		t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
	}
}

func TestEngineVerifyCancellationAndDeadlineRemainCancelled(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			dbconn := newEngineTestDB(t)
			eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			var ctx context.Context
			var cancel context.CancelFunc
			if errors.Is(cause, context.DeadlineExceeded) {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			}
			defer cancel()
			result, err := eng.Verify(ctx, VerifyRequest{Target: "system", Level: "standard"})
			if result != (VerifyResult{}) || !errors.Is(err, cause) || !IsCode(err, ErrorCancelled) {
				t.Fatalf("Verify result=%+v error=%v, want zero result and %v", result, err, cause)
			}
		})
	}
}

func TestDoctorChecksCancellationBetweenEveryStage(t *testing.T) {
	for _, cancelAfter := range []string{"recovery", "schema", "verify"} {
		t.Run(cancelAfter, func(t *testing.T) {
			eng := newDoctorHookEngine(t)
			ctx, cancel := context.WithCancel(context.Background())
			var calls []string
			eng.doctorRecover = func(context.Context) (RecoverResult, error) {
				calls = append(calls, "recovery")
				if cancelAfter == "recovery" {
					cancel()
				}
				return RecoverResult{}, nil
			}
			eng.doctorSchema = func(context.Context, *sql.DB) (int64, error) {
				calls = append(calls, "schema")
				if cancelAfter == "schema" {
					cancel()
				}
				return 16, nil
			}
			eng.doctorVerify = func(context.Context, string) error {
				calls = append(calls, "verify")
				if cancelAfter == "verify" {
					cancel()
				}
				return nil
			}
			eng.doctorAudit = func(context.Context, *sql.DB) (DoctorPhysicalAudit, DoctorSnapshotAudit, error) {
				calls = append(calls, "audit")
				return DoctorPhysicalAudit{}, DoctorSnapshotAudit{}, nil
			}
			_, err := eng.Doctor(ctx, DoctorRequest{})
			if !errors.Is(err, context.Canceled) || !IsCode(err, ErrorCancelled) {
				t.Fatalf("Doctor error = %v, want cancellation", err)
			}
			wantCalls := map[string]int{"recovery": 1, "schema": 2, "verify": 3}[cancelAfter]
			if len(calls) != wantCalls {
				t.Fatalf("stage calls = %v, want cancellation after %s", calls, cancelAfter)
			}
		})
	}
}
