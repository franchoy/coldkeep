package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/storage"
)

type callerContextProbeKey struct{}

var errCallerContextProbeStop = errors.New("caller context probe stop")

type callerContextProbe struct {
	observed chan bool
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
	return nil, errCallerContextProbeStop
}
func (c *callerContextProbeConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.probe.record(ctx)
	return nil, errCallerContextProbeStop
}
func (c *callerContextProbeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

type steppedCancellationContext struct {
	base      context.Context
	remaining int
}

func (c *steppedCancellationContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *steppedCancellationContext) Done() <-chan struct{}       { return nil }
func (c *steppedCancellationContext) Value(key any) any           { return c.base.Value(key) }
func (c *steppedCancellationContext) Err() error {
	if c.remaining == 0 {
		return context.Canceled
	}
	c.remaining--
	return nil
}

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

func TestEngineBatchCancellationStopsNewDispatchWithPartialResult(t *testing.T) {
	newSeededEngine := func(t *testing.T) *DefaultEngine {
		t.Helper()
		dbconn := newEngineTestDB(t)
		if _, err := dbconn.Exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status, chunker_version) VALUES (1, 'first.txt', 0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 1, 'COMPLETED', 'v1-simple-rolling')`); err != nil {
			t.Fatalf("seed logical file: %v", err)
		}
		if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete) VALUES ('first.txt', 1, 0, CURRENT_TIMESTAMP, 0, 0, TRUE)`); err != nil {
			t.Fatalf("seed physical file: %v", err)
		}
		eng, err := New(Config{DB: dbconn, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return eng
	}

	for _, failFast := range []bool{false, true} {
		t.Run("Remove", func(t *testing.T) {
			eng := newSeededEngine(t)
			ctx := &steppedCancellationContext{base: context.Background(), remaining: 2}
			result, err := eng.Remove(ctx, RemoveRequest{FileIDs: []int64{1, 2}, DryRun: true, FailFast: failFast})
			if !errors.Is(err, context.Canceled) || !IsCode(err, ErrorCancelled) {
				t.Fatalf("Remove error = %v, want cancellation", err)
			}
			if len(result.Items) != 1 || result.Summary.OK != 1 || result.Summary.Skipped != 1 {
				t.Fatalf("Remove partial result = %+v", result)
			}
		})

		t.Run("Restore", func(t *testing.T) {
			eng := newSeededEngine(t)
			ctx := &steppedCancellationContext{base: context.Background(), remaining: 2}
			result, err := eng.Restore(ctx, RestoreRequest{FileIDs: []int64{1, 2}, DestinationRoot: t.TempDir(), DryRun: true, FailFast: failFast})
			if !errors.Is(err, context.Canceled) || !IsCode(err, ErrorCancelled) {
				t.Fatalf("Restore error = %v, want cancellation", err)
			}
			if len(result.Items) != 1 || result.Summary.OK != 1 || result.Summary.Skipped != 1 {
				t.Fatalf("Restore partial result = %+v", result)
			}
		})

		t.Run("RemoveStoredPaths", func(t *testing.T) {
			eng := newSeededEngine(t)
			ctx := &steppedCancellationContext{base: context.Background(), remaining: 2}
			result, err := eng.RemoveStoredPaths(ctx, RemoveStoredPathsRequest{StoredPaths: []string{"first.txt", "second.txt"}, DryRun: true, FailFast: failFast})
			if !errors.Is(err, context.Canceled) || !IsCode(err, ErrorCancelled) {
				t.Fatalf("RemoveStoredPaths error = %v, want cancellation", err)
			}
			if len(result.Items) != 1 || result.Summary.OK != 1 {
				t.Fatalf("RemoveStoredPaths partial result = %+v", result)
			}
		})
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
