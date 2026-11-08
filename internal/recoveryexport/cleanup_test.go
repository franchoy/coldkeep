package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

type cleanupSignal struct {
	ch   chan struct{}
	once sync.Once
}

func newCleanupSignal() *cleanupSignal { return &cleanupSignal{ch: make(chan struct{})} }
func (s *cleanupSignal) fire()         { s.once.Do(func() { close(s.ch) }) }
func awaitCleanup(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup barrier timed out")
	}
}

type cleanupDriver struct {
	rollbackStarted, allowRollback, rollbackFinished *cleanupSignal
	closeStarted, allowClose, closeFinished          *cleanupSignal
	rollbackErr, closeErr                            error
}

func newCleanupDriver(t *testing.T, rollbackErr, closeErr error) *cleanupDriver {
	t.Helper()
	d := &cleanupDriver{newCleanupSignal(), newCleanupSignal(), newCleanupSignal(), newCleanupSignal(), newCleanupSignal(), newCleanupSignal(), rollbackErr, closeErr}
	t.Cleanup(func() { d.allowRollback.fire(); d.allowClose.fire() })
	return d
}

type cleanupConnector struct{ d *cleanupDriver }

func (c cleanupConnector) Connect(context.Context) (driver.Conn, error) { return c.d, nil }
func (c cleanupConnector) Driver() driver.Driver                        { return c.d }

func (d *cleanupDriver) Driver() driver.Driver            { return d }
func (d *cleanupDriver) Open(string) (driver.Conn, error) { return d, nil }
func (d *cleanupDriver) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (d *cleanupDriver) Begin() (driver.Tx, error)                                    { return d, nil }
func (d *cleanupDriver) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) { return d, nil }
func (d *cleanupDriver) ResetSession(context.Context) error                           { return nil }
func (d *cleanupDriver) IsValid() bool                                                { return true }
func (d *cleanupDriver) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("unexpected Query")
}
func (d *cleanupDriver) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("unexpected Exec")
}
func (d *cleanupDriver) Commit() error { return errors.New("unexpected Commit") }
func (d *cleanupDriver) Rollback() error {
	d.rollbackStarted.fire()
	<-d.allowRollback.ch
	d.rollbackFinished.fire()
	return d.rollbackErr
}
func (d *cleanupDriver) Close() error {
	d.closeStarted.fire()
	<-d.allowClose.ch
	d.closeFinished.fire()
	return d.closeErr
}

func assertCleanupExclusion(t *testing.T, id coordination.Identity) {
	t.Helper()
	l, e := coordination.AcquireExisting(context.Background(), id)
	if l != nil {
		_ = l.Release()
	}
	if !errors.Is(e, coordination.ErrNestedRepositoryAcquisition) {
		t.Fatalf("passive ownership escaped cleanup: %v", e)
	}
	owner, e := coordination.NewOwner(coordination.OperationInspect, id, "test", time.Now())
	must(t, e)
	legacy, e := coordination.NewCoordinator().Acquire(context.Background(), id, coordination.Request{Operation: coordination.OperationInspect, Mode: coordination.ModeExclusive, Owner: owner})
	if legacy != nil {
		_ = legacy.Release()
	}
	if !errors.Is(e, coordination.ErrNestedRepositoryAcquisition) {
		t.Fatalf("legacy ownership escaped cleanup: %v", e)
	}
}

func TestCleanupJoinsAutomaticRollbackAndDriverClose(t *testing.T) {
	for _, bad := range []bool{false, true} {
		name := "rollback-in-flight"
		var rollbackErr error
		if bad {
			name = "bad-connection-close-in-flight"
			rollbackErr = driver.ErrBadConn
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			d := newCleanupDriver(t, rollbackErr, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			draining := make(chan struct{})
			returned := make(chan struct{})
			var result Result
			var opErr error
			h := &hooks{connector: cleanupConnector{d}, event: func(stage string, _ *sql.Tx) error {
				switch stage {
				case "snapshot-opened":
					cancel()
					<-d.rollbackStarted.ch
					if bad {
						d.allowRollback.fire()
						<-d.closeStarted.ch
					}
					return ctx.Err()
				case "database-draining":
					close(draining)
				case "database-closed":
					select {
					case <-d.closeFinished.ch:
					default:
						return errors.New("database-closed before actual driver close")
					}
				}
				return nil
			}}
			go func() { result, opErr = inspect(ctx, f.config, DefaultLimits(), noConsume, h); close(returned) }()
			// Always unblock and join the operation before fixture teardown, even on a
			// regression failure. No sleep determines cleanup completion.
			defer func() { d.allowRollback.fire(); d.allowClose.fire(); awaitCleanup(t, returned) }()
			select {
			case <-draining:
			case <-returned:
				t.Fatalf("inspection returned with cleanup blocked: result=%+v error=%v", result, opErr)
			case <-time.After(5 * time.Second):
				t.Fatal("inspection did not reach cleanup")
			}
			id, e := coordination.ResolveIdentity(f.root)
			must(t, e)
			assertCleanupExclusion(t, id)
			select {
			case <-returned:
				t.Fatal("returned during held cleanup")
			default:
			}
			if !bad {
				d.allowRollback.fire()
				awaitCleanup(t, d.closeStarted.ch)
				assertCleanupExclusion(t, id)
				select {
				case <-returned:
					t.Fatal("returned during held driver close")
				default:
				}
			}
			d.allowClose.fire()
			awaitCleanup(t, returned)
			awaitCleanup(t, d.rollbackFinished.ch)
			awaitCleanup(t, d.closeFinished.ch)
			if result.Complete || !errors.Is(opErr, context.Canceled) {
				t.Fatalf("cancelled cleanup: %+v %v", result, opErr)
			}
			if bad && !errors.Is(opErr, driver.ErrBadConn) {
				t.Fatalf("automatic rollback error lost: %v", opErr)
			}
			lease, e := coordination.AcquireExisting(context.Background(), id)
			must(t, e)
			must(t, lease.Release())
			t.Log("SCENARIO cleanup-order: held rollback/close => no return, passive+legacy excluded; actual close => incomplete return, lease available")
		})
	}
}

func TestCleanupPreservesDriverErrors(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		name := "explicit-rollback"
		if automatic {
			name = "automatic-rollback"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			rollbackErr, closeErr := errors.New("rollback failure"), errors.New("driver close failure")
			d := newCleanupDriver(t, rollbackErr, closeErr)
			d.allowRollback.fire()
			d.allowClose.fire()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := &hooks{connector: cleanupConnector{d}, event: func(stage string, _ *sql.Tx) error {
				if stage == "snapshot-opened" {
					if automatic {
						cancel()
						<-d.rollbackFinished.ch
					}
					return errors.New("injected query failure")
				}
				return nil
			}}
			result, e := inspect(ctx, f.config, DefaultLimits(), noConsume, h)
			if result.Complete || !errors.Is(e, rollbackErr) || !errors.Is(e, closeErr) {
				t.Fatalf("cleanup failures lost: %+v %v", result, e)
			}
			awaitCleanup(t, d.closeFinished.ch)
		})
	}
}
