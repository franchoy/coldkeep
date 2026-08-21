package catalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	aggregateFaultRegister sync.Once
	errAggregateFaultCause = errors.New("aggregate catalog fault")
)

func TestTranslateServiceErrorContract(t *testing.T) {
	typed := NewError(ErrorConflict, "existing", "", "existing typed error", errAggregateFaultCause)
	if got := translateServiceError("ignored", "ignored", typed); got != typed {
		t.Fatalf("typed error was not returned unchanged: got %p want %p", got, typed)
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		err := translateServiceError("aggregate", "aggregate cancelled", fmt.Errorf("wrapped: %w", cause))
		if !IsCode(err, ErrorCancelled) || !errors.Is(err, cause) {
			t.Fatalf("translate cancellation %v: got %v", cause, err)
		}
	}
	err := translateServiceError("aggregate", "aggregate failed", errAggregateFaultCause)
	if !IsCode(err, ErrorOperationFailed) || !errors.Is(err, errAggregateFaultCause) {
		t.Fatalf("translate operation failure: got %v", err)
	}
}

func TestAggregateServiceQueryFailuresAreTypedAndPreserveCause(t *testing.T) {
	svc, closeDB := newAggregateFaultService(t, "query")
	defer closeDB()
	assertAggregateFailures(t, svc, true)
}

func TestAggregateServiceScanFailuresAreTyped(t *testing.T) {
	svc, closeDB := newAggregateFaultService(t, "scan")
	defer closeDB()
	assertAggregateFailures(t, svc, false)
}

func TestAggregateServiceIterationFailuresAreTypedAndPreserveCause(t *testing.T) {
	svc, closeDB := newAggregateFaultService(t, "iterate")
	defer closeDB()
	ctx := context.Background()
	assertOperationFailure(t, func() error {
		_, err := svc.FindPhysicalFilesForLogicalFile(ctx, 1)
		return err
	}(), true)
	assertOperationFailure(t, func() error {
		_, err := svc.ListSnapshots(ctx, SnapshotFilter{})
		return err
	}(), true)
	assertOperationFailure(t, func() error {
		_, err := svc.LoadReachabilityRoots(ctx)
		return err
	}(), true)
}

func TestAggregateServiceCancellationAndDeadlineFailuresPreserveIdentity(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		ctx   func() context.Context
	}{
		{
			name:  "cancelled",
			cause: context.Canceled,
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
		{
			name:  "deadline",
			cause: context.DeadlineExceeded,
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
				cancel()
				return ctx
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, closeDB := newAggregateFaultService(t, "context")
			defer closeDB()
			ctx := test.ctx()
			checks := []func() error{
				func() error { _, err := svc.FindLogicalFile(ctx, 1); return err },
				func() error { _, err := svc.FindPhysicalFilesForLogicalFile(ctx, 1); return err },
				func() error { _, err := svc.FindSnapshot(ctx, "snap"); return err },
				func() error { _, err := svc.ListSnapshots(ctx, SnapshotFilter{}); return err },
				func() error { _, err := svc.LoadReachabilityRoots(ctx); return err },
			}
			for i, check := range checks {
				t.Run(fmt.Sprintf("method-%d", i+1), func(t *testing.T) {
					err := check()
					if !IsCode(err, ErrorCancelled) || !errors.Is(err, test.cause) {
						t.Fatalf("got %v, want typed cancellation preserving %v", err, test.cause)
					}
				})
			}
		})
	}
}

func assertAggregateFailures(t *testing.T, svc *Service, requireSentinel bool) {
	t.Helper()
	ctx := context.Background()
	checks := []func() error{
		func() error { _, err := svc.FindLogicalFile(ctx, 1); return err },
		func() error { _, err := svc.FindPhysicalFilesForLogicalFile(ctx, 1); return err },
		func() error { _, err := svc.FindSnapshot(ctx, "snap"); return err },
		func() error { _, err := svc.ListSnapshots(ctx, SnapshotFilter{}); return err },
		func() error { _, err := svc.LoadReachabilityRoots(ctx); return err },
	}
	for i, check := range checks {
		t.Run(fmt.Sprintf("method-%d", i+1), func(t *testing.T) {
			assertOperationFailure(t, check(), requireSentinel)
		})
	}
}

func assertOperationFailure(t *testing.T, err error, requireSentinel bool) {
	t.Helper()
	if !IsCode(err, ErrorOperationFailed) {
		t.Fatalf("got %v, want operation_failed", err)
	}
	if requireSentinel && !errors.Is(err, errAggregateFaultCause) {
		t.Fatalf("got %v, want preserved injected cause", err)
	}
}

func newAggregateFaultService(t *testing.T, mode string) (*Service, func()) {
	t.Helper()
	aggregateFaultRegister.Do(func() {
		sql.Register("coldkeep-catalog-aggregate-fault", aggregateFaultDriver{})
	})
	db, err := sql.Open("coldkeep-catalog-aggregate-fault", mode)
	if err != nil {
		t.Fatalf("open aggregate fault database: %v", err)
	}
	db.SetMaxOpenConns(1)
	return NewServiceFromSQL(db), func() { _ = db.Close() }
}

type aggregateFaultDriver struct{}

func (aggregateFaultDriver) Open(name string) (driver.Conn, error) {
	return &aggregateFaultConn{mode: name}, nil
}

type aggregateFaultConn struct{ mode string }

func (*aggregateFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (*aggregateFaultConn) Close() error              { return nil }
func (*aggregateFaultConn) Begin() (driver.Tx, error) { return nil, errors.New("begin unsupported") }

func (c *aggregateFaultConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.mode == "query" {
		return nil, errAggregateFaultCause
	}
	columns, values := aggregateFaultRow(query)
	if c.mode == "scan" {
		values[0] = nil
	}
	return &aggregateFaultRows{columns: columns, values: values, mode: c.mode}, nil
}

type aggregateFaultRows struct {
	columns []string
	values  []driver.Value
	mode    string
	read    int
}

func (r *aggregateFaultRows) Columns() []string { return r.columns }
func (*aggregateFaultRows) Close() error        { return nil }
func (r *aggregateFaultRows) Next(dest []driver.Value) error {
	if r.read == 0 {
		copy(dest, r.values)
		r.read++
		return nil
	}
	if r.mode == "iterate" && r.read == 1 {
		r.read++
		return errAggregateFaultCause
	}
	return io.EOF
}

func aggregateFaultRow(query string) ([]string, []driver.Value) {
	switch {
	case strings.Contains(query, "SELECT DISTINCT logical_file_id"):
		return []string{"logical_file_id"}, []driver.Value{int64(1)}
	case strings.Contains(query, "FROM physical_file"):
		return []string{"path", "logical_file_id", "mode", "mtime", "is_metadata_complete"}, []driver.Value{"path", int64(1), int64(0), time.Now().UTC(), true}
	case strings.Contains(query, "FROM logical_file"):
		return []string{"id", "original_name", "total_size", "file_hash", "ref_count", "status"}, []driver.Value{int64(1), "file", int64(1), "hash", int64(1), "complete"}
	case strings.Contains(query, "FROM snapshot_file"):
		return []string{"logical_file_id"}, []driver.Value{int64(1)}
	case strings.Contains(query, "FROM snapshot"):
		return []string{"id", "type", "label", "parent_id", "created_at"}, []driver.Value{"snap", "full", "label", "", time.Now().UTC().Format(time.RFC3339Nano)}
	default:
		return []string{"unknown"}, []driver.Value{int64(1)}
	}
}

var _ driver.QueryerContext = (*aggregateFaultConn)(nil)
