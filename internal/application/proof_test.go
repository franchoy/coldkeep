package application

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"github.com/mattn/go-sqlite3"
)

type proofEngineProbe struct {
	verify  func(context.Context, engine.VerifyRequest) (engine.VerifyResult, error)
	show    func(context.Context, engine.SnapshotShowRequest) (engine.SnapshotShowResult, error)
	restore func(context.Context, engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error)
}

func (p proofEngineProbe) Verify(ctx context.Context, req engine.VerifyRequest) (engine.VerifyResult, error) {
	return p.verify(ctx, req)
}
func (p proofEngineProbe) SnapshotShow(ctx context.Context, req engine.SnapshotShowRequest) (engine.SnapshotShowResult, error) {
	return p.show(ctx, req)
}
func (p proofEngineProbe) SnapshotRestore(ctx context.Context, req engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error) {
	return p.restore(ctx, req)
}

func TestProofOperationsExactRequestsAndResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var size int64 = 17
	label := " label\x00\u03bb "
	path := " history/Case\u03bb.txt "
	id := "history-id"
	destination := t.TempDir()
	p := &ProofOperations{engine: proofEngineProbe{
		verify: func(got context.Context, req engine.VerifyRequest) (engine.VerifyResult, error) {
			if got != ctx || !reflect.DeepEqual(req, engine.VerifyRequest{Level: "deep", Target: "system"}) {
				t.Fatalf("verification context/request changed: %+v", req)
			}
			return engine.VerifyResult{}, nil
		},
		show: func(got context.Context, req engine.SnapshotShowRequest) (engine.SnapshotShowResult, error) {
			if got != ctx || !reflect.DeepEqual(req, engine.SnapshotShowRequest{SnapshotID: id, Query: engine.SnapshotQuery{Limit: 257}}) {
				t.Fatalf("snapshot context/selection/bound changed: %+v", req)
			}
			return engine.SnapshotShowResult{Snapshot: engine.SnapshotMeta{Label: label}, Files: []engine.SnapshotFile{{StoredPath: path, Size: &size}, {StoredPath: "unknown-size"}}}, nil
		},
		restore: func(got context.Context, req engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error) {
			want := engine.SnapshotRestoreRequest{SnapshotID: id, Paths: []string{path}, Destination: engine.SnapshotRestoreDestination{Mode: engine.SnapshotRestoreDestinationPrefix, Path: destination}, Metadata: engine.SnapshotRestoreMetadataStrict}
			if got != ctx || !reflect.DeepEqual(req, want) {
				t.Fatalf("restore context/exact path/strict metadata/destination changed: %+v", req)
			}
			return engine.SnapshotRestoreResult{RestoredFiles: 1, Warnings: []engine.SnapshotRestoreWarning{{Code: engine.SnapshotRestoreWarningMetadata}}}, nil
		},
	}}
	if err := p.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	history, err := p.Snapshot(ctx, ProofSnapshotRequest{SnapshotID: id, Limit: 257})
	if err != nil || history.Label != label || len(history.Files) != 2 || history.Files[0].StoredPath != path || history.Files[0].Size == nil || *history.Files[0].Size != size || history.Files[1].Size != nil {
		t.Fatalf("history projection lost exact or nullable fields: %+v, %v", history, err)
	}
	restored, err := p.Restore(ctx, ProofRestoreRequest{SnapshotID: id, StoredPath: path, DestinationRoot: destination})
	if err != nil || restored.RestoredFiles != 1 || restored.WarningCount != 1 {
		t.Fatalf("restore projection lost partial/warning state: %+v, %v", restored, err)
	}
	for _, limit := range []int{0, -1} {
		if _, err := p.Snapshot(ctx, ProofSnapshotRequest{SnapshotID: id, Limit: limit}); err == nil {
			t.Fatal("unbounded history admitted")
		}
	}
}

func TestProofOperationsForwardCancellationAndErrors(t *testing.T) {
	for _, failure := range []error{context.Canceled, errors.New("engine failure")} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		check := func(got context.Context) {
			if got != ctx || !errors.Is(got.Err(), context.Canceled) {
				t.Fatal("caller cancellation replaced")
			}
		}
		p := &ProofOperations{engine: proofEngineProbe{
			verify: func(got context.Context, _ engine.VerifyRequest) (engine.VerifyResult, error) {
				check(got)
				return engine.VerifyResult{}, failure
			},
			show: func(got context.Context, _ engine.SnapshotShowRequest) (engine.SnapshotShowResult, error) {
				check(got)
				return engine.SnapshotShowResult{}, failure
			},
			restore: func(got context.Context, _ engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error) {
				check(got)
				return engine.SnapshotRestoreResult{RestoredFiles: 1, Warnings: []engine.SnapshotRestoreWarning{{}}}, failure
			},
		}}
		if err := p.Verify(ctx); !errors.Is(err, failure) {
			t.Fatalf("verify error changed: %v", err)
		}
		if _, err := p.Snapshot(ctx, ProofSnapshotRequest{SnapshotID: "history", Limit: 1}); !errors.Is(err, failure) {
			t.Fatalf("snapshot error changed: %v", err)
		}
		result, err := p.Restore(ctx, ProofRestoreRequest{SnapshotID: "history", StoredPath: "file", DestinationRoot: t.TempDir()})
		if !errors.Is(err, failure) || result.RestoredFiles != 1 || result.WarningCount != 1 {
			t.Fatalf("partial restore/error changed: %+v %v", result, err)
		}
	}
}

type proofBorrowedConnector struct {
	driver *sqlite3.SQLiteDriver
	opens  atomic.Int64
	closes atomic.Int64
}

func (c *proofBorrowedConnector) Driver() driver.Driver { return c.driver }
func (c *proofBorrowedConnector) Connect(context.Context) (driver.Conn, error) {
	raw, err := c.driver.Open(":memory:")
	if err != nil {
		return nil, err
	}
	c.opens.Add(1)
	return &proofBorrowedConnection{Conn: raw, owner: c}, nil
}

type proofBorrowedConnection struct {
	driver.Conn
	owner *proofBorrowedConnector
}

func (c *proofBorrowedConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *proofBorrowedConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *proofBorrowedConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *proofBorrowedConnection) Close() error {
	c.owner.closes.Add(1)
	return c.Conn.Close()
}

func TestProofOperationsBorrowExactHandlesUntilOwnerCloses(t *testing.T) {
	// Default discovery must fail, while the explicitly supplied target works.
	t.Setenv("DB_PORT", "invalid-default-port")
	t.Setenv("COLDKEEP_STORAGE_CONTEXT", "invalid-default-storage")
	connector := &proofBorrowedConnector{driver: &sqlite3.SQLiteDriver{}}
	conn := sql.OpenDB(connector)
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = conn.Close() })
	if err := db.RunMigrations(conn); err != nil {
		t.Fatal(err)
	}
	store := &storage.StorageContext{DB: conn, ContainerDir: t.TempDir()}
	operations, err := NewProofOperations(conn, store, store.ContainerDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := operations.Verify(context.Background()); err != nil {
		t.Fatalf("explicit empty SQLite fixture: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Fatalf("borrowed handle closed: %v", err)
	}
	if connector.opens.Load() != 1 || connector.closes.Load() != 0 || conn.Stats().OpenConnections != 1 {
		t.Fatalf("borrowed ownership changed: opens=%d closes=%d stats=%+v", connector.opens.Load(), connector.closes.Load(), conn.Stats())
	}
	for _, refused := range []struct {
		name      string
		store     *storage.StorageContext
		directory string
	}{
		{name: "missing database", store: &storage.StorageContext{ContainerDir: store.ContainerDir}, directory: store.ContainerDir},
		{name: "missing storage", directory: store.ContainerDir},
		{name: "empty directory", store: &storage.StorageContext{DB: conn}},
		{name: "whitespace default fallback", store: &storage.StorageContext{DB: conn, ContainerDir: " \t "}, directory: " \t "},
		{name: "different directory", store: store, directory: t.TempDir()},
		{name: "directory bytes differ", store: store, directory: store.ContainerDir + " "},
	} {
		t.Run(refused.name, func(t *testing.T) {
			if _, err := NewProofOperations(conn, refused.store, refused.directory); err == nil {
				t.Fatal("inexplicit or mismatched borrowed target admitted")
			}
			if err := conn.Ping(); err != nil {
				t.Fatalf("constructor refusal closed caller handle: %v", err)
			}
			if connector.opens.Load() != 1 || connector.closes.Load() != 0 {
				t.Fatalf("constructor refusal changed ownership: opens=%d closes=%d", connector.opens.Load(), connector.closes.Load())
			}
		})
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if connector.closes.Load() != 1 {
		t.Fatalf("physical owner close count=%d", connector.closes.Load())
	}
	if err := operations.Verify(context.Background()); err == nil {
		t.Fatal("operation reopened a target after owner close")
	}
	if connector.opens.Load() != 1 {
		t.Fatal("operation opened an additional connection after owner close")
	}
}
