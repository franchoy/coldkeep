//go:build linux

package recoveryexport

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

type gatedCatalogConnector struct {
	driver.Connector
	gates *cleanupDriver
}

func (c gatedCatalogConnector) Connect(ctx context.Context) (driver.Conn, error) {
	raw, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	conn, ok := raw.(catalogDriverConn)
	if !ok {
		return nil, errors.Join(ErrUnsupported, raw.Close())
	}
	return &gatedCatalogConn{conn, c.gates}, nil
}

type gatedCatalogConn struct {
	catalogDriverConn
	gates *cleanupDriver
}

func (c *gatedCatalogConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, e := c.catalogDriverConn.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return &gatedCatalogTx{tx, c.gates}, nil
}
func (c *gatedCatalogConn) Close() error {
	c.gates.closeStarted.fire()
	<-c.gates.allowClose.ch
	e := c.catalogDriverConn.Close()
	c.gates.closeFinished.fire()
	return errors.Join(e, c.gates.closeErr)
}

type gatedCatalogTx struct {
	driver.Tx
	gates *cleanupDriver
}

func (tx *gatedCatalogTx) Rollback() error {
	tx.gates.rollbackStarted.fire()
	<-tx.gates.allowRollback.ch
	e := tx.Tx.Rollback()
	tx.gates.rollbackFinished.fire()
	return errors.Join(e, tx.gates.rollbackErr)
}
func noMarker(t *testing.T, path string) {
	t.Helper()
	if _, e := os.Lstat(filepath.Join(path, "COMPLETE")); !os.IsNotExist(e) {
		t.Fatalf("premature marker: %v", e)
	}
}
func TestRecoveryCleanupBeforePublication(t *testing.T) {
	for _, kind := range []string{"blocked-success", "driver-close-error", "automatic-rollback-error"} {
		t.Run(kind, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("actual cleanup failure")
			gates := newCleanupDriver(t, nil, nil)
			if kind == "driver-close-error" {
				gates.closeErr = injected
			}
			if kind == "automatic-rollback-error" {
				gates.rollbackErr = driver.ErrBadConn
			}
			h := &exportHooks{cut: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}, event: func(stage string) error {
				if stage == "staged-verified" && kind == "automatic-rollback-error" {
					cancel()
					<-gates.rollbackStarted.ch
					return ctx.Err()
				}
				if stage == "source-cleaned" {
					id, e := coordination.ResolveIdentity(f.root)
					must(t, e)
					l, e := coordination.AcquireExisting(context.Background(), id)
					must(t, e)
					must(t, l.Release())
					noMarker(t, o.Destination)
				}
				return nil
			}}
			done := make(chan struct{})
			var result ExportResult
			var opErr error
			go func() { result, opErr = export(ctx, f.config, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			id, e := coordination.ResolveIdentity(f.root)
			must(t, e)
			assertCleanupExclusion(t, id)
			noMarker(t, o.Destination)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			assertCleanupExclusion(t, id)
			noMarker(t, o.Destination)
			select {
			case <-done:
				t.Fatal("returned before actual close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			assertNoExportSessions(t, f)
			if kind == "blocked-success" {
				must(t, opErr)
				if !result.Complete {
					t.Fatal(result)
				}
			} else {
				if opErr == nil || result.Published {
					t.Fatalf("cleanup failure completed: %+v %v", result, opErr)
				}
				noMarker(t, o.Destination)
			}
			t.Logf("SCENARIO cleanup=%s rollback and driver close held => lease excluded, no marker; actual close => published=%v", kind, result.Published)
		})
	}
}
func fakeDump(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pg_dump")
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then exec " + os.Getenv("COLDKEEP_PG_DUMP") + " --version; fi\n" + body + "\n"
	must(t, os.WriteFile(path, []byte(script), 0700))
	return path
}
func TestRecoveryChildCancellationJoins(t *testing.T) {
	f := exportFixture(t)
	o := exportOptions(t)
	o.Tools.PGDump = fakeDump(t, "exec /bin/sleep 60")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, joined, allow := make(chan struct{}), make(chan struct{}), newCleanupSignal()
	var pid int
	h := &exportHooks{childPID: func(p int) { pid = p }, event: func(stage string) error {
		switch stage {
		case "child-started":
			close(started)
		case "child-joined":
			close(joined)
			<-allow.ch
		}
		return nil
	}}
	done := make(chan struct{})
	var result ExportResult
	var opErr error
	go func() { result, opErr = export(ctx, f.config, o, h); close(done) }()
	defer func() { cancel(); allow.fire(); awaitCleanup(t, done) }()
	awaitCleanup(t, started)
	if e := syscall.Kill(pid, 0); e != nil {
		t.Fatal("child was not running", e)
	}
	cancel()
	awaitCleanup(t, joined)
	if e := syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
		t.Fatalf("child survived Wait: %v", e)
	}
	id, e := coordination.ResolveIdentity(f.root)
	must(t, e)
	assertCleanupExclusion(t, id)
	noMarker(t, o.Destination)
	allow.fire()
	awaitCleanup(t, done)
	if result.Published || opErr == nil {
		t.Fatal(result, opErr)
	}
	assertNoExportSessions(t, f)
	t.Log("SCENARIO live dump canceled and actual child reaped; lease retained through joined-worker barrier; no marker")
}
func TestRecoveryExplicitPreparation(t *testing.T) {
	f := exportFixture(t)
	must(t, os.RemoveAll(filepath.Join(f.root, ".coldkeep-control")))
	o := exportOptions(t)
	result, e := Export(context.Background(), f.config, o)
	if !errors.Is(e, coordination.ErrPreparationRequired) || result.Published {
		t.Fatalf("export prepared implicitly: %+v %v", result, e)
	}
	if _, e = os.Stat(filepath.Join(f.root, ".coldkeep-control")); !os.IsNotExist(e) {
		t.Fatal("passive export created control")
	}
	before := metadataSemantics(t, f.db)
	must(t, coordination.PrepareExisting(context.Background(), f.root))
	if metadataSemantics(t, f.db) != before {
		t.Fatal("preparation changed catalog")
	}
	o = exportOptions(t)
	result, e = exportPreserved(t, f, o, nil)
	must(t, e)
	if !result.Complete {
		t.Fatal(result)
	}
}

func TestRecoveryOwnedDumpAndCopy(t *testing.T) {
	for _, stage := range []string{"child-started", "payload-copy"} {
		t.Run(stage, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			reached, allow, done := make(chan struct{}), newCleanupSignal(), make(chan struct{})
			first := true
			var result ExportResult
			var opErr error
			h := &exportHooks{event: func(s string) error {
				if s == stage && first {
					first = false
					close(reached)
					<-allow.ch
				}
				return nil
			}}
			go func() { result, opErr = export(context.Background(), f.config, o, h); close(done) }()
			defer func() { allow.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, reached)
			id, e := coordination.ResolveIdentity(f.root)
			must(t, e)
			assertCleanupExclusion(t, id)
			noMarker(t, o.Destination)
			allow.fire()
			awaitCleanup(t, done)
			must(t, opErr)
			if !result.Complete {
				t.Fatal(result)
			}
			assertNoExportSessions(t, f)
			l, e := coordination.AcquireExisting(context.Background(), id)
			must(t, e)
			must(t, l.Release())
		})
	}
}

// Exercise the production version guard using a real connection with a
// controlled unsupported server-version response; no PG15/17 server is claimed.
type versionConnector struct {
	driver.Connector
	version int64
}

func (c versionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	raw, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	conn, ok := raw.(catalogDriverConn)
	if !ok {
		return nil, errors.Join(ErrUnsupported, raw.Close())
	}
	return versionConn{conn, c.version}, nil
}

type versionConn struct {
	catalogDriverConn
	version int64
}

func (c versionConn) Prepare(q string) (driver.Stmt, error) {
	if strings.Contains(q, "transaction_read_only") {
		q = strings.Replace(q, "current_setting('server_version_num')::integer", strconv.FormatInt(c.version, 10), 1)
	}
	return c.catalogDriverConn.Prepare(q)
}
func TestRecoveryServerVersionRefusal(t *testing.T) {
	for _, version := range []int64{150000, 170000} {
		f := exportFixture(t)
		o := exportOptions(t)
		r, e := exportPreserved(t, f, o, &exportHooks{cut: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return versionConnector{c, version} }}})
		assertIncomplete(t, o, r, e)
		if !errors.Is(e, ErrUnsupported) {
			t.Fatal(e)
		}
	}
}
