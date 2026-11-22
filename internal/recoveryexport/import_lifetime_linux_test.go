package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

func assertImportExclusion(t *testing.T, d Destination, o ImportOptions) {
	t.Helper()
	id, e := coordination.ResolveIdentity(d.PayloadRoot)
	must(t, e)
	assertCleanupExclusion(t, id)
	c := Config(d)
	c.PayloadRoot = o.Package
	r, e := resolve(context.Background(), c)
	must(t, e)
	defer clear(r.key)
	reservation, e := reserveImport(context.Background(), r)
	if e == nil {
		_ = reservation.close()
		t.Fatal("database reservation escaped during cleanup")
	}
	if _, e = os.Stat(filepath.Join(d.PayloadRoot, ".coldkeep-control", "IMPORT_READY")); !os.IsNotExist(e) {
		t.Fatal("premature import receipt")
	}
}

func TestRecoveryImportCleanupJoins(t *testing.T) {
	for _, kind := range []string{"blocked-success", "driver-error", "automatic-rollback-error"} {
		t.Run(kind, func(t *testing.T) {
			f, _, o := importFixture(t)
			d, _ := importTarget(t, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gates := newCleanupDriver(t, nil, nil)
			if kind == "driver-error" {
				gates.closeErr = errors.New("driver close failed")
			}
			if kind == "automatic-rollback-error" {
				gates.rollbackErr = driver.ErrBadConn
			}
			h := &importHooks{cut: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }, event: func(s string, _ *sql.Tx) error {
				if s == "consuming" && kind == "automatic-rollback-error" {
					cancel()
					<-gates.rollbackStarted.ch
					return ctx.Err()
				}
				return nil
			}}}
			done := make(chan struct{})
			var result ImportResult
			var opErr error
			go func() { result, opErr = importGeneration(ctx, d, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			assertImportExclusion(t, d, o)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			assertImportExclusion(t, d, o)
			select {
			case <-done:
				t.Fatal("returned before driver cleanup")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if kind == "blocked-success" {
				must(t, opErr)
				if !result.Ready {
					t.Fatal("successful cleanup not ready")
				}
			} else if opErr == nil || result.Published {
				t.Fatal("failed cleanup published")
			}
		})
	}
}

func TestRecoveryImportCredentialJoin(t *testing.T) {
	f, _, o := importFixture(t)
	d, _ := importTarget(t, f)
	started, allow := newCleanupSignal(), newCleanupSignal()
	h := &importHooks{credentials: func(clean func() error) error { started.fire(); <-allow.ch; return clean() }}
	done := make(chan struct{})
	var result ImportResult
	var opErr error
	go func() { result, opErr = importGeneration(context.Background(), d, o, h); close(done) }()
	defer func() { allow.fire(); awaitCleanup(t, done) }()
	awaitCleanup(t, started.ch)
	assertImportExclusion(t, d, o)
	select {
	case <-done:
		t.Fatal("credential cleanup not joined")
	default:
	}
	allow.fire()
	awaitCleanup(t, done)
	must(t, opErr)
	if !result.Ready {
		t.Fatal("successful join incomplete")
	}
}

func fakeRestore(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pg_restore")
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then exec " + os.Getenv("COLDKEEP_PG_RESTORE") + " --version; fi\n" + body + "\n"
	must(t, os.WriteFile(path, []byte(script), 0700))
	return path
}

func TestRecoveryImportChildJoin(t *testing.T) {
	f, _, o := importFixture(t)
	d, _ := importTarget(t, f)
	o.Tools.PGRestore = fakeRestore(t, "exec /bin/sleep 60")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, joined, allow := newCleanupSignal(), newCleanupSignal(), newCleanupSignal()
	var pid int
	h := &importHooks{files: &exportHooks{childPID: func(p int) { pid = p }, event: func(s string) error {
		if s == "child-started" {
			started.fire()
		}
		if s == "child-joined" {
			joined.fire()
			<-allow.ch
		}
		return nil
	}}}
	done := make(chan struct{})
	var result ImportResult
	var opErr error
	go func() { result, opErr = importGeneration(ctx, d, o, h); close(done) }()
	defer func() { cancel(); allow.fire(); awaitCleanup(t, done) }()
	awaitCleanup(t, started.ch)
	cancel()
	awaitCleanup(t, joined.ch)
	assertImportExclusion(t, d, o)
	if e := syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
		t.Fatal("child survived join", e)
	}
	allow.fire()
	awaitCleanup(t, done)
	if opErr == nil || result.Published {
		t.Fatal("canceled child published")
	}
}

func TestRecoveryImportCopyAndBounds(t *testing.T) {
	f, _, base := importFixture(t)
	for _, name := range []string{"member-substitution", "symlink", "hardlink", "count", "bytes", "expanded-sql", "short-write", "enospc", "close", "fsync", "archive-drift", "canceled-before-ready"} {
		t.Run(name, func(t *testing.T) {
			d, _ := importTarget(t, f)
			o := base
			h := &importHooks{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "count":
				o.Limits.Members = 1
			case "bytes":
				o.Limits.TotalBytes = 4096
			case "expanded-sql":
				o.Tools.PGRestore = fakeRestore(t, "exec /bin/dd if=/dev/zero bs=1048576 count=513 2>/dev/null")
			case "short-write":
				h.files = &exportHooks{write: func(_ *os.File, b []byte) (int, error) { return len(b) - 1, nil }}
			case "enospc":
				h.files = &exportHooks{write: func(_ *os.File, _ []byte) (int, error) { return 0, syscall.ENOSPC }}
			case "close":
				h.files = &exportHooks{close: func(f *os.File) error { return errors.Join(f.Close(), errors.New("close failed")) }}
			case "fsync":
				h.files = &exportHooks{sync: func(*os.File) error { return syscall.EIO }}
			case "archive-drift":
				h.event = func(s string) error {
					if s == "copied" {
						return os.WriteFile(filepath.Join(d.PayloadRoot, ".coldkeep-control", "catalog.dump"), []byte("wrong"), 0600)
					}
					return nil
				}
			case "canceled-before-ready":
				h.event = func(s string) error {
					if s == "before-ready" {
						cancel()
					}
					return nil
				}
			default:
				o.Package = filepath.Join(t.TempDir(), "package")
				must(t, copyTreeForImport(base.Package, o.Package))
				member := filepath.Join(o.Package, "object-000001.bin")
				if name == "symlink" {
					must(t, os.Remove(member))
					must(t, os.Symlink(filepath.Join(base.Package, "object-000001.bin"), member))
				}
				if name == "hardlink" {
					must(t, os.Link(member, filepath.Join(t.TempDir(), "second-link")))
				}
				if name == "member-substitution" {
					h.event = func(s string) error {
						if s == "owned" {
							return os.WriteFile(member, []byte("substituted"), 0600)
						}
						return nil
					}
				}
			}
			input := treeFingerprint(t, base.Package)
			r, e := importGeneration(ctx, d, o, h)
			if e == nil || r.Published || r.Ready {
				t.Fatalf("accepted failure %s: %+v %v", name, r, e)
			}
			if input != treeFingerprint(t, base.Package) {
				t.Fatal("original package changed")
			}
			if _, e = Check(context.Background(), base.Package, base.Limits); e != nil {
				t.Fatal("input no longer usable", e)
			}
		})
	}
}

func TestRecoveryImportFreshnessUnderReservation(t *testing.T) {
	f, _, o := importFixture(t)
	d, open := importTarget(t, f)
	h := &importHooks{event: func(s string) error {
		if s == "database-reserved" {
			execSQL(t, open(), "CREATE TABLE concurrent_object(x int)")
		}
		return nil
	}}
	r, e := importGeneration(context.Background(), d, o, h)
	if e == nil || r.Published {
		t.Fatal("freshness not rechecked")
	}
	// The importer must not undo an external writer or drop a provisioned DB.
	var exists bool
	must(t, open().QueryRow("SELECT to_regclass('public.concurrent_object') IS NOT NULL").Scan(&exists))
	if !exists {
		t.Fatal("caller database was erased")
	}
}

func TestRecoveryImportSourceBoundary(t *testing.T) {
	b, e := os.ReadFile("import_linux.go")
	must(t, e)
	if strings.Contains(string(b), "--clean") || strings.Contains(string(b), "--create") || strings.Contains(string(b), "--disable-triggers") {
		t.Fatal("forbidden restore mode")
	}
	if !strings.Contains(string(b), "--single-transaction") {
		t.Fatal("transactional restore missing")
	}
}
