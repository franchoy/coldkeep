package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"golang.org/x/sys/unix"
)

func TestBindingProcessHelper(t *testing.T) {
	path := os.Getenv("CK_BINDING_CHILD")
	if path == "" {
		return
	}
	b, e := os.ReadFile(path)
	must(t, e)
	var payload struct {
		Config  string
		Options BindingOptions
		Stage   string
	}
	must(t, json.Unmarshal(b, &payload))
	b, e = os.ReadFile(payload.Config)
	must(t, e)
	var c struct {
		Host                                                             string
		Port                                                             uint16
		Database, User, PayloadRoot, PasswordFile, KeyFile, KeyReference string
	}
	// Operator JSON uses snake_case; decode into the exact map explicitly.
	var values map[string]json.RawMessage
	must(t, json.Unmarshal(b, &values))
	for name, dest := range map[string]any{"host": &c.Host, "port": &c.Port, "database": &c.Database, "user": &c.User, "payload_root": &c.PayloadRoot, "password_file": &c.PasswordFile, "key_file": &c.KeyFile, "key_reference": &c.KeyReference} {
		must(t, json.Unmarshal(values[name], dest))
	}
	password, e := os.ReadFile(c.PasswordFile)
	must(t, e)
	var key []byte
	if c.KeyFile != "" {
		b, e = os.ReadFile(c.KeyFile)
		must(t, e)
		key, e = hex.DecodeString(string(b))
		must(t, e)
	}
	cfg := Config{Backend: "postgres", Profile: "manual-postgresql-docker", Transport: "local-plaintext", Host: c.Host, Port: c.Port, Database: c.Database, User: c.User, PayloadRoot: c.PayloadRoot, KeyReference: c.KeyReference, Password: func(context.Context) (string, error) { return string(password), nil }}
	if key != nil {
		cfg.Key = func(context.Context) ([]byte, error) { return key, nil }
	}
	_, e = applyBinding(context.Background(), cfg, payload.Options, &bindingHooks{event: func(stage string, _ *sql.Tx) error {
		if stage == payload.Stage {
			_, _ = os.Stdout.WriteString("ACTUAL_PROCESS_CUT " + stage + "\n")
			_ = unix.Kill(os.Getpid(), unix.SIGKILL)
		}
		return nil
	}})
	t.Fatalf("termination stage not reached: %v", e)
}
func TestBindingProcessTermination(t *testing.T) {
	for _, stage := range []string{"before-intent", "after-intent", "before-commit", "after-commit", "published:" + bindingMarkerName, "published:" + bindingDescriptorName} {
		t.Run(stage, func(t *testing.T) {
			f, o := bindingFixture(t)
			payload := struct {
				Config  string
				Options BindingOptions
				Stage   string
			}{readerConfig(t, Destination(f.config)), o, stage}
			path := filepath.Join(t.TempDir(), "child.json")
			must(t, os.WriteFile(path, []byte(bindingJSON(payload)), 0600))
			cmd := exec.Command(os.Args[0], "-test.run=^TestBindingProcessHelper$")
			cmd.Env = append(os.Environ(), "CK_BINDING_CHILD="+path)
			out, e := cmd.CombinedOutput()
			if e == nil || !strings.Contains(string(out), "ACTUAL_PROCESS_CUT "+stage) {
				t.Fatalf("no actual kill boundary: %v %s", e, out)
			}
			if stage == "before-intent" {
				o.Export = exportOptions(t)
			} // retain orphan checkpoint, choose a fresh destination explicitly.
			r, e := ReconcileBinding(context.Background(), f.config, o)
			must(t, e)
			if r.Descriptor.Marker.Revision != 1 {
				t.Fatal(r)
			}
			assertBindingNoSessions(t, f)
		})
	}
}
func assertBindingNoSessions(t *testing.T, f *fixture) {
	t.Helper()
	var n int
	must(t, f.db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name LIKE 'coldkeep-%'").Scan(&n))
	if n != 0 {
		t.Fatal("owned sessions remain", n)
	}
}
func bindingContenders(t *testing.T, f *fixture) {
	t.Helper()
	id, e := coordination.ResolveIdentity(f.root)
	must(t, e)
	lease, e := coordination.AcquireExisting(context.Background(), id)
	if lease != nil {
		_ = lease.Release()
	}
	if !errors.Is(e, coordination.ErrRepositoryBusy) && !errors.Is(e, coordination.ErrNestedRepositoryAcquisition) {
		t.Fatal("same-process contender entered", e)
	}
	adoptionContender(t, f.root)
}
func TestBindingReservationsThroughPublication(t *testing.T) {
	f, o := bindingFixture(t)
	other := filepath.Join(t.TempDir(), "other-root")
	must(t, os.CopyFS(other, os.DirFS(f.root)))
	selected := f.config
	selected.PayloadRoot = other
	r, e := resolve(context.Background(), selected)
	must(t, e)
	defer clear(r.key)
	seen := map[string]bool{}
	_, e = applyBinding(context.Background(), f.config, o, &bindingHooks{event: func(stage string, _ *sql.Tx) error {
		switch stage {
		case "after-commit", "published:" + bindingMarkerName, "published:" + bindingDescriptorName, "cleanup":
			bindingContenders(t, f)
			id, e := coordination.ResolveIdentity(other)
			must(t, e)
			lease, e := coordination.AcquireExisting(context.Background(), id)
			must(t, e)
			reservation, e := reserveImport(context.Background(), r)
			if reservation != nil {
				_ = reservation.close()
			}
			must(t, lease.Release())
			if e == nil {
				t.Fatal("second root entered reserved DB")
			}
			seen[stage] = true
		}
		return nil
	}})
	must(t, e)
	if len(seen) != 4 {
		t.Fatal(seen)
	}
	assertBindingNoSessions(t, f)
}
func TestBindingCleanupJoins(t *testing.T) {
	for _, kind := range []string{"inspection", "migration", "cancellation", "rollback-error", "close-error"} {
		t.Run(kind, func(t *testing.T) {
			f, o := bindingFixture(t)
			gates := newCleanupDriver(t, nil, nil)
			if kind == "rollback-error" {
				gates.rollbackErr = errors.New("injected rollback failure")
			}
			if kind == "close-error" {
				gates.closeErr = errors.New("injected close failure")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			adapter := &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}
			h := &bindingHooks{}
			if kind == "migration" {
				h.migration = adapter
				h.event = func(stage string, _ *sql.Tx) error {
					if stage == "before-commit" {
						return errors.New("rollback barrier")
					}
					return nil
				}
			} else {
				h.cut = adapter
			}
			done := make(chan struct{})
			var err error
			go func() { _, err = applyBinding(ctx, f.config, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			if kind == "cancellation" {
				cancel()
			}
			bindingContenders(t, f)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			bindingContenders(t, f)
			select {
			case <-done:
				t.Fatal("returned before driver close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if kind != "inspection" {
				if err == nil {
					t.Fatal("cancellation lost")
				}
			} else {
				must(t, err)
			}
			assertBindingNoSessions(t, f)
		})
	}
}
func TestBindingPublicationFailures(t *testing.T) {
	for _, stage := range []string{"file-write:", "file-sync:", "file-rename:", "directory-sync:", "file-close:"} {
		t.Run(stage, func(t *testing.T) {
			f, o := bindingFixture(t)
			fault := errors.New("control durability fault")
			r, e := applyBinding(context.Background(), f.config, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
				if s == stage+bindingMarkerName {
					return fault
				}
				return nil
			}})
			if !errors.Is(e, fault) || !errors.Is(e, ErrBindingReconciliation) || r.Status != "committed; reconciliation-required" {
				t.Fatal(r, e)
			}
			r, e = ReconcileBinding(context.Background(), f.config, o)
			must(t, e)
			if r.Descriptor.Marker.Revision != 1 {
				t.Fatal(r)
			}
			assertBindingNoSessions(t, f)
		})
	}
}

func TestBindingExportCleanup(t *testing.T) {
	for _, kind := range []string{"rollback", "credential", "child"} {
		t.Run(kind, func(t *testing.T) {
			f, o := bindingFixture(t)
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var opErr error
			eh := &exportHooks{}
			h := &bindingHooks{export: eh}
			started, allow := newCleanupSignal(), newCleanupSignal()
			switch kind {
			case "rollback":
				gates := newCleanupDriver(t, nil, nil)
				gates.allowClose.fire()
				eh.cut = &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}
				started = gates.rollbackStarted
				allow = gates.allowRollback
			case "credential":
				eh.credentials = func(clean func() error) error { started.fire(); <-allow.ch; return clean() }
			case "child":
				o.Export.Tools.PGDump = fakeDump(t, "exec /bin/sleep 60")
				eh.event = func(s string) error {
					if s == "child-started" {
						cancel()
					}
					if s == "child-joined" {
						started.fire()
						<-allow.ch
					}
					return nil
				}
			}
			go func() { _, opErr = applyBinding(ctx, f.config, o, h); close(done) }()
			defer func() { allow.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, started.ch)
			adoptionContender(t, f.root)
			noMarker(t, o.Export.Destination)
			if kind != "child" {
				cancel()
			}
			allow.fire()
			awaitCleanup(t, done)
			if opErr == nil {
				t.Fatal("canceled cleanup succeeded")
			}
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("export cleanup failure changed source")
			}
		})
	}
}
