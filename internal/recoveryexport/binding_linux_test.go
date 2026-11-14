package recoveryexport

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

func bindingFixture(t *testing.T) (*fixture, BindingOptions) {
	t.Helper()
	f, a := adoptionFixture(t)
	_, e := ApplyAdoption(context.Background(), f.config, a)
	must(t, e)
	directory, e := os.MkdirTemp("/dev/shm", "coldkeep-phase3b-")
	must(t, e)
	t.Cleanup(func() { must(t, os.RemoveAll(directory)) })
	operation, e := bindingID("bind")
	must(t, e)
	q := BindingRequest{Action: BindingInitial, InstallationDirectory: directory, ExpectedSchema: 18, OperationID: operation}
	p, e := PreviewBinding(context.Background(), f.config, q, DefaultLimits())
	must(t, e)
	return f, BindingOptions{Request: q, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)}
}
func nextBinding(t *testing.T, f *fixture, old BindingOptions, action BindingAction, source, path string, srevision int64) BindingOptions {
	t.Helper()
	var rev int64
	must(t, f.db.QueryRow("SELECT revision FROM root_binding_head").Scan(&rev))
	id, e := bindingID("bind")
	must(t, e)
	encoded := ""
	if path != "" {
		encoded, e = EncodeBindingPath(path)
		must(t, e)
	}
	q := BindingRequest{Action: action, InstallationDirectory: old.Request.InstallationDirectory, ExpectedSchema: 19, ExpectedRevision: rev, OperationID: id, SourceID: source, SourcePathBase64: encoded, ExpectedSourceRevision: srevision}
	p, e := PreviewBinding(context.Background(), f.config, q, DefaultLimits())
	must(t, e)
	return BindingOptions{Request: q, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)}
}
func TestBindingMigrationAndRecovery(t *testing.T) {
	f, o := bindingFixture(t)
	legacy := legacyRowsFingerprint(t, f.db)
	r, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	if r.Status != "complete; bounded maintenance only" || r.Descriptor.Marker.Revision != 1 || r.OrdinaryWriterReady {
		t.Fatal(r)
	}
	if legacy != legacyRowsFingerprint(t, f.db) {
		t.Fatal("legacy rows changed")
	}
	must(t, validateBindingFixture(t, f))
	again, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	if again.Descriptor != r.Descriptor {
		t.Fatal("retry changed IDs/revision")
	}
	status, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, &f.config)
	must(t, e)
	if status.Status != "bounded-maintenance-ready; ordinary writers refused" {
		t.Fatal(status)
	}
	if e = db.EnsurePostgresSchema(f.db); e == nil {
		t.Fatal("ordinary schema19 writer admitted")
	}
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
	d, open := importTarget(t, f)
	imported, e := Import(context.Background(), d, importOptionsFor(t, post))
	must(t, e)
	if !imported.Ready || imported.Receipt.Kind != "coldkeep-import-ready/v3" || imported.Receipt.RepositoryID != r.Descriptor.Marker.RepositoryID {
		t.Fatal(imported)
	}
	var importedOps int
	must(t, open().QueryRow("SELECT count(*) FROM binding_operation").Scan(&importedOps))
	if importedOps != 1 {
		t.Fatal(importedOps)
	}
	t.Logf("PHASE3B schema19 package3 receipt3 root=%s revision=%d complete lineage preserved", r.Descriptor.Marker.RootID, r.Descriptor.Marker.Revision)
}
func validateBindingFixture(t *testing.T, f *fixture) error {
	t.Helper()
	_, e := Inspect(context.Background(), f.config, DefaultLimits(), func(_ context.Context, i *Inventory) error {
		if i.CatalogVersion != 19 {
			return ErrUnsupported
		}
		return validateBindingGraph(i)
	})
	return e
}
func TestBindingSourcesAndOfflineRelocation(t *testing.T) {
	f, o := bindingFixture(t)
	first, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	originalLock, e := os.Stat(filepath.Join(f.root, ".coldkeep-control/repository.lock"))
	must(t, e)
	var sources []string
	for _, name := range []string{" Source Ä ", "source ä"} {
		path := filepath.Join(t.TempDir(), name)
		must(t, os.Mkdir(path, 0500))
		source, e := db.NewIdentity("source")
		must(t, e)
		op := nextBinding(t, f, o, BindingSourceRegister, source, path, 0)
		r, e := ApplyBinding(context.Background(), f.config, op)
		must(t, e)
		if r.Continuity != "new" {
			t.Fatal(r)
		}
		entries, e := os.ReadDir(path)
		must(t, e)
		if len(entries) != 0 {
			t.Fatal("source writes")
		}
		sources = append(sources, source)
		if len(sources) == 1 {
			moved := path + " moved"
			must(t, os.Rename(path, moved))
			op = nextBinding(t, f, o, BindingSourceRebind, source, moved, 1)
			r, e = ApplyBinding(context.Background(), f.config, op)
			must(t, e)
			if r.Continuity != "same-object" {
				t.Fatal(r)
			}
			replacement := t.TempDir()
			op = nextBinding(t, f, o, BindingSourceRebind, source, replacement, 2)
			r, e = ApplyBinding(context.Background(), f.config, op)
			must(t, e)
			if r.Continuity != "changed" {
				t.Fatal(r)
			}
			op = nextBinding(t, f, o, BindingSourceDisable, source, "", 3)
			_, e = ApplyBinding(context.Background(), f.config, op)
			must(t, e)
		}
	}
	moved := f.root + " relocated"
	must(t, os.Rename(f.root, moved))
	f.root = moved
	f.config.PayloadRoot = moved
	status, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, nil)
	if e == nil || status.Status != "root-unavailable" {
		t.Fatal(status, e)
	}
	op := nextBinding(t, f, o, BindingRelocate, "", "", 0)
	r, e := ApplyBinding(context.Background(), f.config, op)
	must(t, e)
	if r.Descriptor.Marker.RootID != first.Descriptor.Marker.RootID || r.Descriptor.Marker.Incarnation != first.Descriptor.Marker.Incarnation {
		t.Fatal("relocation changed identity")
	}
	lock, e := os.Stat(filepath.Join(moved, ".coldkeep-control/repository.lock"))
	must(t, e)
	if !os.SameFile(originalLock, lock) {
		t.Fatal("lock replaced")
	}
	op = nextBinding(t, f, o, BindingReconnect, "", "", 0)
	r, e = ApplyBinding(context.Background(), f.config, op)
	must(t, e)
	if r.Descriptor.Marker.Incarnation == first.Descriptor.Marker.Incarnation {
		t.Fatal("reconnect did not advance incarnation")
	}
	must(t, validateBindingFixture(t, f))
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
}
func TestBindingCrashCuts(t *testing.T) {
	for _, stage := range []string{"after-intent", "before-ddl", "during-ddl", "during-data", "before-commit", "after-commit", "published:" + bindingMarkerName, "published:" + bindingDescriptorName, "before-intent-removal"} {
		t.Run(stage, func(t *testing.T) {
			f, o := bindingFixture(t)
			fault := errors.New("binding injected cut")
			r, e := applyBinding(context.Background(), f.config, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
				if s == stage {
					return fault
				}
				return nil
			}})
			if !errors.Is(e, fault) || r.Status == "complete; bounded maintenance only" {
				t.Fatal(r, e)
			}
			var version int
			must(t, f.db.QueryRow("SELECT catalog_version FROM schema_version").Scan(&version))
			r, e = ReconcileBinding(context.Background(), f.config, o)
			must(t, e)
			if r.Descriptor.Marker.Revision != 1 {
				t.Fatal(r)
			}
			var n int
			must(t, f.db.QueryRow("SELECT count(*) FROM binding_operation").Scan(&n))
			if n != 1 {
				t.Fatal("double operation")
			}
			if version == 19 && r.Status != "complete; bounded maintenance only" {
				t.Fatal(r)
			}
		})
	}
}
