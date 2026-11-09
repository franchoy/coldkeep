package recoveryexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestBindingControlAdmission(t *testing.T) {
	f, o := bindingFixture(t)
	_, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	for _, which := range []string{"marker", "descriptor"} {
		t.Run(which, func(t *testing.T) {
			path := filepath.Join(o.Request.InstallationDirectory, bindingDescriptorName)
			max := BindingDescriptorMaxBytes
			if which == "marker" {
				path = filepath.Join(f.root, ".coldkeep-control", bindingMarkerName)
				max = BindingMarkerMaxBytes
			}
			original, e := os.ReadFile(path)
			must(t, e)
			for _, kind := range []string{"missing", "future", "truncated", "oversized", "duplicate", "trailing", "symlink", "fifo", "hardlink", "permissions", "wrong-identity", "stale-revision"} {
				t.Run(kind, func(t *testing.T) {
					must(t, os.Remove(path))
					defer func() { _ = os.Remove(path); must(t, os.WriteFile(path, original, 0600)) }()
					b := append([]byte(nil), original...)
					switch kind {
					case "missing":
					case "future":
						b = []byte(strings.Replace(string(b), "/v1", "/v9", 1))
						must(t, os.WriteFile(path, b, 0600))
					case "truncated":
						must(t, os.WriteFile(path, b[:len(b)/2], 0600))
					case "oversized":
						must(t, os.WriteFile(path, []byte(strings.Repeat(" ", max+1)), 0600))
					case "duplicate":
						b = append([]byte(`{"kind":"bad",`), b[1:]...)
						must(t, os.WriteFile(path, b, 0600))
					case "trailing":
						must(t, os.WriteFile(path, append(b, []byte(" {}")...), 0600))
					case "symlink":
						target := filepath.Join(t.TempDir(), "target")
						must(t, os.WriteFile(target, b, 0600))
						must(t, os.Symlink(target, path))
					case "fifo":
						must(t, unix.Mkfifo(path, 0600))
					case "hardlink":
						target := filepath.Join(filepath.Dir(path), "hardlink")
						must(t, os.WriteFile(target, b, 0600))
						defer func() { must(t, os.Remove(target)) }()
						must(t, os.Link(target, path))
					case "permissions":
						must(t, os.WriteFile(path, b, 0644))
					case "wrong-identity":
						b = []byte(strings.Replace(string(b), "repo_", "xxxx_", 1))
						must(t, os.WriteFile(path, b, 0600))
					case "stale-revision":
						b = []byte(strings.Replace(string(b), `"revision":1`, `"revision":2`, 1))
						must(t, os.WriteFile(path, b, 0600))
					}
					catalog := catalogFingerprint(t, f.db)
					r, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, &f.config)
					if e == nil || r.OrdinaryWriterReady {
						t.Fatal("unsafe control accepted", r, e)
					}
					if catalog != catalogFingerprint(t, f.db) {
						t.Fatal("status mutated catalog")
					}
				})
			}
		})
	}
}
func TestBindingSubstitutedRoots(t *testing.T) {
	for _, kind := range []string{"absent", "empty", "unrelated", "copied-marker", "catalog-unavailable", "weaker-session"} {
		t.Run(kind, func(t *testing.T) {
			f, o := bindingFixture(t)
			_, e := ApplyBinding(context.Background(), f.config, o)
			must(t, e)
			if kind == "catalog-unavailable" {
				c := f.config
				c.Port = 1
				r, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, &c)
				if e == nil || r.Descriptor.Marker.RepositoryID == "" {
					t.Fatal(r, e)
				}
				// Preserve explicit locator while make the actual DB inaccessible to the role.
				execSQL(t, f.db, "REVOKE CONNECT ON DATABASE "+pqIdent(f.config.Database)+" FROM "+pqIdent(f.config.User))
				execSQL(t, f.db, "REVOKE CONNECT ON DATABASE "+pqIdent(f.config.Database)+" FROM PUBLIC")
				r, e = BindingStatus(context.Background(), o.Request.InstallationDirectory, &f.config)
				if e == nil || r.Status != "catalog-unavailable" {
					t.Fatal(r, e)
				}
				return
			}
			if kind == "weaker-session" {
				path := filepath.Join(o.Request.InstallationDirectory, bindingDescriptorName)
				raw, e := os.ReadFile(path)
				must(t, e)
				var d BindingDescriptor
				must(t, json.Unmarshal(raw, &d))
				d.Evidence.Namespace = 0
				must(t, os.WriteFile(path, []byte(bindingJSON(d)), 0600))
			} else {
				moved := f.root + "-original"
				must(t, os.Rename(f.root, moved))
				if kind != "absent" {
					must(t, os.Mkdir(f.root, 0700))
				}
				if kind == "unrelated" {
					must(t, os.WriteFile(filepath.Join(f.root, "unrelated"), []byte("keep"), 0600))
				}
				if kind == "copied-marker" {
					must(t, os.Mkdir(filepath.Join(f.root, ".coldkeep-control"), 0700))
					for _, name := range []string{bindingMarkerName, "repository.lock"} {
						b, e := os.ReadFile(filepath.Join(moved, ".coldkeep-control", name))
						must(t, e)
						must(t, os.WriteFile(filepath.Join(f.root, ".coldkeep-control", name), b, 0600))
					}
				}
			}
			before := catalogFingerprint(t, f.db)
			var watched func()
			if kind == "empty" {
				watched = bindingWatchNoWrites(t, f.root)
			}
			r, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, nil)
			if e == nil {
				t.Fatal(r)
			}
			_, e = ApplyBinding(context.Background(), f.config, o)
			if e == nil {
				t.Fatal("substitute admitted")
			}
			if before != catalogFingerprint(t, f.db) {
				t.Fatal("substitute mutated catalog")
			}
			if kind == "empty" {
				watched()
				entries, e := os.ReadDir(f.root)
				must(t, e)
				if len(entries) != 0 {
					t.Fatal("replacement initialized")
				}
			}
		})
	}
}
func TestBindingEarlyRefusals(t *testing.T) {
	for _, kind := range []string{"same-mount-descriptor", "descriptor-under-root", "unsafe-directory", "wrong-db", "wrong-key", "ambient", "stale-plan", "stale-revision", "retained-v1", "retained-v2", "retained-v3", "retained-malformed", "deadline", "catalog-budget", "checkpoint-budget"} {
		t.Run(kind, func(t *testing.T) {
			f, o := bindingFixture(t)
			c := f.config
			switch kind {
			case "same-mount-descriptor":
				o.Request.InstallationDirectory = t.TempDir()
			case "descriptor-under-root":
				o.Request.InstallationDirectory = filepath.Join(f.root, ".coldkeep-control")
			case "unsafe-directory":
				must(t, os.Chmod(o.Request.InstallationDirectory, 0755))
			case "wrong-db":
				c.Database = "not_the_selected_catalog"
			case "wrong-key":
				c.Key = func(context.Context) ([]byte, error) { return make([]byte, 32), nil }
			case "ambient":
				t.Setenv("PGHOST", "other")
			case "stale-plan":
				o.PlanSHA256 = strings.Repeat("0", 64)
			case "stale-revision":
				o.Request.ExpectedRevision = 3
			case "retained-v1", "retained-v2", "retained-v3", "retained-malformed":
				must(t, os.WriteFile(filepath.Join(f.root, ".coldkeep-control/IMPORT_READY"), []byte(kind), 0600))
			case "deadline":
				o.Export.InventoryLimits.Duration = time.Nanosecond
			case "catalog-budget":
				o.Export.InventoryLimits.Rows = 1
			case "checkpoint-budget":
				o.Export.Limits.TotalBytes = 1
			}
			catalog, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			entries, e := os.ReadDir(o.Request.InstallationDirectory)
			must(t, e)
			r, e := ApplyBinding(context.Background(), c, o)
			if e == nil || r.Status == "complete; bounded maintenance only" {
				t.Fatal(r, e)
			}
			if catalog != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("early refusal mutated source")
			}
			after, e := os.ReadDir(o.Request.InstallationDirectory)
			must(t, e)
			if len(entries) != len(after) {
				t.Fatal("created installation child before refusal")
			}
		})
	}
}
func TestBindingExactPathsAndBudgets(t *testing.T) {
	for _, n := range []int{BindingPathMaxBytes - 1, BindingPathMaxBytes, BindingPathMaxBytes + 1} {
		path := "/" + strings.Repeat("a", n-1)
		encoded, e := EncodeBindingPath(path)
		if n > BindingPathMaxBytes {
			if e == nil {
				t.Fatal("path ceiling")
			}
			continue
		}
		must(t, e)
		back, e := decodeBindingPath(encoded)
		must(t, e)
		if back != path {
			t.Fatal("path altered")
		}
	}
	for _, path := range []string{"/ whitespace Ä ", "/CASE", "/case", string([]byte{'/', 'x', 0xff})} {
		encoded, e := EncodeBindingPath(path)
		must(t, e)
		back, e := decodeBindingPath(encoded)
		must(t, e)
		if back != path {
			t.Fatal("path bytes lost")
		}
	}
	for _, path := range []string{"relative", "/x/../y", "/x/", "/", "/x\x00"} {
		if _, e := EncodeBindingPath(path); e == nil {
			t.Fatal(path)
		}
	}
	for _, max := range []int{BindingMarkerMaxBytes, BindingDescriptorMaxBytes, BindingIntentMaxBytes} {
		for _, delta := range []int{-1, 0, 1} {
			b := append([]byte("{}"), []byte(strings.Repeat(" ", max-2+delta))...)
			e := decodeBindingRecord(context.Background(), b, max, new(map[string]any))
			if delta > 0 {
				if !errors.Is(e, ErrLimit) {
					t.Fatal(e)
				}
			} else {
				must(t, e)
			}
		}
	}
}
func TestBindingSchemaAdmission(t *testing.T) {
	for _, kind := range []string{"column", "type", "nullability", "missing", "relocated", "duplicate", "wrong-kind", "wrong-definition", "not-valid", "deferrable", "compressed", "head", "lineage", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			f, o := bindingFixture(t)
			_, e := ApplyBinding(context.Background(), f.config, o)
			must(t, e)
			switch kind {
			case "column":
				execSQL(t, f.db, "ALTER TABLE root_binding_head ADD COLUMN unexpected text")
			case "type":
				execSQL(t, f.db, "ALTER TABLE configured_source ALTER COLUMN disabled TYPE text USING disabled::text")
			case "nullability":
				execSQL(t, f.db, "ALTER TABLE root_binding_revision ALTER COLUMN evidence DROP NOT NULL")
			case "missing":
				execSQL(t, f.db, "ALTER TABLE root_binding_head DROP CONSTRAINT root_binding_head_root_id_fkey")
			case "relocated":
				execSQL(t, f.db, "ALTER TABLE root_binding_head DROP CONSTRAINT root_binding_head_root_id_fkey; ALTER TABLE root_binding_revision ADD CONSTRAINT root_binding_head_root_id_fkey FOREIGN KEY(root_id) REFERENCES storage_root_identity(root_id)")
			case "duplicate":
				execSQL(t, f.db, "ALTER TABLE root_binding_head ADD CONSTRAINT root_binding_head_extra CHECK (revision>0)")
			case "wrong-kind":
				execSQL(t, f.db, "ALTER TABLE configured_source DROP CONSTRAINT configured_source_slot_key; ALTER TABLE configured_source ADD CONSTRAINT configured_source_slot_key CHECK (slot>0)")
			case "wrong-definition":
				execSQL(t, f.db, "ALTER TABLE configured_source DROP CONSTRAINT configured_source_slot_check; ALTER TABLE configured_source ADD CONSTRAINT configured_source_slot_check CHECK(slot BETWEEN 1 AND 65)")
			case "not-valid":
				execSQL(t, f.db, "ALTER TABLE configured_source DROP CONSTRAINT configured_source_slot_check; ALTER TABLE configured_source ADD CONSTRAINT configured_source_slot_check CHECK(slot BETWEEN 1 AND 64) NOT VALID")
			case "deferrable":
				execSQL(t, f.db, "ALTER TABLE root_binding_head ALTER CONSTRAINT root_binding_head_root_id_fkey DEFERRABLE")
			case "compressed":
				execSQL(t, f.db, "ALTER TABLE configured_source ADD CONSTRAINT too_large CHECK(source_id<>"+"'"+strings.Repeat("x", 20000)+"')")
			case "head":
				execSQL(t, f.db, "DELETE FROM root_binding_head")
			case "lineage":
				execSQL(t, f.db, "UPDATE root_binding_revision SET installation_id='install_00000000000000000000000000000000'") // valid portable ID, but local tuple must refuse.
			case "oversized":
				execSQL(t, f.db, "ALTER TABLE root_binding_revision DROP CONSTRAINT root_binding_revision_evidence_check; UPDATE root_binding_revision SET evidence=repeat('x',4097)")
			}
			consumed := false
			_, e = Inspect(context.Background(), f.config, DefaultLimits(), func(context.Context, *Inventory) error { consumed = true; return nil })
			if kind == "lineage" {
				_, e = BindingStatus(context.Background(), o.Request.InstallationDirectory, &f.config)
				if e == nil {
					t.Fatal("local lineage mismatch")
				}
			} else if e == nil || consumed {
				t.Fatal("malformed profile consumed", e)
			}
		})
	}
}
func TestBindingActiveSubstitution(t *testing.T) {
	for _, kind := range []string{"root", "control", "lock"} {
		t.Run(kind, func(t *testing.T) {
			f, o := bindingFixture(t)
			path := f.root
			if kind == "control" {
				path = filepath.Join(path, ".coldkeep-control")
			}
			if kind == "lock" {
				path = filepath.Join(path, ".coldkeep-control/repository.lock")
			}
			changed := false
			r, e := applyBinding(context.Background(), f.config, o, &bindingHooks{event: func(stage string, _ *sql.Tx) error {
				if stage == "before-commit" && !changed {
					changed = true
					must(t, os.Rename(path, path+"-retained"))
					if kind == "lock" {
						must(t, os.WriteFile(path, nil, 0600))
					} else {
						must(t, os.Mkdir(path, 0700))
					}
				}
				return nil
			}})
			if e == nil || !changed || r.Status == "complete; bounded maintenance only" {
				t.Fatal(r, e)
			}
			var version int
			must(t, f.db.QueryRow("SELECT catalog_version FROM schema_version").Scan(&version))
			if version != 18 {
				t.Fatal("substitution committed")
			}
		})
	}
}
