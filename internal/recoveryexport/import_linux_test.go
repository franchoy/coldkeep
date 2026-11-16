package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
)

func importFixture(t *testing.T) (*fixture, ExportOptions, ImportOptions) {
	t.Helper()
	f := exportFixture(t)
	o := exportOptions(t)
	_, e := exportPreserved(t, f, o, nil)
	must(t, e)
	return f, o, importOptionsFor(t, o)
}
func importOptionsFor(t *testing.T, o ExportOptions) ImportOptions {
	t.Helper()
	check, e := Check(context.Background(), o.Destination, o.Limits)
	must(t, e)
	raw, e := os.ReadFile(filepath.Join(o.Destination, "manifest.json"))
	must(t, e)
	hash := sha256.Sum256(raw)
	return ImportOptions{Package: o.Destination, Generation: check.Manifest.Generation, ManifestSHA256: hex.EncodeToString(hash[:]),
		TrustedOperatorBackup: true, ExclusiveTarget: true, Tools: o.Tools, Limits: o.Limits, InventoryLimits: DefaultLimits(), DatabaseCapacityBytes: 768 << 20}
}
func importTarget(t *testing.T, f *fixture) (Destination, func() *sql.DB) {
	t.Helper()
	name := fmt.Sprintf("ck_import_%d_%d", os.Getpid(), fixtureSeq.Add(1))
	dsn, e := db.BuildPostgresConnStringFromEnv("postgres")
	must(t, e)
	admin, e := sql.Open("postgres", dsn)
	must(t, e)
	execSQL(t, admin, "CREATE DATABASE "+pqIdent(name)+" OWNER "+pqIdent(f.config.User)+" TEMPLATE template0")
	t.Cleanup(func() { execSQL(t, admin, "DROP DATABASE "+pqIdent(name)+" WITH (FORCE)"); must(t, admin.Close()) })
	d := Destination(f.config)
	d.Database = name
	d.PayloadRoot = filepath.Join(t.TempDir(), "import")
	return d, func() *sql.DB {
		dsn, e := db.BuildPostgresConnStringFromEnv(name)
		must(t, e)
		c, e := sql.Open("postgres", dsn)
		must(t, e)
		t.Cleanup(func() { must(t, c.Close()) })
		return c
	}
}
func importPreserved(t *testing.T, f *fixture, d Destination, o ImportOptions, h *importHooks) (ImportResult, error) {
	t.Helper()
	pkg, tree, cat := treeFingerprint(t, o.Package), treeFingerprint(t, f.root), catalogFingerprint(t, f.db)
	r, e := importGeneration(context.Background(), d, o, h)
	if pkg != treeFingerprint(t, o.Package) || tree != treeFingerprint(t, f.root) || cat != catalogFingerprint(t, f.db) {
		t.Fatal("import changed package/source")
	}
	var n int
	must(t, f.db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('coldkeep-import-reservation','coldkeep-recovery-import','coldkeep-passive-inventory')").Scan(&n))
	if n != 0 {
		t.Fatalf("owned session leak: %d", n)
	}
	_, marker := os.Stat(filepath.Join(d.PayloadRoot, ".coldkeep-control", "IMPORT_READY"))
	if !r.Published && !os.IsNotExist(marker) {
		t.Fatalf("premature handoff: %v", marker)
	}
	return r, e
}

func TestRecoveryImportRoundTrip(t *testing.T) {
	f, _, o := importFixture(t)
	d, open := importTarget(t, f)
	r, e := importPreserved(t, f, d, o, nil)
	must(t, e)
	if !r.Ready || !r.Published || r.Receipt.TargetRootHash == r.Receipt.LegacyRootObservation {
		t.Fatalf("receipt: %+v", r)
	}
	c := open()
	if metadataSemantics(t, c) != metadataSemantics(t, f.db) {
		t.Fatal("full metadata differs")
	}
	var last, next int64
	must(t, c.QueryRow("SELECT last_value FROM public.logical_file_id_seq").Scan(&last))
	must(t, c.QueryRow("SELECT nextval('public.logical_file_id_seq')").Scan(&next))
	if next != last+1 {
		t.Fatal("sequence semantics")
	}
	if _, e = c.Exec("INSERT INTO snapshot_file(snapshot_id,path_id,logical_file_id) VALUES('missing',1,1)"); e == nil {
		t.Fatal("constraint omitted")
	}
	t.Logf("D3 full 19-table semantics, sequences/functions/triggers/indexes/constraints retained; target=%s role=%s ready=true", d.Database, d.User)
}

func TestRecoveryImportTrustAndPackage(t *testing.T) {
	f, _, base := importFixture(t)
	for _, name := range []string{"untrusted", "generation", "digest", "missing", "incomplete", "corrupt", "unsupported"} {
		t.Run(name, func(t *testing.T) {
			o := base
			d, _ := importTarget(t, f)
			switch name {
			case "untrusted":
				o.TrustedOperatorBackup = false
			case "generation":
				o.Generation = strings.Repeat("0", 32)
			case "digest":
				o.ManifestSHA256 = strings.Repeat("0", 64)
			case "missing":
				o.Package += "absent"
			default:
				copy := filepath.Join(t.TempDir(), "package")
				must(t, copyTreeForImport(base.Package, copy))
				o.Package = copy
				switch name {
				case "incomplete":
					must(t, os.Remove(filepath.Join(copy, "COMPLETE")))
				case "corrupt":
					must(t, os.WriteFile(filepath.Join(copy, "catalog.dump"), []byte("broken"), 0600))
				case "unsupported":
					mutateImportManifest(t, &o, func(m *Manifest) { m.Version = 2 })
				}
			}
			r, e := importGeneration(context.Background(), d, o, nil)
			if e == nil || r.Ready {
				t.Fatal("accepted invalid input")
			}
			if _, e = os.Stat(d.PayloadRoot); !os.IsNotExist(e) {
				t.Fatal("target created before admission")
			}
		})
	}
}

func copyTreeForImport(src, dst string) error {
	if e := os.Mkdir(dst, 0700); e != nil {
		return e
	}
	entries, e := os.ReadDir(src)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		b, e := os.ReadFile(filepath.Join(src, entry.Name()))
		if e != nil {
			return e
		}
		if e = os.WriteFile(filepath.Join(dst, entry.Name()), b, 0600); e != nil {
			return e
		}
	}
	return nil
}
func mutateImportManifest(t *testing.T, o *ImportOptions, fn func(*Manifest)) {
	t.Helper()
	p := filepath.Join(o.Package, "manifest.json")
	b, e := os.ReadFile(p)
	must(t, e)
	var m Manifest
	must(t, json.Unmarshal(b, &m))
	fn(&m)
	b, e = json.Marshal(m)
	must(t, e)
	must(t, os.WriteFile(p, b, 0600))
	h := sha256.Sum256(b)
	o.ManifestSHA256 = hex.EncodeToString(h[:])
	b, e = json.Marshal(completionMarker{m.Version, m.Generation, o.ManifestSHA256})
	must(t, e)
	must(t, os.WriteFile(filepath.Join(o.Package, "COMPLETE"), b, 0600))
}

func TestRecoveryImportSemanticAdmission(t *testing.T) {
	f, _, base := importFixture(t)
	for _, name := range []string{"unsafe-name", "reserved-name", "id", "format", "rows", "reader", "key-declaration", "name", "missing-object", "extra-object", "payload-header"} {
		t.Run(name, func(t *testing.T) {
			o := base
			o.Package = filepath.Join(t.TempDir(), "package")
			must(t, copyTreeForImport(base.Package, o.Package))
			mutateImportManifest(t, &o, func(m *Manifest) {
				switch name {
				case "unsafe-name":
					m.Members[1].SourceName = "../escape"
				case "reserved-name":
					m.Members[1].SourceName = ".coldkeep-control"
				case "id":
					m.Members[1].ContainerID += 500
				case "format":
					m.Members[1].FormatMajor = 1 - m.Members[1].FormatMajor
				case "rows":
					m.CatalogRows++
				case "reader":
					m.Reader = "/bin/true"
				case "key-declaration":
					m.KeyRequired = false
					m.KeyReference = ""
				case "name":
					m.Members[1].SourceName = "different.bin"
				case "missing-object":
					must(t, os.Remove(filepath.Join(o.Package, m.Members[len(m.Members)-1].Name)))
					m.Members = m.Members[:len(m.Members)-1]
				case "extra-object":
					extra := m.Members[1]
					extra.Name = fmt.Sprintf("object-%06d.bin", len(m.Members))
					extra.ContainerID = 999
					extra.SourceName = "extra.bin"
					data, e := os.ReadFile(filepath.Join(o.Package, m.Members[1].Name))
					must(t, e)
					must(t, os.WriteFile(filepath.Join(o.Package, extra.Name), data, 0600))
					m.Members = append(m.Members, extra)
				case "payload-header":
					path := filepath.Join(o.Package, m.Members[1].Name)
					data, e := os.ReadFile(path)
					must(t, e)
					data[8] = 99
					data[9] = 0
					must(t, os.WriteFile(path, data, 0600))
					h := sha256.Sum256(data)
					m.Members[1].SHA256 = hex.EncodeToString(h[:])
				}
			})
			d, _ := importTarget(t, f)
			if name == "key-declaration" {
				d.Key = nil
				d.KeyReference = ""
			}
			r, e := importPreserved(t, f, d, o, nil)
			if e == nil || r.Ready {
				t.Fatal("semantic forgery admitted")
			}
		})
	}
}

func TestRecoveryImportTargetRefusals(t *testing.T) {
	f, _, o := importFixture(t)
	for _, name := range []string{"populated", "system", "privileged", "membership", "incompatible", "existing-root", "package-alias", "no-exclusive", "missing-key", "wrong-key", "unsupported-key", "ambient"} {
		t.Run(name, func(t *testing.T) {
			d, open := importTarget(t, f)
			opts := o
			switch name {
			case "populated":
				execSQL(t, open(), "CREATE TABLE alien(x int)")
			case "system":
				d.Database = "postgres"
			case "privileged":
				d.User = os.Getenv("DB_USER")
				d.Password = func(context.Context) (string, error) { return os.Getenv("DB_PASSWORD"), nil }
			case "membership":
				execSQL(t, f.db, "GRANT pg_read_server_files TO "+pqIdent(d.User))
				defer execSQL(t, f.db, "REVOKE pg_read_server_files FROM "+pqIdent(d.User))
			case "incompatible":
				opts.Package = filepath.Join(t.TempDir(), "package")
				must(t, copyTreeForImport(o.Package, opts.Package))
				mutateImportManifest(t, &opts, func(m *Manifest) { m.Database.Collate = "mismatch" })
			case "existing-root":
				must(t, os.Mkdir(d.PayloadRoot, 0700))
			case "package-alias":
				d.PayloadRoot = filepath.Join(o.Package, "target")
			case "no-exclusive":
				opts.ExclusiveTarget = false
			case "missing-key":
				d.Key = nil
			case "wrong-key":
				d.Key = func(context.Context) ([]byte, error) { return make([]byte, 32), nil }
			case "unsupported-key":
				d.Key = func(context.Context) ([]byte, error) { return make([]byte, 16), nil }
			case "ambient":
				d.Password = func(context.Context) (string, error) {
					t.Setenv("PGHOST", "hostile")
					return "inventory_fixture_only", nil
				}
			}
			r, e := importPreserved(t, f, d, opts, nil)
			if e == nil || r.Ready {
				t.Fatal("unsafe target admitted")
			}
		})
	}
}

func TestRecoveryImportContention(t *testing.T) {
	f, _, o := importFixture(t)
	d, _ := importTarget(t, f)
	h := &importHooks{event: func(stage string) error {
		if stage != "owned" {
			return nil
		}
		sameDB := d
		sameDB.PayloadRoot = filepath.Join(t.TempDir(), "other")
		r, e := Import(context.Background(), sameDB, o)
		if e == nil || r.Ready {
			t.Fatal("same database/different root admitted")
		}
		other, _ := importTarget(t, f)
		other.PayloadRoot = d.PayloadRoot
		r, e = Import(context.Background(), other, o)
		if e == nil || r.Ready {
			t.Fatal("same root admitted")
		}
		id, e := coordination.ResolveIdentity(d.PayloadRoot)
		must(t, e)
		if l, e := coordination.AcquireExisting(context.Background(), id); e == nil {
			_ = l.Release()
			t.Fatal("lease escaped")
		}
		return nil
	}}
	r, e := importGeneration(context.Background(), d, o, h)
	must(t, e)
	if !r.Ready {
		t.Fatal("owner failed")
	}
}

func TestRecoveryImportFailures(t *testing.T) {
	f, _, o := importFixture(t)
	for _, stage := range []string{"database-reserved", "owned", "copied", "restored", "active-cleaned", "before-ready", "ready-published", "credential-close", "release"} {
		t.Run(stage, func(t *testing.T) {
			d, _ := importTarget(t, f)
			failure := errors.New("injected import failure")
			h := &importHooks{event: func(s string) error {
				if s == stage {
					return failure
				}
				return nil
			}}
			if stage == "credential-close" {
				h.credentials = func(close func() error) error { return errors.Join(close(), failure) }
			}
			if stage == "release" {
				h.release = func(close func() error) error { return errors.Join(close(), failure) }
			}
			r, e := importPreserved(t, f, d, o, h)
			if e == nil || r.Ready {
				t.Fatal("failure ready")
			}
			if r.Published != (stage == "ready-published" || stage == "release") {
				t.Fatalf("false publication outcome %+v", r)
			}
		})
	}
}
