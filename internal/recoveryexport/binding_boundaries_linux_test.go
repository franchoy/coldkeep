package recoveryexport

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBindingRecoveryAdmissionBoundaries(t *testing.T) {
	f, files := recoveryHistory(t, false)
	_ = bindHistory(t, f)
	pkg := exportOptions(t)
	_, e := Export(context.Background(), f.config, pkg)
	must(t, e)
	for _, kind := range []string{"future", "mixed", "wrong-repository", "malformed-restored-head"} {
		t.Run(kind, func(t *testing.T) {
			opts := importOptionsFor(t, pkg)
			opts.Package = filepath.Join(t.TempDir(), "package")
			must(t, copyTreeForImport(pkg.Destination, opts.Package))
			mutateImportManifest(t, &opts, func(m *Manifest) {
				switch kind {
				case "future":
					m.CatalogVersion = 20
				case "mixed":
					m.Version = 2
				case "wrong-repository":
					m.RepositoryID = "repo_00000000000000000000000000000000"
				}
			})
			d, open := importTarget(t, f)
			var h *importHooks
			if kind == "malformed-restored-head" {
				h = &importHooks{event: func(stage string) error {
					if stage == "restored" {
						_, e := open().Exec("DELETE FROM root_binding_head")
						return e
					}
					return nil
				}}
			}
			r, e := importGeneration(context.Background(), d, opts, h)
			if e == nil || r.Ready {
				t.Fatal("bad import admitted", r, e)
			}
			if _, e = os.Lstat(filepath.Join(d.PayloadRoot, ".coldkeep-control/IMPORT_READY")); !os.IsNotExist(e) {
				t.Fatal("new readiness published")
			}
		})
	}
	t.Run("restore-before-pins", func(t *testing.T) {
		d, open := importTarget(t, f)
		r, e := Import(context.Background(), d, importOptionsFor(t, pkg))
		must(t, e)
		if !r.Ready {
			t.Fatal(r)
		}
		conn := open()
		execSQL(t, conn, `CREATE SEQUENCE public.binding_pin_attempts;
CREATE FUNCTION public.binding_pin_probe() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.binding_pin_attempts'); RETURN NEW; END $$;
CREATE TRIGGER binding_pin_probe BEFORE UPDATE OF pin_count ON public.chunk FOR EACH ROW EXECUTE FUNCTION public.binding_pin_probe()`)
		execSQL(t, conn, "GRANT USAGE,SELECT ON SEQUENCE public.binding_pin_attempts TO "+pqIdent(d.User))
		binary := os.Getenv("COLDKEEP_RECOVERY_READER")
		if binary == "" {
			t.Fatal("staged reader required")
		}
		cfg := readerConfig(t, d)
		output := filepath.Join(t.TempDir(), "valid")
		out, e := runReader(t, binary, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
		if e != nil {
			t.Fatalf("positive restore %v %s", e, out)
		}
		checkRestoredHistory(t, output, files)
		var before int64
		var called bool
		must(t, conn.QueryRow("SELECT last_value,is_called FROM binding_pin_attempts").Scan(&before, &called))
		if !called {
			t.Fatal("positive pin detector did not fire")
		}
		execSQL(t, conn, "ALTER TABLE root_binding_head DROP CONSTRAINT root_binding_head_root_id_fkey; ALTER TABLE root_binding_revision ADD CONSTRAINT root_binding_head_root_id_fkey FOREIGN KEY(root_id) REFERENCES storage_root_identity(root_id)")
		receiptPath := filepath.Join(d.PayloadRoot, ".coldkeep-control/IMPORT_READY")
		receipt, e := os.ReadFile(receiptPath)
		must(t, e)
		output = filepath.Join(t.TempDir(), "refused")
		out, e = runReader(t, binary, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
		if e == nil || !strings.Contains(string(out), ErrUnsupported.Error()) {
			t.Fatalf("restore refusal %v %s", e, out)
		}
		if _, e = os.Lstat(output); !os.IsNotExist(e) {
			t.Fatal("refused restore output exists")
		}
		var after int64
		must(t, conn.QueryRow("SELECT last_value FROM binding_pin_attempts").Scan(&after))
		now, e := os.ReadFile(receiptPath)
		must(t, e)
		if before != after || string(now) != string(receipt) {
			t.Fatal("refused restore pins/receipt changed")
		}
	})
	t.Run("export-no-complete", func(t *testing.T) {
		execSQL(t, f.db, "ALTER TABLE root_binding_head DROP CONSTRAINT root_binding_head_root_id_fkey")
		o := exportOptions(t)
		r, e := Export(context.Background(), f.config, o)
		if e == nil || r.Complete {
			t.Fatal("malformed schema19 exported")
		}
		noMarker(t, o.Destination)
	})
}
