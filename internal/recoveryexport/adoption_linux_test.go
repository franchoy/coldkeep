package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
)

func adoptionFixture(t *testing.T) (*fixture, AdoptionOptions) {
	t.Helper()
	f := exportFixture(t)
	execSQL(t, f.db, "INSERT INTO snapshot(id,type,created_at) VALUES('legacy-adoption','full','2025-01-02T03:04:05Z')")
	// Deliberately grant the existing non-superuser fixture role adoption ownership.
	for _, spec := range catalogTables {
		execSQL(t, f.db, "ALTER TABLE public."+pqIdent(spec.name)+" OWNER TO "+pqIdent(f.config.User))
	}
	execSQL(t, f.db, "GRANT CREATE ON SCHEMA public TO "+pqIdent(f.config.User))
	p, e := PreviewAdoption(context.Background(), f.config, DefaultLimits())
	must(t, e)
	id, e := db.NewIdentity("adopt")
	must(t, e)
	return f, AdoptionOptions{ExpectedSchema: 17, OperationID: id, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)}
}
func TestAdoptionRoundTrip(t *testing.T) {
	f, o := adoptionFixture(t)
	before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
	legacy := legacyRowsFingerprint(t, f.db)
	preview, e := PreviewAdoption(context.Background(), f.config, DefaultLimits())
	must(t, e)
	if preview.PlanSHA256 != o.PlanSHA256 || before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
		t.Fatal("preview mutated or changed plan")
	}
	must(t, db.EnsurePostgresSchema(f.db))
	r, e := ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	if !db.ValidIdentity("repo", string(r.RepositoryID)) {
		t.Fatal(r)
	}
	if legacy != legacyRowsFingerprint(t, f.db) {
		t.Fatal("adoption changed existing history/recipes")
	}
	if tree != treeFingerprint(t, f.root) {
		t.Fatal("adoption changed payload/control")
	}
	again, e := ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	if r != again {
		t.Fatal("retry changed lineage")
	}
	conflicting := o
	conflicting.OperationID, _ = db.NewIdentity("adopt")
	_, e = ApplyAdoption(context.Background(), f.config, conflicting)
	if !errors.Is(e, ErrAdoptionConflict) {
		t.Fatal(e)
	}
	if e = db.EnsurePostgresSchema(f.db); e == nil {
		t.Fatal("general writer admitted schema18")
	}
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
	for _, pkg := range []ExportOptions{o.Export, post} {
		d, open := importTarget(t, f)
		result, e := Import(context.Background(), d, importOptionsFor(t, pkg))
		must(t, e)
		if !result.Ready {
			t.Fatal(result)
		}
		conn := open()
		id, e := coordination.ResolveIdentity(d.PayloadRoot)
		must(t, e)
		lease, e := coordination.AcquireExisting(context.Background(), id)
		must(t, e)
		must(t, ValidateRestoreOwned(context.Background(), Config(d), lease, *result.Receipt))
		must(t, lease.Release())
		if pkg.Destination == post.Destination {
			var repo string
			must(t, conn.QueryRow("SELECT repository_id FROM repository_identity").Scan(&repo))
			if repo != string(r.RepositoryID) {
				t.Fatal("lineage changed")
			}
		}
	}
	t.Log("PHASE3A preview/apply/reconcile/schema17+18 recovery profiles PASS")
}
func TestAdoptionTransactionFaults(t *testing.T) {
	for _, stage := range []string{"before-ddl", "during-ddl", "during-data", "before-commit", "after-commit"} {
		t.Run(stage, func(t *testing.T) {
			f, o := adoptionFixture(t)
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			injected := errors.New("deterministic transaction fault")
			r, e := applyAdoption(context.Background(), f.config, o, &adoptionHooks{event: func(s string, _ *sql.Tx) error {
				if s == stage {
					return injected
				}
				return nil
			}})
			if !errors.Is(e, injected) {
				t.Fatal(e)
			}
			if tree != treeFingerprint(t, f.root) {
				t.Fatal("payload/control mutated")
			}
			if stage == "after-commit" {
				if !errors.Is(e, ErrAdoptionUncertain) {
					t.Fatal(e)
				}
				again, e := ApplyAdoption(context.Background(), f.config, o)
				must(t, e)
				if again.RepositoryID != r.RepositoryID {
					t.Fatal("lost acknowledgement created new identity")
				}
			} else if before != catalogFingerprint(t, f.db) {
				t.Fatal("failed transaction changed logical catalog/schema")
			}
			check, e := Check(context.Background(), o.Export.Destination, o.Export.Limits)
			must(t, e)
			if !check.ByteComplete {
				t.Fatal("rollback checkpoint lost")
			}
		})
	}
}
func TestAdoptionEarlyRefusals(t *testing.T) {
	for _, kind := range []string{"sqlite", "expected-schema", "request", "plan", "missing-key", "retained-receipt", "checkpoint-failure", "checkpoint-corrupt", "checkpoint-missing"} {
		t.Run(kind, func(t *testing.T) {
			f, o := adoptionFixture(t)
			var h *adoptionHooks
			switch kind {
			case "sqlite":
				f.config.Backend = "sqlite"
			case "expected-schema":
				o.ExpectedSchema = 18
			case "request":
				o.OperationID = "adopt_bad"
			case "plan":
				o.PlanSHA256 = fmt.Sprintf("%064d", 1)
			case "missing-key":
				f.config.Key = nil
				f.config.KeyReference = ""
			case "retained-receipt":
				must(t, os.WriteFile(f.root+"/.coldkeep-control/IMPORT_READY", []byte(`{"kind":"coldkeep-import-ready/v1"}`), 0600))
			case "checkpoint-failure":
				h = &adoptionHooks{export: &exportHooks{event: func(s string) error {
					if s == "payload-copy" {
						return errors.New("copy failure")
					}
					return nil
				}}}
			case "checkpoint-corrupt", "checkpoint-missing":
				h = &adoptionHooks{event: func(s string, _ *sql.Tx) error {
					if s == "checkpoint-published" {
						if kind == "checkpoint-missing" {
							return os.Remove(o.Export.Destination + "/COMPLETE")
						}
						return os.WriteFile(o.Export.Destination+"/catalog.dump", []byte("corrupt"), 0600)
					}
					return nil
				}}
			}
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			_, e := applyAdoption(context.Background(), f.config, o, h)
			if e == nil {
				t.Fatal("expected refusal")
			}
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("early refusal changed source")
			}
		})
	}
}

func legacyRowsFingerprint(t *testing.T, conn *sql.DB) string {
	t.Helper()
	hash := sha256.New()
	for _, spec := range catalogTables {
		if spec.name == "schema_version" {
			continue
		}
		rows, e := conn.Query("SELECT row_to_json(t)::text FROM public." + pqIdent(spec.name) + " t ORDER BY row_to_json(t)::text")
		must(t, e)
		for rows.Next() {
			var row string
			must(t, rows.Scan(&row))
			fmt.Fprintln(hash, row)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	return hex.EncodeToString(hash.Sum(nil))
}
