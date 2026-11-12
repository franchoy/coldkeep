package recoveryexport

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/generation"
)

func TestRecoveryReadDisabledHistoryAndSelectorBounds(t *testing.T) {
	f, b, _ := upgradeFixture(t)
	source := generationID(t, "source")
	input := filepath.Join(t.TempDir(), "source")
	must(t, os.Mkdir(input, 0700))
	_, e := ApplyBinding(context.Background(), f.config, nextBinding(t, f, b, BindingSourceRegister, source, input, 0))
	must(t, e)
	_, e = ApplyBinding(context.Background(), f.config, nextBinding(t, f, b, BindingSourceDisable, source, "", 1))
	must(t, e)
	_, e = ApplyUpgrade(context.Background(), f.config, upgradeOptions(t, f, b))
	must(t, e)
	policy, gid := generationID(t, "policy"), generationID(t, "generation")
	fixtureGenerationPolicy(t, f, policy, source, "capture")
	insertGeneration(t, f, gid, source, policy)
	q := generationReadRequest(t, f, b)
	prepareReadFixture(t, f, q)
	// The repository root remains installed; the unrelated input source is gone.
	must(t, os.Rename(input, input+"-unavailable"))
	var targets []string
	var paths [][]string
	for chain, count := range []int{128, 13} {
		var parent any
		var components []string
		for n := range count {
			name := []byte("d")
			if chain == 1 {
				length := 255
				if n == 0 {
					length = 254
				}
				if n == 12 {
					length = 1
				}
				name = []byte(strings.Repeat("m", length))
			}
			id := fixedEntryID(10000 + chain*1000 + n)
			insertGenerationEntry(t, f, gid, id, parent, name, "directory", "stored", nil)
			components = append(components, base64.StdEncoding.EncodeToString(name))
			parent = id
			if n == count-1 {
				targets = append(targets, id)
			}
		}
		paths = append(paths, components)
	}
	before := catalogFingerprint(t, f.db)
	files := treeFingerprint(t, f.root)
	result, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
		page, err := reader.Read(ctx, readRequest(q, source, "", generation.QueryGenerations))
		if err != nil {
			return err
		}
		if len(page.Generations) != 1 || page.Generations[0].ID != gid {
			t.Fatal("disabled source lost history", page)
		}
		for n, target := range targets {
			request := readRequest(q, source, gid, generation.QueryEntry)
			request.EntryID = target
			entry, err := reader.Read(ctx, request)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(entry.Selection.Selector.Components, paths[n]) {
				t.Fatal("bounded selector differs")
			}
			request.Query, request.EntryID, request.Selector = generation.QuerySelector, "", &entry.Selection.Selector
			round, err := reader.Read(ctx, request)
			if err != nil {
				return err
			}
			if round.Selection.Entry.ID != target {
				t.Fatal("maximum selector selected a different entry")
			}
		}
		return nil
	})
	must(t, e)
	if !result.Complete || before != catalogFingerprint(t, f.db) || files != treeFingerprint(t, f.root) {
		t.Fatal("read-only history changed", result)
	}
}

func TestRecoveryReadAdmissionRefusals(t *testing.T) {
	f, b, _, _, _ := generationFixture(t)
	q := generationReadRequest(t, f, b)
	prepareReadFixture(t, f, q)
	for _, kind := range []string{"row-security", "changed-policy", "invalid-index", "missing-control"} {
		t.Run(kind, func(t *testing.T) {
			var restore func()
			switch kind {
			case "row-security":
				execSQL(t, f.db, `ALTER TABLE generation_entry ENABLE ROW LEVEL SECURITY; ALTER TABLE generation_entry FORCE ROW LEVEL SECURITY; CREATE POLICY hidden_read ON generation_entry USING(false)`)
				restore = func() {
					execSQL(t, f.db, `DROP POLICY hidden_read ON generation_entry; ALTER TABLE generation_entry DISABLE ROW LEVEL SECURITY; ALTER TABLE generation_entry NO FORCE ROW LEVEL SECURITY`)
				}
			case "changed-policy":
				execSQL(t, f.db, `ALTER TABLE generation_policy DISABLE TRIGGER aaaa_coldkeep_schema20_fence`)
				restore = func() { execSQL(t, f.db, `ALTER TABLE generation_policy ENABLE TRIGGER aaaa_coldkeep_schema20_fence`) }
			case "invalid-index":
				execSQL(t, f.db, `UPDATE pg_catalog.pg_index SET indisvalid=false WHERE indexrelid='public.coldkeep_generation_source_read_v1'::regclass`)
				restore = func() {
					execSQL(t, f.db, `UPDATE pg_catalog.pg_index SET indisvalid=true WHERE indexrelid='public.coldkeep_generation_source_read_v1'::regclass`)
				}
			case "missing-control":
				control := filepath.Join(f.root, ".coldkeep-control")
				must(t, os.Rename(control, control+"-saved"))
				restore = func() {
					if _, err := os.Stat(control); !os.IsNotExist(err) {
						t.Fatal("read created missing control")
					}
					must(t, os.Rename(control+"-saved", control))
				}
			}
			defer restore()
			result, err := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(context.Context, generation.Reader) error {
				t.Fatal("invalid admission reached consumer")
				return nil
			})
			if err == nil || result.Complete {
				t.Fatal(result, err)
			}
			prepared, err := PrepareGenerationReadIndex(context.Background(), f.config, q, exportOptions(t))
			if err == nil || prepared.Complete {
				t.Fatal("invalid installed source prepared", prepared, err)
			}
		})
	}
}
