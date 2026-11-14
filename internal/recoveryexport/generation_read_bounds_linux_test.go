package recoveryexport

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestRecoveryReadPageBoundaries(t *testing.T) {
	f, b, _ := upgradeFixture(t)
	sources := registerGenerationSources(t, f, b)
	_, err := ApplyUpgrade(context.Background(), f.config, upgradeOptions(t, f, b))
	must(t, err)
	q := generationReadRequest(t, f, b)
	prepareReadFixture(t, f, q)
	t.Run("empty-source-versus-missing", func(t *testing.T) {
		_, err := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
			request := readRequest(q, sources[0], "", generation.QueryGenerations)
			page, err := reader.Read(ctx, request)
			if err != nil {
				return err
			}
			if len(page.Generations) != 0 || page.NextCursor != "" {
				t.Fatal("empty source not empty")
			}
			request.SourceID = "source_" + strings.Repeat("0", 32)
			if _, err = reader.Read(ctx, request); !errors.Is(err, ErrGenerationNotFound) {
				t.Fatal("missing source collapsed into empty", err)
			}
			return nil
		})
		must(t, err)
	})
	policy, gid := generationID(t, "policy"), fixedGenerationID(1000000)
	fixtureGenerationPolicy(t, f, policy, sources[0], "capture")
	insertGeneration(t, f, gid, sources[0], policy)
	parent, empty := fixedEntryID(1000000), fixedEntryID(1000001)
	insertGenerationEntry(t, f, gid, parent, nil, []byte("populated"), "directory", "stored", nil)
	insertGenerationEntry(t, f, gid, empty, nil, []byte("empty"), "directory", "stored", nil)
	previous := 0
	for _, count := range []int{0, 1, 100, 101, 200, 201} {
		t.Run(fmt.Sprintf("members-%d", count), func(t *testing.T) {
			for n := previous + 1; n <= count; n++ {
				insertGenerationEntry(t, f, gid, fixedEntryID(n), parent, []byte(fmt.Sprintf("%03d", n)), "directory", "stored", nil)
				if n > 1 {
					insertGeneration(t, f, fixedGenerationID(n), sources[0], policy)
				}
			}
			previous = count
			_, err := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
				for _, query := range []string{generation.QueryChildren, generation.QueryGenerations} {
					request := readRequest(q, sources[0], gid, query)
					request.Limit = 100
					expected := count
					if query == generation.QueryChildren {
						request.ParentID = &parent
					} else {
						request.GenerationID = ""
						expected = max(1, count)
					}
					seen := map[string]bool{}
					for {
						page, err := reader.Read(ctx, request)
						if err != nil {
							return err
						}
						if len(page.Entries)+len(page.Generations) != min(100, expected-len(seen)) {
							t.Fatal("wrong exact page count")
						}
						for _, e := range page.Entries {
							if seen[e.ID] {
								t.Fatal("duplicate entry")
							}
							seen[e.ID] = true
						}
						for _, g := range page.Generations {
							if seen[g.ID] {
								t.Fatal("duplicate generation")
							}
							seen[g.ID] = true
						}
						if (page.NextCursor != "") != (len(seen) < expected) {
							t.Fatal("wrong sentinel/cursor boundary")
						}
						if page.NextCursor == "" {
							break
						}
						request.Cursor = page.NextCursor
					}
					if len(seen) != expected {
						t.Fatal("incomplete exact enumeration")
					}
				}
				return nil
			})
			must(t, err)
		})
	}
	t.Run("empty-parent-versus-missing", func(t *testing.T) {
		_, err := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
			request := readRequest(q, sources[0], gid, generation.QueryChildren)
			request.ParentID = &empty
			page, err := reader.Read(ctx, request)
			if err != nil {
				return err
			}
			if len(page.Entries) != 0 || page.NextCursor != "" {
				t.Fatal("empty parent not empty")
			}
			unknown := fixedEntryID(99999)
			request.ParentID = &unknown
			if _, err = reader.Read(ctx, request); !errors.Is(err, ErrGenerationNotFound) {
				t.Fatal("missing parent collapsed into empty", err)
			}
			return nil
		})
		must(t, err)
	})
	t.Run("caller-response-limit", func(t *testing.T) {
		_, err := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
			request := readRequest(q, sources[0], gid, generation.QueryEntry)
			request.EntryID = parent
			page, err := reader.Read(ctx, request)
			if err != nil {
				return err
			}
			raw, err := generation.EncodeResponse(page)
			if err != nil {
				return err
			}
			request.ResponseLimit = len(raw)
			if _, err = reader.Read(ctx, request); err != nil {
				return err
			}
			for _, limit := range []int{len(raw) - 1, 1} {
				request.ResponseLimit = limit
				refused, err := reader.Read(ctx, request)
				if !errors.Is(err, generation.ErrResponseLimit) || refused.Kind != "" {
					t.Fatal("oversized single item published", refused, err)
				}
			}
			return nil
		})
		must(t, err)
	})
	for _, kind := range []string{"lowered-rows", "lowered-bytes", "lowered-deadline"} {
		t.Run(kind, func(t *testing.T) {
			limits := DefaultLimits()
			switch kind {
			case "lowered-rows":
				limits.Rows = 1
			case "lowered-bytes":
				limits.CatalogBytes = 1
			case "lowered-deadline":
				limits.Duration = time.Nanosecond
			}
			result, err := WithGenerationReadView(context.Background(), f.config, q, limits, func(context.Context, generation.Reader) error { t.Fatal("lowered admission ignored"); return nil })
			if err == nil || result.Complete {
				t.Fatal(result, err)
			}
		})
	}
	t.Run("oversized-database-component", func(t *testing.T) {
		// Test-only corruption is last, retained in this disposable catalog only.
		execSQL(t, f.db, `ALTER TABLE generation_entry DROP CONSTRAINT generation_entry_component_check`)
		execSQL(t, f.db, `UPDATE generation_entry SET component=decode(repeat('ff',256),'hex') WHERE entry_id=$1`, empty)
		result, err := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(context.Context, generation.Reader) error {
			t.Fatal("oversized database field reached reader")
			return nil
		})
		if err == nil || result.Complete {
			t.Fatal(result, err)
		}
	})
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
