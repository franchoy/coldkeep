package recoveryexport

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/generation"
)

func generationReadRequest(t *testing.T, f *fixture, b BindingOptions) ReadViewRequest {
	t.Helper()
	q := ReadViewRequest{InstallationDirectory: b.Request.InstallationDirectory}
	must(t, f.db.QueryRow(`SELECT repository_id FROM repository_identity`).Scan(&q.RepositoryID))
	must(t, f.db.QueryRow(`SELECT root_id,revision FROM root_binding_head`).Scan(&q.RootID, &q.ExpectedRevision))
	return q
}
func prepareReadFixture(t *testing.T, f *fixture, q ReadViewRequest) {
	t.Helper()
	r, e := PrepareGenerationReadIndex(context.Background(), f.config, q, exportOptions(t))
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
}
func readRequest(q ReadViewRequest, source, gid, query string) generation.Request {
	return generation.Request{Version: 1, RepositoryID: q.RepositoryID, SourceID: source, GenerationID: gid, Query: query}
}
func fixedGenerationID(n int) string { return fmt.Sprintf("generation_%032x", n) }
func fixedEntryID(n int) string      { return fmt.Sprintf("entry_%032x", n) }

func TestRecoveryReadIndexAndModel(t *testing.T) {
	f, b, _, sources, gens := generationFixture(t)
	q := generationReadRequest(t, f, b)
	before := upgradeCatalogDigest(generationInventory(t, f))
	payload := treeFingerprint(t, f.root)
	t.Run("preparation-required", func(t *testing.T) {
		r, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(context.Context, generation.Reader) error { t.Fatal("unprepared view admitted"); return nil })
		if !errors.Is(e, ErrReadPreparationRequired) || r.Complete {
			t.Fatal(r, e)
		}
	})
	t.Run("checkpoint-and-idempotent", func(t *testing.T) {
		o := exportOptions(t)
		r, e := PrepareGenerationReadIndex(context.Background(), f.config, q, o)
		must(t, e)
		if !r.Complete || r.Reconciled || r.ManifestSHA256 == "" {
			t.Fatal(r)
		}
		check, e := Check(context.Background(), o.Destination, o.Limits)
		must(t, e)
		if !check.ByteComplete || check.Manifest.CatalogVersion != 20 {
			t.Fatal(check)
		}
		again, e := PrepareGenerationReadIndex(context.Background(), f.config, q, o)
		must(t, e)
		if !again.Complete || !again.Reconciled || again.Checkpoint != "" {
			t.Fatal(again)
		}
		if before != upgradeCatalogDigest(generationInventory(t, f)) || payload != treeFingerprint(t, f.root) {
			t.Fatal("logical or physical identity changed")
		}
	})
	parent := fixedEntryID(1)
	insertGenerationEntry(t, f, gens[0], parent, nil, []byte("directory"), "directory", "stored", nil)
	names := [][]byte{[]byte("" + "A"), []byte("a"), []byte(" leading "), []byte("é"), []byte("e\u0301"), []byte("\\"), []byte("\n"), []byte("�"), {0xff}, []byte(strings.Repeat("m", 255))}
	want := map[string]string{}
	for i, name := range names {
		for _, gid := range gens {
			insertGenerationEntry(t, f, gid, fixedEntryID(i+10), nil, name, "directory", "stored", nil)
		}
		want[base64.StdEncoding.EncodeToString(name)] = fixedEntryID(i + 10)
	}
	want[base64.StdEncoding.EncodeToString([]byte("directory"))] = parent
	insertGenerationEntry(t, f, gens[0], fixedEntryID(100), parent, []byte("nested"), "file", "missing", nil)
	for i := 1; i <= 4; i++ {
		var policy string
		must(t, f.db.QueryRow(`SELECT policy_id FROM capture_generation WHERE generation_id=$1`, gens[0]).Scan(&policy))
		insertGeneration(t, f, fixedGenerationID(i*10), sources[0], policy)
	}
	var retained generation.Reader
	var oldCursor string
	t.Run("scopes-pages-and-selectors", func(t *testing.T) {
		r, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
			retained = reader
			for _, limit := range []int{1, 0, 100} {
				request := readRequest(q, sources[0], gens[0], generation.QueryChildren)
				request.Limit = limit
				seen := map[string]string{}
				var order []string
				for {
					page, e := reader.Read(ctx, request)
					if e != nil {
						return e
					}
					if request.Cursor == "" {
						again, e := reader.Read(ctx, request)
						if e != nil || !reflect.DeepEqual(again, page) {
							t.Fatal("first page changed", e)
						}
						if limit == 1 {
							oldCursor = page.NextCursor
						}
					}
					for _, entry := range page.Entries {
						if _, ok := seen[entry.ComponentBase64]; ok {
							t.Fatal("duplicate entry")
						}
						seen[entry.ComponentBase64] = entry.ID
						raw, e := base64.StdEncoding.DecodeString(entry.ComponentBase64)
						must(t, e)
						order = append(order, string(raw))
						lookup := readRequest(q, sources[0], gens[0], generation.QueryEntry)
						lookup.EntryID = entry.ID
						selected, e := reader.Read(ctx, lookup)
						if e != nil {
							return e
						}
						lookup.Query = generation.QuerySelector
						lookup.EntryID = ""
						lookup.Selector = &selected.Selection.Selector
						round, e := reader.Read(ctx, lookup)
						if e != nil {
							return e
						}
						if !reflect.DeepEqual(round.Selection, selected.Selection) {
							t.Fatal("selector changed identity")
						}
					}
					if page.NextCursor == "" {
						break
					}
					request.Cursor = page.NextCursor
				}
				if !reflect.DeepEqual(seen, want) || !sort.StringsAreSorted(order) {
					t.Fatal("wrong exact enumeration", seen, want)
				}
			}
			request := readRequest(q, sources[0], gens[0], generation.QueryChildren)
			for _, kind := range []string{"watermark", "entry-key", "parent", "source", "query"} {
				c, e := generation.DecodeCursor(oldCursor)
				must(t, e)
				bad := request
				switch kind {
				case "watermark":
					c.Upper = base64.StdEncoding.EncodeToString([]byte{0xff, 0xff})
				case "entry-key":
					c.EntryID = fixedEntryID(999)
				case "parent":
					bad.ParentID = &parent
				case "source":
					bad.SourceID = sources[1]
				case "query":
					bad = readRequest(q, sources[0], "", generation.QueryGenerations)
				}
				bad.Cursor, e = generation.EncodeCursor(c)
				must(t, e)
				if _, e = reader.Read(ctx, bad); e == nil {
					t.Fatal("invalid cursor scope/key admitted", kind)
				}
			}
			request.ParentID = &parent
			page, e := reader.Read(ctx, request)
			if e != nil {
				return e
			}
			if len(page.Entries) != 1 || page.Entries[0].ID != fixedEntryID(100) {
				t.Fatal(page)
			}
			detail, e := reader.Read(ctx, readRequest(q, sources[0], gens[0], generation.QueryDetail))
			if e != nil {
				return e
			}
			if detail.Detail.Generation.PublicationReceipt != nil || detail.Detail.Generation.Outcome != "incomplete" || detail.Detail.Root.Mode == nil {
				t.Fatal(detail)
			}
			request = readRequest(q, sources[0], "", generation.QueryGenerations)
			request.Limit = 1
			var ids []string
			for {
				page, e := reader.Read(ctx, request)
				if e != nil {
					return e
				}
				for _, g := range page.Generations {
					ids = append(ids, g.ID)
				}
				if page.NextCursor == "" {
					break
				}
				request.Cursor = page.NextCursor
			}
			if len(ids) != 5 || !sort.StringsAreSorted(ids) {
				t.Fatal(ids)
			}
			for _, kind := range []string{"source", "repository", "generation", "parent", "entry", "limit"} {
				bad := readRequest(q, sources[0], gens[0], generation.QueryChildren)
				switch kind {
				case "source":
					bad.SourceID = sources[1]
				case "repository":
					bad.RepositoryID = "repo_" + strings.Repeat("0", 32)
				case "generation":
					bad.GenerationID = fixedGenerationID(999)
				case "parent":
					id := fixedEntryID(999)
					bad.ParentID = &id
				case "entry":
					bad.Query = generation.QueryEntry
					bad.EntryID = fixedEntryID(999)
				case "limit":
					bad.Limit = 101
				}
				if _, e := reader.Read(ctx, bad); e == nil {
					t.Fatal("wrong scope accepted", kind)
				}
			}
			return nil
		})
		must(t, e)
		if !r.Complete {
			t.Fatal(r)
		}
	})
	t.Run("closed-and-cross-view", func(t *testing.T) {
		request := readRequest(q, sources[0], gens[0], generation.QueryChildren)
		if _, e := retained.Read(context.Background(), request); !errors.Is(e, ErrReadViewClosed) {
			t.Fatal(e)
		}
		r, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
			request.Cursor = oldCursor
			if _, e := reader.Read(ctx, request); e == nil {
				t.Fatal("cross-view cursor accepted")
			}
			return nil
		})
		must(t, e)
		if !r.Complete {
			t.Fatal(r)
		}
	})
}

func TestRecoveryReadLateCommit(t *testing.T) {
	f, b, _, sources, gens := generationFixture(t)
	q := generationReadRequest(t, f, b)
	prepareReadFixture(t, f, q)
	var policy, repo string
	must(t, f.db.QueryRow(`SELECT policy_id,repository_id FROM capture_generation WHERE generation_id=$1`, gens[0]).Scan(&policy, &repo))
	for _, n := range []int{20, 30, 40} {
		insertGeneration(t, f, fixedGenerationID(n), sources[0], policy)
	}
	for n, name := range []string{"a", "m", "z"} {
		insertGenerationEntry(t, f, gens[0], fixedEntryID(n+1), nil, []byte(name), "directory", "stored", nil)
	}
	late, e := f.db.Begin()
	must(t, e)
	defer func() { _ = late.Rollback() }()
	_, e = late.Exec(`INSERT INTO capture_generation SELECT $1,repository_id,source_id,source_revision,policy_id,policy_sha256,started_ns,ended_ns,scan_state,outcome,NULL FROM capture_generation WHERE generation_id=$2`, fixedGenerationID(10), gens[0])
	must(t, e)
	_, e = late.Exec(`INSERT INTO generation_root SELECT $1,mode,mtime_ns FROM generation_root WHERE generation_id=$2`, fixedGenerationID(10), gens[0])
	must(t, e)
	_, e = late.Exec(`INSERT INTO generation_entry VALUES($1,$2,NULL,'directory',$3,'directory','stored',NULL,NULL,NULL)`, gens[0], fixedEntryID(10), []byte("b"))
	must(t, e)
	r, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
		request := readRequest(q, sources[0], "", generation.QueryGenerations)
		request.Limit = 1
		first, e := reader.Read(ctx, request)
		if e != nil {
			return e
		}
		children := readRequest(q, sources[0], gens[0], generation.QueryChildren)
		children.Limit = 1
		firstChildren, e := reader.Read(ctx, children)
		if e != nil {
			return e
		}
		must(t, late.Commit())
		insertGenerationEntry(t, f, gens[0], fixedEntryID(11), nil, []byte("c"), "directory", "stored", nil)
		insertGenerationEntry(t, f, gens[0], fixedEntryID(12), nil, []byte("zz"), "directory", "stored", nil)
		insertGeneration(t, f, fixedGenerationID(15), sources[0], policy)
		insertGeneration(t, f, "generation_"+strings.Repeat("f", 32), sources[0], policy)
		seen := []string{first.Generations[0].ID}
		request.Cursor = first.NextCursor
		for request.Cursor != "" {
			page, e := reader.Read(ctx, request)
			if e != nil {
				return e
			}
			for _, g := range page.Generations {
				seen = append(seen, g.ID)
			}
			request.Cursor = page.NextCursor
		}
		if len(seen) != 4 {
			t.Fatal("snapshot admitted late commits", seen)
		}
		childCount := len(firstChildren.Entries)
		children.Cursor = firstChildren.NextCursor
		for children.Cursor != "" {
			page, e := reader.Read(ctx, children)
			if e != nil {
				return e
			}
			childCount += len(page.Entries)
			children.Cursor = page.NextCursor
		}
		if childCount != 3 {
			t.Fatal("child snapshot admitted late commits", childCount)
		}
		return nil
	})
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
	r, e = WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
		page, e := reader.Read(ctx, readRequest(q, sources[0], "", generation.QueryGenerations))
		if e == nil && len(page.Generations) != 7 {
			t.Fatal("new view lost committed rows", page)
		}
		childPage, childErr := reader.Read(ctx, readRequest(q, sources[0], gens[0], generation.QueryChildren))
		if childErr != nil {
			return childErr
		}
		if len(childPage.Entries) != 6 {
			t.Fatal("new child view lost commits")
		}
		return e
	})
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
}

func TestRecoveryReadIndexRefusals(t *testing.T) {
	f, b, _, _, _ := generationFixture(t)
	q := generationReadRequest(t, f, b)
	for label, definition := range map[string]string{
		"wrong-relation":  `CREATE INDEX coldkeep_generation_source_read_v1 ON public.generation_policy(repository_id,source_id,policy_id)`,
		"same-name-table": `CREATE TABLE coldkeep_generation_source_read_v1 (dummy integer)`,
		"unique":          `CREATE UNIQUE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id COLLATE "C",source_id COLLATE "C",generation_id COLLATE "C")`,
		"nulls-first":     `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id COLLATE "C",source_id COLLATE "C",generation_id COLLATE "C" NULLS FIRST)`,
		"column-order":    `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(source_id,repository_id,generation_id)`,
		"descending":      `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id COLLATE "C",source_id COLLATE "C",generation_id COLLATE "C" DESC)`,
		"predicate":       `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id COLLATE "C",source_id COLLATE "C",generation_id COLLATE "C") WHERE outcome='incomplete'`,
		"included-column": `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id COLLATE "C",source_id COLLATE "C",generation_id COLLATE "C") INCLUDE (outcome)`,
		"opclass":         `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id COLLATE "C" text_pattern_ops,source_id COLLATE "C",generation_id COLLATE "C")`,
		"collation":       `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation(repository_id,source_id,generation_id)`,
	} {
		t.Run("definition-"+label, func(t *testing.T) {
			execSQL(t, f.db, definition)
			if label == "same-name-table" {
				defer execSQL(t, f.db, `DROP TABLE public.coldkeep_generation_source_read_v1`)
			} else {
				defer execSQL(t, f.db, `DROP INDEX public.coldkeep_generation_source_read_v1`)
			}
			o := exportOptions(t)
			r, e := PrepareGenerationReadIndex(context.Background(), f.config, q, o)
			if e == nil || r.Complete {
				t.Fatal(r, e)
			}
			if _, e = os.Stat(o.Destination); !os.IsNotExist(e) {
				t.Fatal("conflicting definition wrote checkpoint")
			}
		})
	}
	for _, kind := range []string{"repository", "root", "revision", "intent", "imported"} {
		t.Run(kind, func(t *testing.T) {
			bad := q
			var path string
			switch kind {
			case "repository":
				bad.RepositoryID = "repo_" + strings.Repeat("0", 32)
			case "root":
				bad.RootID = "root_" + strings.Repeat("0", 32)
			case "revision":
				bad.ExpectedRevision++
			case "intent":
				path = filepath.Join(q.InstallationDirectory, bindingIntentName)
			case "imported":
				path = filepath.Join(f.root, ".coldkeep-control", "IMPORT_READY")
			}
			if path != "" {
				must(t, os.WriteFile(path, []byte("unrecognized"), 0600))
				defer os.Remove(path)
			}
			r, e := PrepareGenerationReadIndex(context.Background(), f.config, bad, exportOptions(t))
			if e == nil || r.Complete {
				t.Fatal(r, e)
			}
		})
	}
	for _, stage := range []string{"before-ddl", "after-ddl", "before-commit", "after-commit"} {
		t.Run(stage, func(t *testing.T) {
			o := exportOptions(t)
			cut := errors.New("injected index cut")
			r, e := prepareGenerationReadIndex(context.Background(), f.config, q, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
				if s == stage {
					return cut
				}
				return nil
			}})
			if !errors.Is(e, cut) || r.Complete {
				t.Fatal(r, e)
			}
			var exists bool
			must(t, f.db.QueryRow(`SELECT to_regclass('public.coldkeep_generation_source_read_v1') IS NOT NULL`).Scan(&exists))
			if exists != (stage == "after-commit") {
				t.Fatal("wrong commit outcome")
			}
			if !exists {
				if r, e = PrepareGenerationReadIndex(context.Background(), f.config, q, o); e == nil || r.Complete {
					t.Fatal("old checkpoint recycled")
				}
				o = exportOptions(t)
			}
			r, e = PrepareGenerationReadIndex(context.Background(), f.config, q, o)
			must(t, e)
			if !r.Complete || r.Reconciled != exists {
				t.Fatal(r)
			}
			execSQL(t, f.db, `DROP INDEX public.coldkeep_generation_source_read_v1`)
		})
	}
	if report := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); report != "" {
		data, e := json.Marshal(map[string]any{"definition": generationReadIndexDDL, "definition_sha256": generation.Digest([]byte(generationReadIndexDDL)), "logical_schema": 20, "package": 4})
		must(t, e)
		must(t, os.WriteFile(filepath.Join(report, "generation-index.json"), data, 0600))
	}
}
