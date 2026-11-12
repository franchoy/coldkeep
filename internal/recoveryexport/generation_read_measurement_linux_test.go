package recoveryexport

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/generation"
)

type measuredReadQuery struct {
	SQL  string
	Args []any
}
type measuredReadPage struct {
	Query, Position                                string
	DataQueries, ReservationQueries                int
	Selected, Distractors, Returned, ResponseBytes int
	AllocatedBytes, Allocations                    uint64
	ElapsedNS                                      int64
	Plans                                          []json.RawMessage
}

func TestRecoveryReadQueryWork(t *testing.T) {
	f, b, _, sources, gens := generationFixture(t)
	q := generationReadRequest(t, f, b)
	prepareReadFixture(t, f, q)
	parent := fixedEntryID(700000)
	insertGenerationEntry(t, f, gens[0], parent, nil, []byte("parent"), "directory", "stored", nil)
	var pages []measuredReadPage
	var openings []map[string]any
	previous := 0
	for _, size := range []int{50, 500, 2500} {
		t.Run(fmt.Sprintf("records-%d", 2*size), func(t *testing.T) {
			for sourceIndex := range 2 {
				execSQL(t, f.db, `INSERT INTO capture_generation SELECT 'generation_'||lpad(to_hex(n+$4),32,'0'),g.repository_id,g.source_id,g.source_revision,g.policy_id,g.policy_sha256,1,2,'enumerated','incomplete',NULL FROM capture_generation g CROSS JOIN generate_series($1::integer,$2::integer) n WHERE g.generation_id=$3`, previous+1, size, gens[sourceIndex], sourceIndex*100000)
			}
			execSQL(t, f.db, `INSERT INTO generation_root SELECT generation_id,NULL,NULL FROM capture_generation g WHERE NOT EXISTS(SELECT 1 FROM generation_root r WHERE r.generation_id=g.generation_id)`)
			execSQL(t, f.db, `INSERT INTO generation_entry SELECT $1,'entry_'||lpad(to_hex(n),32,'0'),NULL,'directory',convert_to(lpad(n::text,6,'0'),'UTF8'),'directory','stored',NULL,NULL,NULL FROM generate_series($2::integer,$3::integer) n`, gens[0], previous+1, size)
			execSQL(t, f.db, `INSERT INTO generation_entry SELECT $1,'entry_'||lpad(to_hex(n+100000),32,'0'),$4,'directory',convert_to(lpad(n::text,6,'0'),'UTF8'),'directory','stored',NULL,NULL,NULL FROM generate_series($2::integer,$3::integer) n`, gens[0], previous+1, size, parent)
			execSQL(t, f.db, `ANALYZE public.capture_generation`)
			execSQL(t, f.db, `ANALYZE public.generation_entry`)
			var captured []measuredReadQuery
			h := &generationReadHooks{query: func(query string, args []any) {
				captured = append(captured, measuredReadQuery{query, append([]any(nil), args...)})
			}}
			var before runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			result, e := withGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
				var opened runtime.MemStats
				runtime.ReadMemStats(&opened)
				openings = append(openings, map[string]any{"selected": size, "distractors": size, "total_alloc_before": before.TotalAlloc, "total_alloc_at_callback": opened.TotalAlloc, "heap_before": before.HeapAlloc, "heap_at_callback": opened.HeapAlloc})
				for _, query := range []string{generation.QueryGenerations, generation.QueryChildren, "children-nonroot"} {
					request := readRequest(q, sources[0], gens[0], query)
					expectedCount := size + 1
					if query == "children-nonroot" {
						request.Query = generation.QueryChildren
						request.ParentID = &parent
						expectedCount = size
					}
					if query == generation.QueryGenerations {
						request.GenerationID = ""
					}
					request.Limit = 100
					pageNumber := 0
					seen := map[string]bool{}
					count := 0
					for {
						captured = nil
						var start, end runtime.MemStats
						runtime.ReadMemStats(&start)
						at := time.Now()
						response, e := reader.Read(ctx, request)
						elapsed := time.Since(at)
						runtime.ReadMemStats(&end)
						if e != nil {
							return e
						}
						serialized, e := generation.EncodeResponse(response)
						if e != nil {
							return e
						}
						n := len(response.Entries) + len(response.Generations)
						for _, g := range response.Generations {
							if seen[g.ID] {
								t.Fatal("duplicate scaled generation")
							}
							seen[g.ID] = true
						}
						for _, entry := range response.Entries {
							if seen[entry.ID] {
								t.Fatal("duplicate scaled entry")
							}
							seen[entry.ID] = true
						}
						count += n
						position := ""
						switch {
						case pageNumber == 0:
							position = "first"
						case response.NextCursor == "":
							position = "final"
						case pageNumber == size/200:
							position = "middle"
						}
						if position != "" {
							m := measuredReadPage{DataQueries: len(captured), ReservationQueries: 2, Query: query, Position: position, Selected: size, Distractors: size, Returned: n, ResponseBytes: len(serialized), AllocatedBytes: end.TotalAlloc - start.TotalAlloc, Allocations: end.Mallocs - start.Mallocs, ElapsedNS: elapsed.Nanoseconds()}
							for _, observed := range captured {
								var plan []byte
								must(t, f.db.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+observed.SQL, observed.Args...).Scan(&plan))
								m.Plans = append(m.Plans, json.RawMessage(plan))
								if size == 2500 && strings.Contains(observed.SQL, "ORDER BY") && strings.Contains(observed.SQL, "LIMIT") {
									assertReadPlanBound(t, plan)
								}
							}
							pages = append(pages, m)
						}
						if response.NextCursor == "" {
							break
						}
						request.Cursor = response.NextCursor
						pageNumber++
					}
					if count != expectedCount {
						t.Fatal("scaled enumeration differs", count, expectedCount)
					}
					// Limit-one cost is comparable across sizes and independent of the
					// total admitted catalog; the opening inventory remains separately measured.
					request.Cursor = ""
					request.Limit = 1
					captured = nil
					var start, end runtime.MemStats
					runtime.ReadMemStats(&start)
					at := time.Now()
					response, e := reader.Read(ctx, request)
					elapsed := time.Since(at)
					runtime.ReadMemStats(&end)
					if e != nil {
						return e
					}
					raw, e := generation.EncodeResponse(response)
					if e != nil {
						return e
					}
					pages = append(pages, measuredReadPage{DataQueries: len(captured), ReservationQueries: 2, Query: query, Position: "limit-one", Selected: size, Distractors: size, Returned: 1, ResponseBytes: len(raw), AllocatedBytes: end.TotalAlloc - start.TotalAlloc, Allocations: end.Mallocs - start.Mallocs, ElapsedNS: elapsed.Nanoseconds()})
				}
				for _, index := range []int{1, size / 2, size} {
					entryRequest := readRequest(q, sources[0], gens[0], generation.QueryEntry)
					entryRequest.EntryID = fixedEntryID(index)
					for _, query := range []string{generation.QueryEntry, generation.QuerySelector} {
						captured = nil
						var start, end runtime.MemStats
						runtime.ReadMemStats(&start)
						at := time.Now()
						response, err := reader.Read(ctx, entryRequest)
						elapsed := time.Since(at)
						runtime.ReadMemStats(&end)
						if err != nil {
							return err
						}
						raw, err := generation.EncodeResponse(response)
						if err != nil {
							return err
						}
						m := measuredReadPage{DataQueries: len(captured), ReservationQueries: 2, Query: query, Position: fmt.Sprintf("entry-%d", index), Selected: size, Distractors: size, Returned: 1, ResponseBytes: len(raw), AllocatedBytes: end.TotalAlloc - start.TotalAlloc, Allocations: end.Mallocs - start.Mallocs, ElapsedNS: elapsed.Nanoseconds()}
						for _, observed := range captured {
							var plan []byte
							must(t, f.db.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+observed.SQL, observed.Args...).Scan(&plan))
							m.Plans = append(m.Plans, json.RawMessage(plan))
							if size == 2500 {
								assertReadPlanBound(t, plan)
							}
						}
						pages = append(pages, m)
						entryRequest.Query = generation.QuerySelector
						entryRequest.EntryID = ""
						entryRequest.Selector = &response.Selection.Selector
					}
				}
				return nil
			}, h)
			must(t, e)
			if !result.Complete {
				t.Fatal(result)
			}
			openings[len(openings)-1]["result"] = result
			previous = size
		})
	}
	for _, query := range []string{generation.QueryGenerations, generation.QueryChildren, "children-nonroot"} {
		var first, last uint64
		for _, p := range pages {
			if p.Query == query && p.Position == "limit-one" {
				if first == 0 {
					first = p.AllocatedBytes
				}
				last = p.AllocatedBytes
			}
		}
		if last > first*3+65536 {
			t.Fatalf("per-page allocation scaled with unrelated rows: %s %d -> %d", query, first, last)
		}
	}
	if report := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); report != "" {
		raw, e := json.MarshalIndent(map[string]any{"profile": "installed Linux/PostgreSQL16 schema20; standard planner settings", "fixture": "50/500/2500 selected generations plus same-count source distractors; same-count root and other-parent entries; original fixture rows retained", "pages": pages, "openings": openings, "timing": "observations, not latency guarantees", "query_accounting": "Plans capture parameterized data queries only. Each successful Read also performs two inherited reservation-check SELECT EXISTS queries over pg_locks on its separate non-data connection. Their time/allocation is included in Read metrics; their global-lock work is not qualified as indexed data-page work."}, "", "  ")
		must(t, e)
		must(t, os.WriteFile(filepath.Join(report, "generation-read-query-work.json"), raw, 0600))
	}
}

func assertReadPlanBound(t *testing.T, raw []byte) {
	t.Helper()
	var plans []struct{ Plan map[string]any }
	must(t, json.Unmarshal(raw, &plans))
	if len(plans) != 1 {
		t.Fatal("missing EXPLAIN plan")
	}
	var walk func(map[string]any)
	walk = func(p map[string]any) {
		rows, _ := p["Actual Rows"].(float64)
		removed, _ := p["Rows Removed by Filter"].(float64)
		loops, _ := p["Actual Loops"].(float64)
		if (rows+removed)*loops > 202 {
			t.Fatalf("largest-profile page scanned unrelated scope: %s", raw)
		}
		children, _ := p["Plans"].([]any)
		for _, child := range children {
			node, ok := child.(map[string]any)
			if !ok {
				t.Fatal("invalid plan node")
			}
			walk(node)
		}
	}
	walk(plans[0].Plan)
}
