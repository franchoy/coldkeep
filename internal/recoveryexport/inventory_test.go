package recoveryexport

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/blocks"
	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

func TestCompatibleFullCatalogCut(t *testing.T) {
	f := newFixture(t)
	// File 2 survives only in snapshot history; file 3 has no current/snapshot
	// root but remains in the full catalog, with its chunk pinned. All 7 recipes,
	// 35 chunks and 8 packed blocks must survive this inventory.
	execSQL(t, f.db, `INSERT INTO snapshot(id,created_at,type) VALUES('history',NOW(),'full')`)
	execSQL(t, f.db, `INSERT INTO snapshot_path(id,path) VALUES(1,'history.txt')`)
	execSQL(t, f.db, `INSERT INTO snapshot_file(id,snapshot_id,path_id,logical_file_id,size) VALUES(1,'history',1,2,4)`)
	execSQL(t, f.db, `DELETE FROM physical_file WHERE logical_file_id IN (2,3)`)
	execSQL(t, f.db, `UPDATE logical_file SET ref_count=0 WHERE id IN (2,3)`)
	execSQL(t, f.db, `UPDATE chunk SET live_ref_count=0 WHERE id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id IN (2,3))`)
	execSQL(t, f.db, `UPDATE chunk SET pin_count=1 WHERE id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id=3)`)
	must(t, os.WriteFile(filepath.Join(f.root, "uncataloged.tmp"), []byte("interrupted bytes outside catalog closure"), 0600))
	keyCalls, passCalls := 0, 0
	f.config.Key = func(context.Context) ([]byte, error) { keyCalls++; return f.key, nil }
	f.config.Password = func(context.Context) (string, error) { passCalls++; return "inventory_fixture_only", nil }
	var retained *Inventory
	r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(ctx context.Context, i *Inventory) error {
		retained = i
		if len(i.Catalog["logical_file"]) != 7 || len(i.Catalog["physical_file"]) != 5 || len(i.Catalog["snapshot_file"]) != 1 || len(i.Catalog["chunk"]) != 35 || len(i.Catalog["storage_blocks"]) != 8 || len(i.Objects) != 1 || !i.KeyRequired {
			t.Fatalf("incomplete relationships: %+v", i)
		}
		if !reflect.DeepEqual(i.Uncataloged, []string{"uncataloged.tmp"}) {
			t.Fatal(i.Uncataloged)
		}
		if i.Objects[0].Size != 465751 || i.Objects[0].RequiredEnd != 465751 {
			t.Fatal(i.Objects)
		}
		id, e := coordination.ResolveIdentity(f.root)
		must(t, e)
		if l, e := coordination.AcquireExisting(ctx, id); !errors.Is(e, coordination.ErrNestedRepositoryAcquisition) {
			if l != nil {
				_ = l.Release()
			}
			t.Fatalf("lease not held: %v", e)
		}
		var out bytes.Buffer
		must(t, i.WriteDiagnostic(&out))
		if !json.Valid(out.Bytes()) {
			t.Fatal("invalid diagnostic JSON")
		}
		if strings.Contains(out.String(), hex.EncodeToString(f.key)) || strings.Contains(out.String(), "inventory_fixture_only") {
			t.Fatal("secret in diagnostic")
		}
		if dest := os.Getenv("COLDKEEP_PHASE1B_REPORT_DIR"); dest != "" {
			must(t, os.WriteFile(filepath.Join(dest, "inventory-example.json"), out.Bytes(), 0600))
		}
		return nil
	})
	must(t, e)
	if !r.Complete || !r.DiagnosticOnly || keyCalls != 1 || passCalls != 1 {
		t.Fatalf("result=%+v key=%d password=%d", r, keyCalls, passCalls)
	}
	// A retained value exposes only diagnostics and cannot reopen or validate a cut.
	must(t, retained.WriteDiagnostic(io.Discard))
	t.Logf("SCENARIO compatible-full-catalog expected=COMPLETE observed=%+v", r)
}

func TestPassiveRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *fixture)
		want   error
	}{
		{"missing-root", func(t *testing.T, f *fixture) { f.config.PayloadRoot = filepath.Join(f.root, "absent") }, ErrConfiguration},
		{"missing-database", func(t *testing.T, f *fixture) { f.config.Database += "_absent" }, nil},
		{"missing-control", func(t *testing.T, f *fixture) { must(t, os.RemoveAll(filepath.Join(f.root, ".coldkeep-control"))) }, coordination.ErrPreparationRequired},
		{"missing-lock", func(t *testing.T, f *fixture) {
			must(t, os.Remove(filepath.Join(f.root, ".coldkeep-control", "repository.lock")))
		}, coordination.ErrPreparationRequired},
		{"wrong-column-type", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "ALTER TABLE container ALTER COLUMN quarantine DROP DEFAULT; ALTER TABLE container ALTER COLUMN quarantine TYPE integer USING quarantine::integer")
		}, ErrUnsupported},
		{"logical-counter", func(t *testing.T, f *fixture) { execSQL(t, f.db, "UPDATE logical_file SET ref_count=99 WHERE id=1") }, ErrCorrupt},
		{"chunk-counter", func(t *testing.T, f *fixture) { execSQL(t, f.db, "UPDATE chunk SET live_ref_count=99 WHERE id=1") }, ErrCorrupt},
		{"future-container-header", func(t *testing.T, f *fixture) {
			p := payloadPath(t, f)
			b, e := os.ReadFile(p)
			must(t, e)
			binary.LittleEndian.PutUint16(b[8:10], 2)
			must(t, os.WriteFile(p, b, 0600))
		}, ErrUnsupported},
		{"future-schema", func(t *testing.T, f *fixture) { execSQL(t, f.db, "UPDATE schema_version SET catalog_version=18") }, ErrUnsupported},
		{"future-catalog-block-with-v1-bytes", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "UPDATE storage_blocks SET format_version=2 WHERE id=1")
		}, ErrUnsupported},
		{"future-encoded-header", func(t *testing.T, f *fixture) {
			p := payloadPath(t, f)
			b, e := os.ReadFile(p)
			must(t, e)
			binary.LittleEndian.PutUint16(b[68:70], 2)
			must(t, os.WriteFile(p, b, 0600))
			execSQL(t, f.db, "UPDATE storage_blocks SET physical_hash=NULL,compressed_hash=NULL WHERE id=1")
		}, ErrUnsupported},
		{"missing-key", func(t *testing.T, f *fixture) { f.config.Key = nil; f.config.KeyReference = "" }, ErrKeyUnavailable},
		{"wrong-key", func(t *testing.T, f *fixture) {
			f.config.Key = func(context.Context) ([]byte, error) { return bytes.Repeat([]byte{1}, 32), nil }
		}, ErrCorrupt},
		{"wrong-root", func(t *testing.T, f *fixture) { must(t, os.Remove(payloadPath(t, f))) }, ErrCorrupt},
		{"corrupt-object", func(t *testing.T, f *fixture) {
			p := payloadPath(t, f)
			b, e := os.ReadFile(p)
			must(t, e)
			b[110] ^= 1
			must(t, os.WriteFile(p, b, 0600))
		}, ErrCorrupt},
		{"truncated-object", func(t *testing.T, f *fixture) { must(t, os.Truncate(payloadPath(t, f), 100)) }, ErrCorrupt},
		{"payload-symlink", func(t *testing.T, f *fixture) {
			p := payloadPath(t, f)
			must(t, os.Rename(p, p+".outside"))
			must(t, os.Symlink(p+".outside", p))
		}, ErrCorrupt},
		{"processing-file", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "UPDATE logical_file SET status='PROCESSING' WHERE id=1")
		}, ErrCorrupt},
		{"recipe-gap", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "UPDATE file_chunk SET chunk_order=900 WHERE logical_file_id=2")
		}, ErrCorrupt},
		{"missing-placement", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "DELETE FROM chunk_block_refs WHERE chunk_id=1")
			execSQL(t, f.db, "DELETE FROM blocks WHERE chunk_id=1")
		}, ErrCorrupt},
		{"incomplete-repair", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, `INSERT INTO store_repair_attempt(id,logical_file_id,source_file_hash,source_total_size,recipe_fingerprint,status) SELECT 1,id,file_hash,total_size,file_hash,'PREPARING' FROM logical_file WHERE id=2`)
		}, ErrCorrupt},
		{"unknown-table", func(t *testing.T, f *fixture) { execSQL(t, f.db, "CREATE TABLE future_reference(id bigint)") }, ErrUnsupported},
		{"unknown-column", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "ALTER TABLE chunk ADD COLUMN future_reference bigint")
		}, ErrUnsupported},
		{"oversized-field", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "UPDATE logical_file SET original_name=repeat('x',1000000) WHERE id=1")
		}, ErrLimit},
		{"oversized-decoded-block", func(t *testing.T, f *fixture) {
			execSQL(t, f.db, "UPDATE storage_blocks SET plaintext_size=1000000000 WHERE id=1")
		}, ErrLimit},
		{"unsupported-backend", func(t *testing.T, f *fixture) { f.config.Backend = "sqlite" }, ErrConfiguration},
		{"ambiguous-config", func(t *testing.T, f *fixture) { f.config.Database = "" }, ErrConfiguration},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			tc.mutate(t, f)
			consumed := false
			r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(context.Context, *Inventory) error { consumed = true; return nil })
			if e == nil || r.Complete || consumed {
				t.Fatalf("admitted refusal: result=%+v error=%v consumed=%v", r, e, consumed)
			}
			if tc.want != nil && !errors.Is(e, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, e)
			}
			t.Logf("SCENARIO %s expected=REFUSED observed=%v complete=%v", tc.name, e, r.Complete)
		})
	}
}
func payloadPath(t *testing.T, f *fixture) string {
	t.Helper()
	var name string
	must(t, f.db.QueryRow("SELECT filename FROM container ORDER BY id LIMIT 1").Scan(&name))
	return filepath.Join(f.root, name)
}

// Named legacy builders follow the already accepted header-0/row recipe. They
// are separate from the immutable retained packed bytes; no format is changed.
func addLegacy(t *testing.T, f *fixture, encrypted bool) {
	data := []byte("phase1b legacy historical recipe")
	payload := data
	nonce := []byte{}
	codec := "plain"
	if encrypted {
		codec = "aes-gcm"
		b, e := (&blocks.AESGCMTransformer{Key: f.key}).Encode(context.Background(), blocks.EncodeInput{Plaintext: data})
		must(t, e)
		payload = b.Payload
		nonce = b.Descriptor.Nonce
	}
	header := make([]byte, 64)
	copy(header, []byte("ColdKeep"))
	binary.LittleEndian.PutUint16(header[10:12], 9)
	binary.LittleEndian.PutUint32(header[12:16], 64)
	binary.LittleEndian.PutUint64(header[28:36], 1<<20)
	binary.LittleEndian.PutUint32(header[52:56], crc32.ChecksumIEEE(header[:52]))
	must(t, os.WriteFile(filepath.Join(f.root, "legacy.bin"), append(header, payload...), 0600))
	hash := fmt.Sprintf("%x", blocks.ComputeBlockHash(data))
	execSQL(t, f.db, "INSERT INTO container(id,filename,current_size,max_size,sealed) VALUES(100,'legacy.bin',$1,1048576,TRUE)", 64+len(payload))
	execSQL(t, f.db, "INSERT INTO chunk(id,chunk_hash,size,status,live_ref_count) VALUES(100,$1,$2,'COMPLETED',0)", hash, len(data))
	execSQL(t, f.db, "INSERT INTO blocks(id,chunk_id,codec,format_version,plaintext_size,stored_size,nonce,container_id,block_offset) VALUES(100,100,$1,1,$2,$3,$4,100,64)", codec, len(data), len(payload), nonce)
	execSQL(t, f.db, "INSERT INTO logical_file(id,original_name,total_size,file_hash,status,ref_count) VALUES(100,'legacy.bin',$1,$2,'COMPLETED',0)", len(data), hash)
	execSQL(t, f.db, "INSERT INTO file_chunk(logical_file_id,chunk_id,chunk_order) VALUES(100,100,0)")
}
func TestLegacyAndPackedInventory(t *testing.T) {
	for _, aes := range []bool{false, true} {
		t.Run(fmt.Sprint("encrypted=", aes), func(t *testing.T) {
			f := newFixture(t)
			addLegacy(t, f, aes)
			r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(_ context.Context, i *Inventory) error {
				if len(i.Objects) != 2 || len(i.Catalog["logical_file"]) != 8 {
					t.Fatal("legacy missing")
				}
				return nil
			})
			must(t, e)
			if !r.Complete {
				t.Fatal(r)
			}
		})
	}
}

func TestFailureClosureAndCancellation(t *testing.T) {
	for _, stage := range []string{"acquired", "database-opened", "snapshot-opened", "row:chunk", "payload-root-opened", "payload-read", "consuming", "snapshot-closed", "database-closed"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t)
			events := []string{}
			injected := errors.New("injected")
			h := &hooks{event: func(s string, _ *sql.Tx) error {
				events = append(events, s)
				// Every callback, including closure callbacks, runs before releasing lease.
				id, e := coordination.ResolveIdentity(f.root)
				must(t, e)
				if l, e := coordination.AcquireExisting(context.Background(), id); !errors.Is(e, coordination.ErrNestedRepositoryAcquisition) {
					if l != nil {
						_ = l.Release()
					}
					t.Fatalf("released before %s: %v", s, e)
				}
				if s == stage {
					return injected
				}
				return nil
			}}
			r, e := runPreserved(t, f, context.Background(), DefaultLimits(), h, noConsume)
			if !errors.Is(e, injected) || r.Complete {
				t.Fatalf("partial success: %+v %v", r, e)
			}
			id, e := coordination.ResolveIdentity(f.root)
			must(t, e)
			l, e := coordination.AcquireExisting(context.Background(), id)
			must(t, e)
			must(t, l.Release())
			if stage != "acquired" && events[len(events)-1] != "database-closed" {
				t.Fatal(events)
			}
			t.Logf("SCENARIO failure-%s expected=INCOMPLETE_CLOSED observed=INCOMPLETE_CLOSED events=%v", stage, events)
		})
	}
	t.Run("cancel-row", func(t *testing.T) {
		f := newFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		n := 0
		h := &hooks{event: func(s string, _ *sql.Tx) error {
			if s == "row:chunk" {
				n++
				if n == 4 {
					cancel()
				}
			}
			return nil
		}}
		r, e := runPreserved(t, f, ctx, DefaultLimits(), h, noConsume)
		if !errors.Is(e, context.Canceled) || r.Complete || n != 4 {
			t.Fatalf("%+v %v rows=%d", r, e, n)
		}
	})
	t.Run("cancel-consumer", func(t *testing.T) {
		f := newFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r, e := runPreserved(t, f, ctx, DefaultLimits(), nil, func(context.Context, *Inventory) error { cancel(); return nil })
		if !errors.Is(e, context.Canceled) || r.Complete {
			t.Fatalf("%+v %v", r, e)
		}
	})
}

func TestReadOnlySnapshotAndAmbientIsolation(t *testing.T) {
	f := newFixture(t)
	var held *sql.Tx
	originalRoot := f.root
	h := &hooks{event: func(stage string, tx *sql.Tx) error {
		if stage == "snapshot-opened" {
			held = tx
			var before int
			must(t, tx.QueryRow("SELECT count(*) FROM public.logical_file").Scan(&before))
			execSQL(t, f.db, "UPDATE logical_file SET original_name='external writer outside cooperative contract' WHERE id=1")
			var observed string
			must(t, tx.QueryRow("SELECT original_name FROM public.logical_file WHERE id=1").Scan(&observed))
			if observed == "external writer outside cooperative contract" {
				t.Fatal("snapshot was not repeatable")
			}
			// Config already captured; later process values cannot redirect any adapter.
			t.Setenv("PGDATABASE", "wrong")
			t.Setenv("PGPASSWORD", "wrong")
			t.Setenv("COLDKEEP_KEY", "wrong")
			t.Setenv("COLDKEEP_CONTAINER_DIR", filepath.Join(f.root, "second-root"))
		}
		return nil
	}}
	r, e := inspect(context.Background(), f.config, DefaultLimits(), noConsume, h)
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
	if _, e = held.Exec("SELECT 1"); !errors.Is(e, sql.ErrTxDone) {
		t.Fatalf("snapshot survived: %v", e)
	}
	if _, e = os.Stat(filepath.Join(originalRoot, "second-root")); !os.IsNotExist(e) {
		t.Fatal("hidden second root")
	}
}
func TestReadOnlyWriteAttemptAndQueryFailure(t *testing.T) {
	for _, query := range []string{"UPDATE public.chunk SET pin_count=pin_count+1", "CREATE TABLE public.silent_bootstrap(id int)", "SELECT * FROM public.does_not_exist"} {
		t.Run(query, func(t *testing.T) {
			f := newFixture(t)
			h := &hooks{event: func(s string, tx *sql.Tx) error {
				if s == "snapshot-opened" {
					_, e := tx.Exec(query)
					if e == nil {
						t.Fatal("write/failing query accepted")
					}
					return e
				}
				return nil
			}}
			r, e := runPreserved(t, f, context.Background(), DefaultLimits(), h, noConsume)
			if e == nil || r.Complete {
				t.Fatalf("%+v %v", r, e)
			}
		})
	}
}

func TestExecutionBounds(t *testing.T) {
	f := newFixture(t)
	// Thousands of empty-file historical memberships exercise database iteration
	// without creating fake payload objects or materializing an unbounded query.
	execSQL(t, f.db, `INSERT INTO snapshot(id,created_at,type) VALUES('large-history',NOW(),'full')`)
	execSQL(t, f.db, `INSERT INTO snapshot_path(id,path) SELECT 1000+n,'empty-history-'||n FROM generate_series(1,6000) n`)
	execSQL(t, f.db, `INSERT INTO snapshot_file(id,snapshot_id,path_id,logical_file_id,size) SELECT 1000+n,'large-history',1000+n,1,0 FROM generate_series(1,6000) n`)
	l := DefaultLimits()
	l.Rows = 500
	seen := 0
	h := &hooks{event: func(s string, _ *sql.Tx) error {
		if strings.HasPrefix(s, "row:") {
			seen++
		}
		return nil
	}}
	start := time.Now()
	r, e := runPreserved(t, f, context.Background(), l, h, noConsume)
	if !errors.Is(e, ErrLimit) || r.Complete || seen != 500 {
		t.Fatalf("unbounded: %+v %v rows=%d", r, e, seen)
	}
	t.Logf("SCENARIO row-cap expected=500_DELIVERED_INCOMPLETE observed=%d elapsed=%s", seen, time.Since(start))
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	r, e = runPreserved(t, f, context.Background(), DefaultLimits(), nil, noConsume)
	runtime.ReadMemStats(&memAfter)
	must(t, e)
	if !r.Complete || r.Rows < 6000 || r.Objects != 1 {
		t.Fatal(r)
	}
	t.Logf("SCENARIO bounded-large expected=COMPLETE_6000_HISTORY_ONE_OBJECT observed=%+v total_alloc_delta=%d", r, memAfter.TotalAlloc-memBefore.TotalAlloc)
	for _, kind := range []string{"catalog-bytes", "diagnostic", "block", "object", "total-object", "directory", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			l := DefaultLimits()
			switch kind {
			case "catalog-bytes":
				l.CatalogBytes = 100
			case "diagnostic":
				l.DiagnosticBytes = 100
			case "block":
				l.BlockBytes = 32
			case "object":
				l.ObjectBytes = 100
			case "total-object":
				l.TotalObjectBytes = 100
			case "directory":
				l.DirectoryEntries = 1
			case "deadline":
				l.Duration = time.Nanosecond
			}
			r, e := runPreserved(t, f, context.Background(), l, nil, noConsume)
			if e == nil || r.Complete {
				t.Fatalf("bound bypass: %+v %v", r, e)
			}
		})
	}
}

func TestRepairAndRetiredPhysicalClosure(t *testing.T) {
	f := newFixture(t)
	execSQL(t, f.db, `INSERT INTO store_repair_attempt(id,logical_file_id,source_file_hash,source_total_size,recipe_fingerprint,status)
 SELECT 1,id,file_hash,total_size,file_hash,'PUBLISHED' FROM logical_file WHERE id=2`)
	p := payloadPath(t, f)
	data, e := os.ReadFile(p)
	must(t, e)
	digest := fmt.Sprintf("%x", blocks.ComputeBlockHash(data))
	execSQL(t, f.db, `INSERT INTO store_repair_container(attempt_id,container_id,physical_size,physical_hash,status) VALUES(1,1,$1,$2,'PUBLISHED')`, len(data), digest)
	execSQL(t, f.db, `INSERT INTO store_repair_block(attempt_id,block_ordinal,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,container_id,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash)
 SELECT 1,0,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,container_id,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash FROM storage_blocks WHERE id=1`)
	execSQL(t, f.db, `INSERT INTO store_repair_chunk(attempt_id,chunk_id,chunk_order,block_ordinal,offset_in_block,size_in_block) VALUES(1,1,0,0,0,4)`)
	must(t, os.WriteFile(filepath.Join(f.root, "retired-packed.bin"), data, 0600))
	execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) SELECT 200,'retired-packed.bin',current_size,max_size FROM container WHERE id=1`)
	execSQL(t, f.db, `INSERT INTO storage_blocks(id,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,container_id,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash)
 SELECT 200,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,200,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash FROM storage_blocks WHERE id=1`)
	execSQL(t, f.db, `INSERT INTO retired_chunk_block_ref(block_id,embedded_chunk_id,offset_in_block,size_in_block,repair_attempt_id) VALUES(200,1,0,4,1)`)
	addLegacy(t, f, true)
	legacy, e := os.ReadFile(filepath.Join(f.root, "legacy.bin"))
	must(t, e)
	must(t, os.WriteFile(filepath.Join(f.root, "retired-legacy.bin"), legacy, 0600))
	execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) SELECT 201,'retired-legacy.bin',current_size,max_size FROM container WHERE id=100`)
	execSQL(t, f.db, `INSERT INTO retired_legacy_block_extent(container_id,block_offset,stored_size,plaintext_size,codec,format_version,nonce,historical_block_id,historical_chunk_id,repair_attempt_id)
 SELECT 201,block_offset,stored_size,plaintext_size,codec,format_version,nonce,199,199,1 FROM blocks WHERE id=100`)
	r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(_ context.Context, i *Inventory) error {
		if len(i.Objects) != 4 || len(i.Catalog["retired_chunk_block_ref"]) != 1 || len(i.Catalog["retired_legacy_block_extent"]) != 1 {
			t.Fatal("retired physical closure lost")
		}
		return nil
	})
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
	t.Logf("SCENARIO repair-retired-closure expected=FOUR_WHOLE_OBJECTS observed=%+v", r)
}

func TestClosedResourcesAndLostLeaseNeverComplete(t *testing.T) {
	f := newFixture(t)
	var txHeld *sql.Tx
	r, e := inspect(context.Background(), f.config, DefaultLimits(), noConsume, &hooks{event: func(s string, tx *sql.Tx) error {
		if s == "snapshot-opened" {
			txHeld = tx
		}
		return nil
	}})
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
	if _, e = txHeld.Exec("SELECT 1"); !errors.Is(e, sql.ErrTxDone) {
		t.Fatal("transaction survived")
	}
	var sessions int
	must(t, f.db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='coldkeep-passive-inventory'").Scan(&sessions))
	if sessions != 0 {
		t.Fatalf("surviving sessions: %d", sessions)
	}
	lock := filepath.Join(f.root, ".coldkeep-control", "repository.lock")
	r, e = Inspect(context.Background(), f.config, DefaultLimits(), func(context.Context, *Inventory) error {
		must(t, os.Rename(lock, lock+".original"))
		must(t, os.WriteFile(lock, nil, 0600))
		return nil
	})
	if r.Complete || !errors.Is(e, coordination.ErrOwnershipLost) {
		t.Fatalf("lost lease produced success: %+v %v", r, e)
	}
}

func TestPlaintextWithoutKeyAndMixedRecipe(t *testing.T) {
	t.Run("packed-plain-no-key", func(t *testing.T) {
		f := newFixture(t)
		for _, q := range []string{"DELETE FROM physical_file WHERE logical_file_id>2", "DELETE FROM file_chunk WHERE logical_file_id>2", "DELETE FROM logical_file WHERE id>2", "DELETE FROM blocks WHERE chunk_id>1", "DELETE FROM chunk_block_refs WHERE chunk_id>1", "DELETE FROM chunk WHERE id>1", "DELETE FROM storage_blocks WHERE id>1"} {
			execSQL(t, f.db, q)
		}
		f.config.Key = nil
		f.config.KeyReference = ""
		r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(_ context.Context, i *Inventory) error {
			if i.KeyRequired || len(i.Objects) != 1 || len(i.Catalog["logical_file"]) != 2 {
				t.Fatal("plaintext closure mismatch")
			}
			return nil
		})
		must(t, e)
		if !r.Complete {
			t.Fatal(r)
		}
	})
	t.Run("mixed-recipe", func(t *testing.T) {
		f := newFixture(t)
		addLegacy(t, f, false)
		data := []byte("phase1b legacy historical recipetiny")
		execSQL(t, f.db, "UPDATE logical_file SET total_size=$1,file_hash=$2 WHERE id=100", len(data), fmt.Sprintf("%x", blocks.ComputeBlockHash(data)))
		execSQL(t, f.db, "INSERT INTO file_chunk(logical_file_id,chunk_id,chunk_order) VALUES(100,1,1)")
		execSQL(t, f.db, "UPDATE chunk SET live_ref_count=1 WHERE id=1")
		r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, noConsume)
		must(t, e)
		if !r.Complete {
			t.Fatal(r)
		}
	})
}

func TestPinOnlyAndUnreferencedContainer(t *testing.T) {
	f := newFixture(t)
	execSQL(t, f.db, "DELETE FROM physical_file WHERE logical_file_id=3")
	execSQL(t, f.db, "DELETE FROM file_chunk WHERE logical_file_id=3")
	execSQL(t, f.db, "DELETE FROM logical_file WHERE id=3")
	execSQL(t, f.db, "UPDATE chunk SET pin_count=1,live_ref_count=0 WHERE id=2")
	original, e := os.ReadFile(payloadPath(t, f))
	must(t, e)
	must(t, os.WriteFile(filepath.Join(f.root, "catalog-orphan.bin"), original[:64], 0600))
	execSQL(t, f.db, "INSERT INTO container(id,filename,current_size,max_size) VALUES(999,'catalog-orphan.bin',64,67108864)")
	r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(_ context.Context, i *Inventory) error {
		if len(i.Objects) != 2 || len(i.Catalog["chunk"]) != 35 || len(i.Catalog["file_chunk"]) != 34 {
			t.Fatal("pin-only/full-catalog object omitted")
		}
		return nil
	})
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
	t.Logf("SCENARIO pin-only-and-catalog-orphan expected=TWO_OBJECTS_35_CHUNKS observed=%+v", r)
}
