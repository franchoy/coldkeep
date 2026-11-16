//go:build linux

package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

func exportFixture(t *testing.T) *fixture {
	t.Helper()
	if os.Getenv("COLDKEEP_PHASE1C_TEST") != "1" {
		t.Skip("requires explicit Phase 1C disposable PostgreSQL and client tools")
	}
	f := newFixture(t)
	// Explicit-ID fixture import does not advance PostgreSQL sequences. Establish
	// deliberate valid values during setup, never during export or restore.
	rows, e := f.db.Query(`SELECT c.relname,a.attname,pg_get_serial_sequence('public.'||quote_ident(c.relname),a.attname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid WHERE n.nspname='public' AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped`)
	must(t, e)
	type sequence struct{ table, column, name string }
	var seqs []sequence
	for rows.Next() {
		var s sequence
		var name sql.NullString
		must(t, rows.Scan(&s.table, &s.column, &name))
		if name.Valid {
			s.name = name.String
			seqs = append(seqs, s)
		}
	}
	must(t, rows.Err())
	must(t, rows.Close())
	for _, s := range seqs {
		var maximum int64
		must(t, f.db.QueryRow("SELECT COALESCE(max("+pqIdent(s.column)+"),0)+10000 FROM public."+pqIdent(s.table)).Scan(&maximum))
		execSQL(t, f.db, "SELECT setval($1::regclass,$2,true)", s.name, maximum)
	}
	execSQL(t, f.db, "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO "+pqIdent(f.config.User))
	return f
}
func exportOptions(t *testing.T) ExportOptions {
	t.Helper()
	return ExportOptions{Destination: filepath.Join(t.TempDir(), "generation"), Tools: Tools{os.Getenv("COLDKEEP_PG_DUMP"), os.Getenv("COLDKEEP_PG_RESTORE")}, InventoryLimits: DefaultLimits(), Limits: DefaultExportLimits()}
}
func exportPreserved(t *testing.T, f *fixture, o ExportOptions, h *exportHooks) (ExportResult, error) {
	t.Helper()
	catalog, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
	schema := metadataSemantics(t, f.db)
	result, e := export(context.Background(), f.config, o, h)
	if catalogFingerprint(t, f.db) != catalog || treeFingerprint(t, f.root) != tree || metadataSemantics(t, f.db) != schema {
		t.Fatal("export mutated source")
	}
	assertNoExportSessions(t, f)
	return result, e
}
func assertNoExportSessions(t *testing.T, f *fixture) {
	t.Helper()
	assertNoInventorySessions(t, f.db)
	var n int
	must(t, f.db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='coldkeep-recovery-dump'").Scan(&n))
	if n != 0 {
		t.Fatalf("surviving pg_dump sessions: %d", n)
	}
}
func metadataSemantics(t *testing.T, conn *sql.DB) string {
	t.Helper()
	var out strings.Builder
	for _, spec := range catalogTables {
		rows, e := conn.Query("SELECT row_to_json(t)::text FROM public." + pqIdent(spec.name) + " t ORDER BY row_to_json(t)::text")
		must(t, e)
		for rows.Next() {
			var s string
			must(t, rows.Scan(&s))
			fmt.Fprintln(&out, s)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	for _, query := range []string{
		`SELECT sequencename||':'||data_type||':'||start_value||':'||min_value||':'||max_value||':'||increment_by||':'||cycle||':'||cache_size FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename`,
		`SELECT conname||':'||pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace='public'::regnamespace ORDER BY conname`,
		`SELECT indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY indexname`,
		`SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' ORDER BY proname`,
		`SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal ORDER BY tgname`,
	} {
		rows, e := conn.Query(query)
		must(t, e)
		for rows.Next() {
			var s string
			must(t, rows.Scan(&s))
			fmt.Fprintln(&out, s)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	rows, e := conn.Query("SELECT sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename")
	must(t, e)
	var names []string
	for rows.Next() {
		var name string
		must(t, rows.Scan(&name))
		names = append(names, name)
	}
	must(t, rows.Close())
	for _, name := range names {
		var last int64
		var called bool
		must(t, conn.QueryRow("SELECT last_value,is_called FROM public."+pqIdent(name)).Scan(&last, &called))
		fmt.Fprintln(&out, name, last, called)
	}
	return out.String()
}
func restoreMetadata(t *testing.T, f *fixture, o ExportOptions) *sql.DB {
	t.Helper()
	name := f.config.Database + "_restore"
	adminDSN, e := db.BuildPostgresConnStringFromEnv("postgres")
	must(t, e)
	admin, e := sql.Open("postgres", adminDSN)
	must(t, e)
	execSQL(t, admin, "CREATE DATABASE "+pqIdent(name)+" OWNER "+pqIdent(f.config.User)+" TEMPLATE template0")
	dsn, e := db.BuildPostgresConnStringFromEnv(name)
	must(t, e)
	restored, e := sql.Open("postgres", dsn)
	must(t, e)
	t.Cleanup(func() {
		must(t, restored.Close())
		execSQL(t, admin, "DROP DATABASE "+pqIdent(name)+" WITH (FORCE)")
		must(t, admin.Close())
	})
	r, e := resolve(context.Background(), f.config)
	must(t, e)
	defer clear(r.key)
	r.pg.Database = name
	env, remove, e := childCredentials(r, o.Destination)
	must(t, e)
	defer func() { must(t, remove()) }()
	var clean []string
	for _, v := range env {
		if !strings.HasPrefix(v, "PGOPTIONS=") {
			clean = append(clean, v)
		}
	}
	args := []string{"--no-owner", "--no-acl", "--exit-on-error", "--host=" + r.pg.Host, fmt.Sprint("--port=", r.pg.Port), "--username=" + r.pg.User, "--dbname=" + name, filepath.Join(o.Destination, "catalog.dump")}
	must(t, runChild(context.Background(), o.Tools.PGRestore, args, clean, nil, &strings.Builder{}, 4096, nil))
	var super bool
	must(t, restored.QueryRow("SELECT rolsuper FROM pg_roles WHERE rolname=$1", f.config.User).Scan(&super))
	if super {
		t.Fatal("restore role was superuser")
	}
	return restored
}
func TestRecoveryExportRoundTrip(t *testing.T) {
	for _, plain := range []bool{false, true} {
		t.Run(fmt.Sprint("plaintext=", plain), func(t *testing.T) {
			f := exportFixture(t)
			if plain {
				for _, q := range []string{"DELETE FROM physical_file WHERE logical_file_id>2", "DELETE FROM file_chunk WHERE logical_file_id>2", "DELETE FROM logical_file WHERE id>2", "DELETE FROM blocks WHERE chunk_id>1", "DELETE FROM chunk_block_refs WHERE chunk_id>1", "DELETE FROM storage_blocks WHERE id>1", "DELETE FROM chunk WHERE id>1"} {
					execSQL(t, f.db, q)
				}
				f.config.Key = nil
				f.config.KeyReference = ""
			}
			addLegacy(t, f, !plain)
			execSQL(t, f.db, `INSERT INTO snapshot(id,created_at,type) VALUES('export-history',NOW(),'full'); INSERT INTO snapshot_path(id,path) VALUES(90,'history/file.txt'); INSERT INTO snapshot_file(id,snapshot_id,path_id,logical_file_id,size) VALUES(90,'export-history',90,2,4); DELETE FROM physical_file WHERE logical_file_id=2; UPDATE logical_file SET ref_count=0 WHERE id=2; UPDATE chunk SET live_ref_count=0,pin_count=1 WHERE id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id=2)`)
			before := metadataSemantics(t, f.db)
			o := exportOptions(t)
			changed := watchMutations(t, f.root)
			result, e := exportPreserved(t, f, o, nil)
			must(t, e)
			if !result.Complete || !result.Published || changed() {
				t.Fatalf("result=%+v mutation=%v", result, changed())
			}
			check, e := Check(context.Background(), o.Destination, o.Limits)
			must(t, e)
			if !check.ByteComplete || check.Manifest.KeyRequired == plain || len(check.Manifest.Members) != 3 {
				t.Fatalf("requirements/closure: %+v", check)
			}
			for _, m := range check.Manifest.Members[1:] {
				src, e := os.Stat(filepath.Join(f.root, m.SourceName))
				must(t, e)
				dst, e := os.Stat(filepath.Join(o.Destination, m.Name))
				must(t, e)
				if os.SameFile(src, dst) {
					t.Fatal("payload copy aliases source")
				}
			}
			restored := restoreMetadata(t, f, o)
			if got := metadataSemantics(t, restored); got != before {
				t.Fatalf("metadata round trip differs\nBEFORE %s\nAFTER %s", before, got)
			}
			var last, next int64
			must(t, restored.QueryRow("SELECT last_value FROM public.logical_file_id_seq").Scan(&last))
			must(t, restored.QueryRow("SELECT nextval('public.logical_file_id_seq')").Scan(&next))
			if next != last+1 {
				t.Fatal("sequence next-ID mismatch")
			}
			if _, e = restored.Exec("INSERT INTO snapshot_file(snapshot_id,path_id,logical_file_id) VALUES('absent',90,2)"); e == nil {
				t.Fatal("restored foreign key absent")
			}
			t.Logf("SCENARIO recovery roundtrip plaintext=%v full tables=19 members=%d nextID=%d non-superuser=true", plain, len(check.Manifest.Members), next)
			if report := os.Getenv("COLDKEEP_PHASE1C_REPORT_DIR"); report != "" {
				b, e := json.MarshalIndent(check, "", "  ")
				must(t, e)
				must(t, os.WriteFile(filepath.Join(report, fmt.Sprintf("package-plain-%v.json", plain)), b, 0600))
			}
		})
	}
}
func TestRecoveryExportSameSnapshot(t *testing.T) {
	f := exportFixture(t)
	o := exportOptions(t)
	before := metadataSemantics(t, f.db)
	changed := false
	h := &exportHooks{event: func(stage string) error {
		if stage == "snapshot-exported" {
			execSQL(t, f.db, "UPDATE logical_file SET original_name='external-after-snapshot' WHERE id=2")
			changed = true
		}
		return nil
	}}
	result, e := export(context.Background(), f.config, o, h)
	must(t, e)
	if !result.Complete || !changed {
		t.Fatal(result)
	}
	restored := restoreMetadata(t, f, o)
	if metadataSemantics(t, restored) != before {
		t.Fatal("dump did not import inventory snapshot")
	}
	t.Log("SCENARIO external table commit after snapshot: restored archive retains earlier value; deliberate external fixture mutation excluded from passive comparison")
}
func TestRecoveryExportStages(t *testing.T) {
	for _, stage := range []string{"archive-written", "payload-copy", "payloads-written", "staged-verified", "source-cleaned", "before-publication", "published"} {
		t.Run(stage, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			injected := errors.New("injected stage failure")
			result, e := exportPreserved(t, f, o, &exportHooks{event: func(s string) error {
				if s == stage {
					return injected
				}
				return nil
			}})
			if !errors.Is(e, injected) || result.Complete {
				t.Fatalf("result=%+v error=%v", result, e)
			}
			check, checkErr := Check(context.Background(), o.Destination, o.Limits)
			if stage == "published" {
				if !result.Published || checkErr != nil || !check.ByteComplete {
					t.Fatalf("lost publication acknowledgement: %+v %v", result, checkErr)
				}
			} else {
				if result.Published || checkErr == nil {
					t.Fatalf("early completion: %+v %v", result, checkErr)
				}
			}
			t.Logf("SCENARIO interruption=%s published=%v checkerAccepted=%v", stage, result.Published, checkErr == nil)
		})
	}
}

func TestRecoveryHistoricalClosure(t *testing.T) {
	f := exportFixture(t)
	execSQL(t, f.db, `INSERT INTO store_repair_attempt(id,logical_file_id,source_file_hash,source_total_size,recipe_fingerprint,status)
 SELECT 1,id,file_hash,total_size,file_hash,'PUBLISHED' FROM logical_file WHERE id=2`)
	p := payloadPath(t, f)
	data, e := os.ReadFile(p)
	must(t, e)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
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
	execSQL(t, f.db, "DELETE FROM physical_file WHERE logical_file_id=3; DELETE FROM file_chunk WHERE logical_file_id=3; DELETE FROM logical_file WHERE id=3; UPDATE chunk SET pin_count=1,live_ref_count=0 WHERE id=2")
	must(t, os.WriteFile(filepath.Join(f.root, "catalog-orphan.bin"), data[:64], 0600))
	execSQL(t, f.db, "INSERT INTO container(id,filename,current_size,max_size) VALUES(999,'catalog-orphan.bin',64,67108864)")
	execSQL(t, f.db, `INSERT INTO snapshot(id,created_at,type) VALUES('history',NOW(),'full'); INSERT INTO snapshot_path(id,path) VALUES(90,'history/original-name'); INSERT INTO snapshot_file(id,snapshot_id,path_id,logical_file_id,size) VALUES(90,'history',90,2,4); DELETE FROM physical_file WHERE logical_file_id=2; UPDATE logical_file SET ref_count=0 WHERE id=2; UPDATE chunk SET live_ref_count=0 WHERE id=1`)
	before := metadataSemantics(t, f.db)
	o := exportOptions(t)
	r, e := exportPreserved(t, f, o, nil)
	must(t, e)
	c, e := Check(context.Background(), o.Destination, o.Limits)
	must(t, e)
	if !r.Complete || len(c.Manifest.Members) != 6 {
		t.Fatalf("missing historical closure: %+v", c)
	}
	restored := restoreMetadata(t, f, o)
	if metadataSemantics(t, restored) != before {
		t.Fatal("historical metadata closure differs")
	}
	t.Log("SCENARIO historical closure: five whole objects (packed, legacy, retired packed, retired legacy, catalog orphan); all repair tables, snapshot-only zero-current recipe and pin-only chunk survive exact metadata round trip")
}
