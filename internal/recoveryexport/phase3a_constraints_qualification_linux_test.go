//go:build linux

package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

func phase3aFollowupObserve(p *phase3aFollowupMetadataProbe) *hooks {
	return &hooks{wrapConnector: func(c driver.Connector) driver.Connector {
		return phase3aFollowupProbeConnector{c, p}
	}}
}

func TestPhase3AFollowupConstraintRefusals(t *testing.T) {
	for _, kind := range []string{"missing", "extra", "wrong-kind", "wrong-definition", "not-validated", "deferrable", "no-inherit", "many-records"} {
		t.Run(kind, func(t *testing.T) {
			f := phase3aFollowupAdopted(t)
			switch kind {
			case "missing":
				execSQL(t, f.db, `ALTER TABLE source_identity DROP CONSTRAINT source_identity_repository_id_fkey`)
			case "extra":
				execSQL(t, f.db, `ALTER TABLE source_identity ADD CONSTRAINT followup_extra CHECK(singleton=1)`)
			case "wrong-kind":
				execSQL(t, f.db, `ALTER TABLE source_identity DROP CONSTRAINT source_identity_repository_id_fkey;
      ALTER TABLE source_identity ADD CONSTRAINT source_identity_repository_id_fkey CHECK(singleton=1)`)
			case "wrong-definition":
				execSQL(t, f.db, `ALTER TABLE source_identity DROP CONSTRAINT source_identity_binding_check;
      ALTER TABLE source_identity ADD CONSTRAINT source_identity_binding_check CHECK(binding<>'bound')`)
			case "not-validated":
				execSQL(t, f.db, `ALTER TABLE source_identity DROP CONSTRAINT source_identity_repository_id_fkey;
      ALTER TABLE source_identity ADD CONSTRAINT source_identity_repository_id_fkey FOREIGN KEY(repository_id) REFERENCES repository_identity(repository_id) NOT VALID`)
			case "deferrable":
				execSQL(t, f.db, `ALTER TABLE source_identity ALTER CONSTRAINT source_identity_repository_id_fkey DEFERRABLE INITIALLY DEFERRED`)
			case "no-inherit":
				execSQL(t, f.db, `ALTER TABLE source_identity DROP CONSTRAINT source_identity_binding_check;
      ALTER TABLE source_identity ADD CONSTRAINT source_identity_binding_check CHECK(binding='unbound') NO INHERIT`)
			case "many-records":
				for n := 0; n < 80; n++ {
					execSQL(t, f.db, fmt.Sprintf("ALTER TABLE source_identity ADD CONSTRAINT followup_extra_%d CHECK(singleton=1)", n))
				}
			}
			p := &phase3aFollowupMetadataProbe{}
			consumed := false
			constraints := phase3aFollowupConstraintState(t, f.db)
			r, e := runPreserved(t, f, context.Background(), DefaultLimits(), phase3aFollowupObserve(p), func(context.Context, *Inventory) error { consumed = true; return nil })
			if !errors.Is(e, ErrUnsupported) || r.Complete || consumed {
				t.Fatalf("malformed profile admitted: %+v %v", r, e)
			}
			if constraints != phase3aFollowupConstraintState(t, f.db) {
				t.Fatal("constraint ownership changed")
			}
			stats := p.snapshot()
			if kind != "wrong-definition" && (stats.definitions != 0 || stats.rows > 54) {
				t.Fatalf("set/status refusal performed definition work or exceeded enumeration: %+v", stats)
			}
			t.Logf("constraint refusal %s: %+v", kind, stats)
		})
	}
}

func phase3aFollowupMeasureEnvelope(t *testing.T, conn *sql.DB) {
	t.Helper()
	var count, maximum, total, compressed int64
	must(t, conn.QueryRow(`SELECT count(*),max(`+identityConstraintRawBytes+`),sum(`+identityConstraintRawBytes+`),count(*) FILTER (WHERE NOT `+identityConstraintUncompressed+`)`+identityConstraintRelations).Scan(&count, &maximum, &total, &compressed))
	if count != 53 || maximum > identityConstraintMetadataBytes || total > 53*identityConstraintMetadataBytes || compressed != 0 {
		t.Fatalf("canonical envelope: count=%d max=%d total=%d compressed=%d", count, maximum, total, compressed)
	}
	t.Logf("canonical PostgreSQL metadata: count=%d max=%d total=%d compressed=%d cap=%d aggregate cap=%d", count, maximum, total, compressed, identityConstraintMetadataBytes, 53*identityConstraintMetadataBytes)
}

func TestPhase3AFollowupCanonicalRecovery(t *testing.T) {
	f, _ := adoptionFixture(t)
	// A legitimate v7-derived column order, produced by the real retained SQL
	// migration, must remain valid before and after identity adoption/import.
	execSQL(t, f.db, `ALTER TABLE schema_version RENAME COLUMN catalog_version TO version;
 UPDATE schema_version SET version=7; DROP TABLE snapshot_file;
 ALTER TABLE snapshot DROP COLUMN parent_id;
 CREATE TABLE snapshot_file(id BIGSERIAL PRIMARY KEY,snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
 path TEXT NOT NULL CHECK(path<>''),logical_file_id BIGINT NOT NULL REFERENCES logical_file(id),size BIGINT,mode BIGINT,mtime TIMESTAMPTZ);
 INSERT INTO snapshot_file(snapshot_id,path,logical_file_id,size) VALUES('legacy-adoption','history/migrated.txt',2,4)`)
	schema, e := os.ReadFile("../../db/schema_postgres.sql")
	must(t, e)
	execSQL(t, f.db, string(schema))
	o := grantAdopter(t, f)
	r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, noConsume)
	must(t, e)
	if !r.Complete {
		t.Fatal("migrated schema17 refused")
	}
	_, e = ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	phase3aFollowupMeasureEnvelope(t, f.db)
	pkg := exportOptions(t)
	_, e = Export(context.Background(), f.config, pkg)
	must(t, e)
	d, open := importTarget(t, f)
	imported, e := importPreserved(t, f, d, importOptionsFor(t, pkg), nil)
	must(t, e)
	if !imported.Ready {
		t.Fatal(imported)
	}
	conn := open()
	phase3aFollowupMeasureEnvelope(t, conn)
	if phase3aFollowupConstraintState(t, f.db) != phase3aFollowupConstraintState(t, conn) {
		t.Fatal("dump/import constraint identity changed")
	}
	p := &phase3aFollowupMetadataProbe{}
	r, e = inspect(context.Background(), Config(d), DefaultLimits(), noConsume, phase3aFollowupObserve(p))
	must(t, e)
	if !r.Complete || p.snapshot().definitions != 53 {
		t.Fatalf("restored canonical profile not fully checked: %+v", p.snapshot())
	}
}

func TestPhase3AFollowupMetadataEnvelope(t *testing.T) {
	f := phase3aFollowupAdopted(t)
	phase3aFollowupMeasureEnvelope(t, f.db)
	// This isolated temporary relation exercises the exact SQL guard at adjacent
	// byte boundaries. PLAIN storage intentionally prevents compression. It is
	// not a forged system catalog or a claim that arbitrary definitions are valid.
	conn, e := f.db.Conn(context.Background())
	must(t, e)
	defer func() { must(t, conn.Close()) }()
	_, e = conn.ExecContext(context.Background(), `CREATE TEMP TABLE followup_raw(
 conbin text, conkey int2[], confkey int2[], conpfeqop oid[], conppeqop oid[],conffeqop oid[],confdelsetcols int2[],conexclop oid[]);
 ALTER TABLE followup_raw ALTER COLUMN conbin SET STORAGE PLAIN`)
	must(t, e)
	for _, size := range []int64{4095, 4096, 4097} {
		_, e = conn.ExecContext(context.Background(), `TRUNCATE followup_raw`)
		must(t, e)
		_, e = conn.ExecContext(context.Background(), `INSERT INTO followup_raw(conbin) SELECT repeat('x',$1)`, size-8)
		must(t, e)
		var observed int64
		var plain, allowed bool
		must(t, conn.QueryRowContext(context.Background(), `SELECT `+identityConstraintRawBytes+`,`+identityConstraintUncompressed+`, CASE WHEN `+identityConstraintUncompressed+` AND `+identityConstraintRawBytes+`<=$1 THEN true ELSE false END FROM followup_raw c`, identityConstraintMetadataBytes).Scan(&observed, &plain, &allowed))
		if observed != size || !plain || allowed != (size <= 4096) {
			t.Fatalf("raw envelope boundary: size=%d observed=%d plain=%v allowed=%v", size, observed, plain, allowed)
		}
		t.Logf("real SQL raw guard boundary: bytes=%d admitted=%v", observed, allowed)
	}
	// Retain lower caller-selected row and serialization ceilings.
	for _, kind := range []string{"rows", "bytes"} {
		limits := DefaultLimits()
		if kind == "rows" {
			limits.Rows = 1
		} else {
			limits.CatalogBytes = 1
		}
		p := &phase3aFollowupMetadataProbe{}
		r, e := runPreserved(t, f, context.Background(), limits, phase3aFollowupObserve(p), noConsume)
		if !errors.Is(e, ErrLimit) || r.Complete || p.snapshot().queries != 0 {
			t.Fatalf("lower %s limit ignored: %+v %v", kind, r, e)
		}
	}
}

func TestPhase3AFollowupA2ExportRefusal(t *testing.T) {
	f := phase3aFollowupAdopted(t)
	execSQL(t, f.db, `ALTER TABLE source_identity DROP CONSTRAINT source_identity_binding_check`)
	execSQL(t, f.db, `ALTER TABLE source_identity ADD CONSTRAINT source_identity_binding_check CHECK(binding='unbound' OR binding='`+strings.Repeat("x", 16384)+`')`)
	p := &phase3aFollowupMetadataProbe{}
	o := exportOptions(t)
	r, e := exportPreserved(t, f, o, &exportHooks{cut: phase3aFollowupObserve(p)})
	if !errors.Is(e, ErrUnsupported) {
		t.Fatal(e)
	}
	assertIncomplete(t, o, r, e)
	noMarker(t, o.Destination)
	if p.snapshot().definitions != 0 || p.snapshot().maxValueBytes > 4096 {
		t.Fatalf("unsafe export metadata: %+v", p.snapshot())
	}
}

func TestPhase3AFollowupRestoreBeforePins(t *testing.T) {
	f, files := recoveryHistory(t, false)
	_, e := ApplyAdoption(context.Background(), f.config, grantAdopter(t, f))
	must(t, e)
	pkg := exportOptions(t)
	_, e = Export(context.Background(), f.config, pkg)
	must(t, e)
	d, open := importTarget(t, f)
	r, e := importPreserved(t, f, d, importOptionsFor(t, pkg), nil)
	must(t, e)
	if !r.Ready {
		t.Fatal(r)
	}
	conn := open()
	// A nontransactional sequence witnesses even transient pin UPDATE attempts.
	// It is fixture-only; a valid restore first proves the trigger is exercised.
	execSQL(t, conn, `CREATE SEQUENCE public.followup_pin_attempts;
 CREATE FUNCTION public.followup_pin_probe() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN PERFORM nextval('public.followup_pin_attempts'); RETURN NEW; END $$;
 CREATE TRIGGER followup_pin_probe BEFORE UPDATE OF pin_count ON public.chunk
 FOR EACH ROW EXECUTE FUNCTION public.followup_pin_probe()`)
	execSQL(t, conn, "GRANT USAGE,SELECT ON SEQUENCE public.followup_pin_attempts TO "+pqIdent(d.User))
	binary := os.Getenv("COLDKEEP_RECOVERY_READER")
	if binary == "" {
		t.Fatal("ordinary runner must stage current operator")
	}
	cfg := readerConfig(t, d)
	output := filepath.Join(t.TempDir(), "valid")
	out, e := runReader(t, binary, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
	if e != nil {
		t.Fatalf("valid operator control: %v %s", e, out)
	}
	checkRestoredHistory(t, output, files)
	var pinSequence int64
	var called bool
	must(t, conn.QueryRow("SELECT last_value,is_called FROM public.followup_pin_attempts").Scan(&pinSequence, &called))
	if !called {
		t.Fatal("pin probe positive control not exercised")
	}
	phase3aFollowupMisplaceConstraint(t, conn, "foreign-key-relocated")
	before, tree := catalogFingerprint(t, conn), treeFingerprint(t, d.PayloadRoot)
	constraints := phase3aFollowupConstraintState(t, conn)
	source, sourceTree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
	receiptPath := filepath.Join(d.PayloadRoot, ".coldkeep-control/IMPORT_READY")
	receipt, e := os.ReadFile(receiptPath)
	must(t, e)
	output = filepath.Join(t.TempDir(), "refused")
	out, e = runReader(t, binary, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
	if e == nil || !strings.Contains(string(out), ErrUnsupported.Error()) {
		t.Fatalf("wrong operator refusal: %v %s", e, out)
	}
	if _, e = os.Lstat(output); !os.IsNotExist(e) {
		t.Fatalf("output published: %v", e)
	}
	var after int64
	must(t, conn.QueryRow("SELECT last_value FROM public.followup_pin_attempts").Scan(&after))
	now, e := os.ReadFile(receiptPath)
	must(t, e)
	if after != pinSequence || string(now) != string(receipt) || before != catalogFingerprint(t, conn) || tree != treeFingerprint(t, d.PayloadRoot) || constraints != phase3aFollowupConstraintState(t, conn) || source != catalogFingerprint(t, f.db) || sourceTree != treeFingerprint(t, f.root) {
		t.Fatal("early restore refusal changed pins/catalog/receipt/source/payload")
	}
	assertNoInventorySessions(t, conn)
	var sessions int
	must(t, conn.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE application_name='coldkeep-recovery-reader'").Scan(&sessions))
	if sessions != 0 {
		t.Fatal("operator session leaked")
	}
	t.Log("actual operator: valid history restored; malformed constraints refused before pin UPDATE or output; receipt preserved")
}

func TestPhase3AFollowupMetadataCleanup(t *testing.T) {
	for _, stage := range []string{"preflight", "definition"} {
		t.Run(stage, func(t *testing.T) {
			f := phase3aFollowupAdopted(t)
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gates := newCleanupDriver(t, nil, nil)
			var once sync.Once
			p := &phase3aFollowupMetadataProbe{afterRow: func(definition bool) {
				if definition == (stage == "definition") {
					once.Do(cancel)
				}
			}}
			h := &hooks{wrapConnector: func(c driver.Connector) driver.Connector {
				return gatedCatalogConnector{phase3aFollowupProbeConnector{c, p}, gates}
			}}
			done := make(chan struct{})
			var r Result
			var opErr error
			consumed := false
			go func() {
				r, opErr = inspect(ctx, f.config, DefaultLimits(), func(context.Context, *Inventory) error { consumed = true; return nil }, h)
				close(done)
			}()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			if ctx.Err() == nil {
				t.Fatal("did not cancel at metadata row boundary")
			}
			id, e := coordination.ResolveIdentity(f.root)
			must(t, e)
			assertCleanupExclusion(t, id)
			adoptionContender(t, f.root)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			assertCleanupExclusion(t, id)
			adoptionContender(t, f.root)
			select {
			case <-done:
				t.Fatal("returned before physical driver cleanup")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if !errors.Is(opErr, context.Canceled) || r.Complete || consumed {
				t.Fatalf("canceled metadata result: %+v %v", r, opErr)
			}
			assertNoInventorySessions(t, f.db)
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("cancellation mutated source")
			}
			t.Logf("metadata %s canceled; rollback/close retained in-process and second-process ownership: %+v", stage, p.snapshot())
		})
	}
}
