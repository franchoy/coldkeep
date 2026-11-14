package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/engine"
)

func grantAdopter(t *testing.T, f *fixture) AdoptionOptions {
	t.Helper()
	for _, spec := range catalogTables {
		execSQL(t, f.db, "ALTER TABLE public."+pqIdent(spec.name)+" OWNER TO "+pqIdent(f.config.User))
	}
	execSQL(t, f.db, "GRANT CREATE ON SCHEMA public TO "+pqIdent(f.config.User))
	p, e := PreviewAdoption(context.Background(), f.config, DefaultLimits())
	must(t, e)
	id, e := db.NewIdentity("adopt")
	must(t, e)
	return AdoptionOptions{ExpectedSchema: 17, OperationID: id, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)}
}
func TestAdoptionHistoricalIsolation(t *testing.T) {
	for _, plain := range []bool{true, false} {
		t.Run(fmt.Sprintf("plaintext=%v", plain), func(t *testing.T) {
			f, files := recoveryHistory(t, plain)
			key := stageRecoveryKey(t, f)
			o := grantAdopter(t, f)
			r, e := ApplyAdoption(context.Background(), f.config, o)
			must(t, e)
			post := exportOptions(t)
			_, e = Export(context.Background(), f.config, post)
			must(t, e)
			clear(f.key)
			isolatedRecovery(t, f, o.Export, files, key, fmt.Sprintf("phase3a-pre17-plain-%v", plain))
			isolatedRecovery(t, f, post, files, key, fmt.Sprintf("phase3a-post18-plain-%v", plain))
			t.Logf("PHASE3A isolated pre17/post18 fixed history; repository=%s migration=%s", r.RepositoryID, r.OperationID)
		})
	}
}
func TestAdoptionWriterFences(t *testing.T) {
	if os.Getenv("COLDKEEP_PHASE1C_TEST") != "1" {
		t.Skip("explicit disposable PostgreSQL required")
	}
	readers := os.Getenv("COLDKEEP_PHASE3A_READERS")
	if readers == "" {
		t.Fatal("blocking runner must stage exact preserved writers")
	}
	for _, label := range []string{"d19", "accepted", "current"} {
		t.Run(label, func(t *testing.T) {
			f, o := adoptionFixture(t)
			execSQL(t, f.db, "DELETE FROM repository_config WHERE key='compression'")
			run := func() ([]byte, error) {
				cmd := exec.Command(filepath.Join(readers, label+"-writer"))
				cmd.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true")
				return cmd.CombinedOutput()
			}
			out, e := run()
			if e != nil {
				t.Fatalf("actual schema17 positive control: %v %s", e, out)
			}
			var value string
			must(t, f.db.QueryRow("SELECT value FROM repository_config WHERE key='compression'").Scan(&value))
			if value != "none" {
				t.Fatal("no actual positive mutation")
			}
			p, e := PreviewAdoption(context.Background(), f.config, DefaultLimits())
			must(t, e)
			o.PlanSHA256 = p.PlanSHA256
			_, e = ApplyAdoption(context.Background(), f.config, o)
			must(t, e)
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			out, e = run()
			if e == nil || !strings.Contains(string(out), "catalog_version 18") || !strings.Contains(string(out), "future version") {
				t.Fatalf("wrong refusal: %v %s", e, out)
			}
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("incompatible production writer changed source")
			}
			t.Logf("PHASE3A %s actual OpenLocalStorage/SetConfiguration: schema17 writes; schema18 bootstrap-enabled refuses: %s", label, out)
		})
	}
}
func readerConfig(t *testing.T, d Destination) string {
	t.Helper()
	dir := t.TempDir()
	password, e := d.Password(context.Background())
	must(t, e)
	must(t, os.WriteFile(filepath.Join(dir, "password"), []byte(password), 0600))
	keyfile := ""
	if d.Key != nil {
		key, e := d.Key(context.Background())
		must(t, e)
		keyfile = filepath.Join(dir, "key")
		must(t, os.WriteFile(keyfile, []byte(hex.EncodeToString(key)), 0600))
	}
	b, e := json.Marshal(map[string]any{"host": d.Host, "port": d.Port, "database": d.Database, "user": d.User, "payload_root": d.PayloadRoot, "password_file": filepath.Join(dir, "password"), "key_file": keyfile, "key_reference": d.KeyReference, "pg_dump": os.Getenv("COLDKEEP_PG_DUMP"), "pg_restore": os.Getenv("COLDKEEP_PG_RESTORE")})
	must(t, e)
	path := filepath.Join(dir, "config.json")
	must(t, os.WriteFile(path, b, 0600))
	return path
}
func runReader(t *testing.T, binary string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(binary, args...)
	env := []string{}
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "PG") && !strings.HasPrefix(v, "COLDKEEP_KEY=") {
			env = append(env, v)
		}
	}
	cmd.Env = env
	return cmd.CombinedOutput()
}
func TestAdoptionOldReceiptCompatibility(t *testing.T) {
	f, files := recoveryHistory(t, false)
	pre := exportOptions(t)
	_, e := Export(context.Background(), f.config, pre)
	must(t, e)
	old := filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), "accepted-reader")
	current := os.Getenv("COLDKEEP_RECOVERY_READER")
	if current == "" {
		t.Fatal("staged current reader required")
	}
	d, open := importTarget(t, f)
	cfg := readerConfig(t, d)
	opts := importOptionsFor(t, pre)
	out, e := runReader(t, old, "import", "-config", cfg, "-package", pre.Destination, "-generation", opts.Generation, "-manifest-sha256", opts.ManifestSHA256, "-trusted-operator-backup", "-exclusive-target", "-database-capacity-bytes", "805306368")
	if e != nil {
		t.Fatalf("old import: %v %s", e, out)
	}
	receiptPath := filepath.Join(d.PayloadRoot, ".coldkeep-control/IMPORT_READY")
	original, e := os.ReadFile(receiptPath)
	must(t, e)
	for _, binary := range []string{old, current} {
		output := filepath.Join(t.TempDir(), "restored")
		out, e = runReader(t, binary, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
		if e != nil {
			t.Fatalf("receipt compatibility: %v %s", e, out)
		}
		checkRestoredHistory(t, output, files)
		now, e := os.ReadFile(receiptPath)
		must(t, e)
		if string(now) != string(original) {
			t.Fatal("receipt rewritten")
		}
	}
	conn := open()
	before, tree := catalogFingerprint(t, conn), treeFingerprint(t, d.PayloadRoot)
	id, e := db.NewIdentity("adopt")
	must(t, e)
	_, e = ApplyAdoption(context.Background(), Config(d), AdoptionOptions{ExpectedSchema: 17, OperationID: id, PlanSHA256: strings.Repeat("0", 64), Export: exportOptions(t)})
	if !errors.Is(e, ErrRetainedAuthority) {
		t.Fatal(e)
	}
	if before != catalogFingerprint(t, conn) || tree != treeFingerprint(t, d.PayloadRoot) {
		t.Fatal("retained v1 adoption refusal changed target")
	}
	// Untouched old reader remains valid after the refused adoption.
	output := filepath.Join(t.TempDir(), "again")
	out, e = runReader(t, old, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
	if e != nil {
		t.Fatalf("old retained authority control: %v %s", e, out)
	}
	checkRestoredHistory(t, output, files)
	// New schema18 package and receipt reject in the untouched operator.
	o := grantAdopter(t, f)
	_, e = ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
	next, openNext := importTarget(t, f)
	nextCfg := readerConfig(t, next)
	postOpts := importOptionsFor(t, post)
	out, e = runReader(t, old, "import", "-config", nextCfg, "-package", post.Destination, "-generation", postOpts.Generation, "-manifest-sha256", postOpts.ManifestSHA256, "-trusted-operator-backup", "-exclusive-target", "-database-capacity-bytes", "805306368")
	if e == nil {
		t.Fatalf("old imported new tuple: %s", out)
	}
	if _, e = os.Stat(next.PayloadRoot); !os.IsNotExist(e) {
		t.Fatal("old import mutated root")
	}
	imported, e := Import(context.Background(), next, postOpts)
	must(t, e)
	if imported.Receipt.Kind != "coldkeep-import-ready/v2" {
		t.Fatal(imported)
	}
	nextDB := openNext()
	before, tree = catalogFingerprint(t, nextDB), treeFingerprint(t, next.PayloadRoot)
	out, e = runReader(t, old, "restore", "-config", nextCfg, "-snapshot", "independent-history", "-output", filepath.Join(t.TempDir(), "refused"))
	if e == nil || (!strings.Contains(string(out), "unknown field") && !strings.Contains(string(out), "receipt/target/key mismatch")) {
		t.Fatalf("wrong old reader refusal %v %s", e, out)
	}
	if before != catalogFingerprint(t, nextDB) || tree != treeFingerprint(t, next.PayloadRoot) {
		t.Fatal("old reader mutated new catalog")
	}
}
func TestAdoptionProcessContender(t *testing.T) {
	root := os.Getenv("CK_PHASE3A_CONTEND_ROOT")
	if root == "" {
		return
	}
	id, e := coordination.ResolveIdentity(root)
	must(t, e)
	lease, e := coordination.AcquireExisting(context.Background(), id)
	if lease != nil {
		_ = lease.Release()
	}
	if !errors.Is(e, coordination.ErrRepositoryBusy) {
		t.Fatalf("second process entered owned interval: %v", e)
	}
}
func adoptionContender(t *testing.T, root string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAdoptionProcessContender$")
	cmd.Env = append(os.Environ(), "CK_PHASE3A_CONTEND_ROOT="+root)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("contender %v %s", e, out)
	}
}
func TestAdoptionOwnershipContinuity(t *testing.T) {
	f, o := adoptionFixture(t)
	seen := map[string]bool{}
	_, e := applyAdoption(context.Background(), f.config, o, &adoptionHooks{event: func(s string, _ *sql.Tx) error {
		switch s {
		case "before-checkpoint", "checkpoint-published", "checkpoint-checked", "before-ddl", "before-commit", "after-commit", "migration-draining", "migration-closed":
			adoptionContender(t, f.root)
			seen[s] = true
		}
		return nil
	}})
	must(t, e)
	if len(seen) != 8 {
		t.Fatal(seen)
	}
}
func TestAdoptionCleanupOwnership(t *testing.T) {
	for _, cancelWork := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelWork), func(t *testing.T) {
			f, o := adoptionFixture(t)
			gates := newCleanupDriver(t, nil, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var err error
			h := &adoptionHooks{cut: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}}
			go func() { _, err = applyAdoption(ctx, f.config, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			if cancelWork {
				cancel()
			}
			adoptionContender(t, f.root)
			noMarker(t, o.Export.Destination)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			adoptionContender(t, f.root)
			select {
			case <-done:
				t.Fatal("returned before physical close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if cancelWork {
				if err == nil {
					t.Fatal("canceled operation succeeded")
				}
			} else {
				must(t, err)
			}
		})
	}
}
func TestAdoptionProfileRefusals(t *testing.T) {
	for _, kind := range []string{"future", "missing-table", "extra-table", "unknown-column", "wrong-type", "missing-lineage", "unknown-capability", "constraint", "orphan", "identity-bound", "row-budget", "text-budget"} {
		t.Run(kind, func(t *testing.T) {
			f, o := adoptionFixture(t)
			_, e := ApplyAdoption(context.Background(), f.config, o)
			must(t, e)
			limits := DefaultLimits()
			switch kind {
			case "future":
				execSQL(t, f.db, "UPDATE schema_version SET catalog_version=19")
			case "missing-table":
				execSQL(t, f.db, "DROP TABLE legacy_snapshot_origin")
			case "extra-table":
				execSQL(t, f.db, "CREATE TABLE unrelated(x text)")
			case "unknown-column":
				execSQL(t, f.db, "ALTER TABLE source_identity ADD COLUMN extra text")
			case "wrong-type":
				execSQL(t, f.db, "ALTER TABLE legacy_snapshot_origin ADD COLUMN unsupported numeric")
			case "missing-lineage":
				execSQL(t, f.db, "DELETE FROM legacy_snapshot_origin")
			case "unknown-capability":
				execSQL(t, f.db, "ALTER TABLE repository_identity DROP CONSTRAINT repository_identity_capability_check; UPDATE repository_identity SET capability='future'")
			case "constraint":
				execSQL(t, f.db, "ALTER TABLE source_identity DROP CONSTRAINT source_identity_repository_id_fkey")
			case "orphan":
				execSQL(t, f.db, "ALTER TABLE legacy_snapshot_origin DROP CONSTRAINT legacy_snapshot_origin_source_id_fkey; UPDATE legacy_snapshot_origin SET source_id='source_00000000000000000000000000000000'")
			case "identity-bound":
				execSQL(t, f.db, "ALTER TABLE source_identity DROP CONSTRAINT source_identity_binding_check; UPDATE source_identity SET binding='bound'")
			case "row-budget":
				limits.Rows = 1
			case "text-budget":
				execSQL(t, f.db, "ALTER TABLE repository_identity DROP CONSTRAINT repository_identity_capability_check; UPDATE repository_identity SET capability=repeat('x',4097)")
			}
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			_, e = Inspect(context.Background(), f.config, limits, func(context.Context, *Inventory) error { return nil })
			if e == nil {
				t.Fatal("bad profile admitted")
			}
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("profile refusal changed source")
			}
		})
	}
}
func TestAdoptionIdentityConstraints(t *testing.T) {
	f, o := adoptionFixture(t)
	_, e := ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	for _, q := range []string{"INSERT INTO repository_identity SELECT * FROM repository_identity", "UPDATE storage_root_identity SET repository_id='repo_00000000000000000000000000000000'", "UPDATE source_identity SET enabled=true", "UPDATE device_identity SET device_id='device_notcanonical'"} {
		if _, e = f.db.Exec(q); e == nil {
			t.Fatal("database constraint missing", q)
		}
	}
	var inv *Inventory
	_, e = Inspect(context.Background(), f.config, DefaultLimits(), func(_ context.Context, i *Inventory) error { inv = i; return nil })
	must(t, e)
	original := adoptionResult(inv)
	inv.Catalog["adoption_operation"][0]["source_id"] = "source_00000000000000000000000000000000"
	if validateIdentity(inv) == nil {
		t.Fatal("orphan checkpoint admitted")
	}
	inv.Catalog["adoption_operation"][0]["source_id"] = string(original.SourceID)
	if !reflect.DeepEqual(original, adoptionResult(inv)) {
		t.Fatal("identity changed")
	}
}

func checkRestoredHistory(t *testing.T, output string, files []expectedRecoveryFile) {
	t.Helper()
	for _, f := range files {
		path := filepath.Join(output, f.Path)
		b, e := os.ReadFile(path)
		must(t, e)
		hash := sha256.Sum256(b)
		info, e := os.Stat(path)
		must(t, e)
		if hex.EncodeToString(hash[:]) != f.Hash || info.Mode().Perm() != 0640 || info.ModTime().UTC().Format("2006-01-02T15:04:05Z") != "2025-01-02T03:04:05Z" {
			t.Fatalf("fixed history mismatch %s", f.Path)
		}
	}
}

const identityLineageQuery = `SELECT jsonb_build_object(
 'repository',(SELECT jsonb_agg(to_jsonb(t) ORDER BY singleton) FROM repository_identity t),
 'root',(SELECT jsonb_agg(to_jsonb(t) ORDER BY singleton) FROM storage_root_identity t),
 'device',(SELECT jsonb_agg(to_jsonb(t) ORDER BY singleton) FROM device_identity t),
 'source',(SELECT jsonb_agg(to_jsonb(t) ORDER BY singleton) FROM source_identity t),
 'operation',(SELECT jsonb_agg(to_jsonb(t) ORDER BY singleton) FROM adoption_operation t),
 'legacy',(SELECT jsonb_agg(to_jsonb(t) ORDER BY snapshot_id) FROM legacy_snapshot_origin t))::text`

func TestAdoptionMigrationCleanup(t *testing.T) {
	for _, kind := range []string{"rollback", "driver-close", "cancellation"} {
		t.Run(kind, func(t *testing.T) {
			f, o := adoptionFixture(t)
			before := catalogFingerprint(t, f.db)
			gates := newCleanupDriver(t, nil, nil)
			if kind == "driver-close" {
				gates.closeErr = errors.New("injected physical close failure")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var opErr error
			h := &adoptionHooks{migration: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}, event: func(s string, _ *sql.Tx) error {
				if s == "before-commit" {
					if kind == "cancellation" {
						cancel()
					}
					return errors.New("abort before commit")
				}
				return nil
			}}
			go func() { _, opErr = applyAdoption(ctx, f.config, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			adoptionContender(t, f.root)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			adoptionContender(t, f.root)
			select {
			case <-done:
				t.Fatal("migration cleanup released ownership early")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if opErr == nil {
				t.Fatal("cleanup fault succeeded")
			}
			if catalogFingerprint(t, f.db) != before {
				t.Fatal("precommit cleanup failure changed schema/data")
			}
			_, e := Check(context.Background(), o.Export.Destination, o.Export.Limits)
			must(t, e)
		})
	}
}
func TestAdoptionExportCleanup(t *testing.T) {
	for _, kind := range []string{"rollback", "credential", "child"} {
		t.Run(kind, func(t *testing.T) {
			f, o := adoptionFixture(t)
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var opErr error
			eh := &exportHooks{}
			h := &adoptionHooks{export: eh}
			started, allow := newCleanupSignal(), newCleanupSignal()
			switch kind {
			case "rollback":
				gates := newCleanupDriver(t, nil, nil)
				gates.allowClose.fire()
				eh.cut = &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}
				started = gates.rollbackStarted
				allow = gates.allowRollback
			case "credential":
				eh.credentials = func(clean func() error) error { started.fire(); <-allow.ch; return clean() }
			case "child":
				o.Export.Tools.PGDump = fakeDump(t, "exec /bin/sleep 60")
				eh.event = func(s string) error {
					if s == "child-started" {
						cancel()
					}
					if s == "child-joined" {
						started.fire()
						<-allow.ch
					}
					return nil
				}
			}
			go func() { _, opErr = applyAdoption(ctx, f.config, o, h); close(done) }()
			defer func() { allow.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, started.ch)
			adoptionContender(t, f.root)
			noMarker(t, o.Export.Destination)
			if kind != "child" {
				cancel()
			}
			allow.fire()
			awaitCleanup(t, done)
			if opErr == nil {
				t.Fatal("canceled cleanup succeeded")
			}
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("export cleanup failure changed source")
			}
		})
	}
}
func TestAdoptionOperatorAndDatabaseReservation(t *testing.T) {
	f, o := adoptionFixture(t)
	binary := os.Getenv("COLDKEEP_RECOVERY_READER")
	if binary == "" {
		t.Fatal("staged operator required")
	}
	cfg := readerConfig(t, Destination(f.config))
	out, e := runReader(t, binary, "adopt-preview", "-config", cfg)
	if e != nil {
		t.Fatalf("operator preview %v %s", e, out)
	}
	var preview AdoptionResult
	must(t, json.Unmarshal(out, &preview))
	if preview.PlanSHA256 != o.PlanSHA256 {
		t.Fatal("operator plan differs")
	}
	// A different canonical root for the same database must still contend.
	otherRoot := filepath.Join(t.TempDir(), "alias-copy")
	must(t, os.Mkdir(otherRoot, 0700))
	must(t, filepath.WalkDir(f.root, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(f.root, path)
		if e != nil || rel == "." {
			return e
		}
		target := filepath.Join(otherRoot, rel)
		if d.IsDir() {
			return os.Mkdir(target, 0700)
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return os.WriteFile(target, b, 0600)
	}))
	other := f.config
	other.PayloadRoot = otherRoot
	otherCfg := readerConfig(t, Destination(other))
	otherPreview, e := PreviewAdoption(context.Background(), other, DefaultLimits())
	must(t, e)
	seen := false
	result, e := applyAdoption(context.Background(), f.config, o, &adoptionHooks{event: func(s string, _ *sql.Tx) error {
		if s != "checkpoint-checked" {
			return nil
		}
		seen = true
		out, e := runReader(t, binary, "adopt-apply", "-config", otherCfg, "-expected-schema", "17", "-operation-id", o.OperationID, "-plan-sha256", otherPreview.PlanSHA256, "-checkpoint", filepath.Join(t.TempDir(), "contending-checkpoint"))
		if e == nil || !strings.Contains(string(out), "database reserved") {
			t.Fatalf("distinct-root process bypassed reservation: %v %s", e, out)
		}
		return nil
	}})
	must(t, e)
	if !seen {
		t.Fatal("missing reservation barrier")
	}
	// The public operator reconciles the committed same request without re-export.
	out, e = runReader(t, binary, "adopt-apply", "-config", cfg, "-expected-schema", "17", "-operation-id", o.OperationID, "-plan-sha256", o.PlanSHA256, "-checkpoint", o.Export.Destination)
	if e != nil {
		t.Fatalf("operator reconcile %v %s", e, out)
	}
	var actual AdoptionResult
	must(t, json.Unmarshal(out, &actual))
	if result != actual {
		t.Fatal("operator assigned competing IDs")
	}
	// A fresh valid schema17 source is also adopted through the actual command.
	second, opts := adoptionFixture(t)
	secondCfg := readerConfig(t, Destination(second.config))
	out, e = runReader(t, binary, "adopt-apply", "-config", secondCfg, "-expected-schema", "17", "-operation-id", opts.OperationID, "-plan-sha256", opts.PlanSHA256, "-checkpoint", opts.Export.Destination)
	if e != nil {
		t.Fatalf("actual operator apply %v %s", e, out)
	}
	must(t, json.Unmarshal(out, &actual))
	if actual.RepositoryID == "" {
		t.Fatal("operator no identity")
	}
}

func TestAdoptionImportProfiles(t *testing.T) {
	f, o := adoptionFixture(t)
	_, e := ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
	for _, kind := range []string{"mixed-version", "false-schema17", "unknown-reader", "repository-conflict", "catalog-missing-lineage", "receipt-v1", "receipt-reader", "receipt-identity"} {
		t.Run(kind, func(t *testing.T) {
			opts := importOptionsFor(t, post)
			opts.Package = filepath.Join(t.TempDir(), "package")
			must(t, copyTreeForImport(post.Destination, opts.Package))
			mutateImportManifest(t, &opts, func(m *Manifest) {
				switch kind {
				case "mixed-version":
					m.Version = 1
				case "false-schema17":
					m.Version = 1
					m.CatalogVersion = 17
					m.Tables = 19
					m.Reader = importReader
					m.RepositoryID = ""
				case "unknown-reader":
					m.Reader = "future"
				case "repository-conflict":
					m.RepositoryID = "repo_00000000000000000000000000000000"
				}
			})
			d, open := importTarget(t, f)
			var h *importHooks
			if kind == "catalog-missing-lineage" {
				h = &importHooks{event: func(s string) error {
					if s == "restored" {
						conn := open()
						_, e := conn.Exec("DELETE FROM legacy_snapshot_origin")
						return e
					}
					return nil
				}}
			}
			result, e := importGeneration(context.Background(), d, opts, h)
			if strings.HasPrefix(kind, "receipt-") {
				must(t, e)
				receipt := *result.Receipt
				switch kind {
				case "receipt-v1":
					receipt.Kind = "coldkeep-import-ready/v1"
				case "receipt-reader":
					receipt.Reader = importReader
				case "receipt-identity":
					receipt.RepositoryID = "repo_00000000000000000000000000000000"
				}
				conn := open()
				before, tree := catalogFingerprint(t, conn), treeFingerprint(t, d.PayloadRoot)
				id, e := coordination.ResolveIdentity(d.PayloadRoot)
				must(t, e)
				lease, e := coordination.AcquireExisting(context.Background(), id)
				must(t, e)
				e = ValidateRestoreOwned(context.Background(), Config(d), lease, receipt)
				must(t, lease.Release())
				if e == nil {
					t.Fatal("mixed receipt admitted")
				}
				if before != catalogFingerprint(t, conn) || tree != treeFingerprint(t, d.PayloadRoot) {
					t.Fatal("receipt refusal mutated target")
				}
			} else {
				if e == nil || result.Ready {
					t.Fatal("false profile admitted")
				}
				if _, e = os.Stat(filepath.Join(d.PayloadRoot, ".coldkeep-control/IMPORT_READY")); !os.IsNotExist(e) {
					t.Fatal("invalid profile ready")
				}
			}
		})
	}
}
func TestAdoptionEngineAdmission(t *testing.T) {
	f, o := adoptionFixture(t)
	eng, e := engine.New(engine.Config{DB: f.db, ContainerDir: f.root})
	must(t, e)
	if eng == nil {
		t.Fatal("legacy engine missing")
	}
	_, e = ApplyAdoption(context.Background(), f.config, o)
	must(t, e)
	before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
	_, e = engine.New(engine.Config{DB: f.db, ContainerDir: f.root})
	if !errors.Is(e, db.ErrIdentityBindingRequired) {
		t.Fatal("raw-handle engine bypass", e)
	}
	if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
		t.Fatal("engine refusal mutated source")
	}
}

func TestAdoptionCheckpointWrongSource(t *testing.T) {
	f, o := adoptionFixture(t)
	other := exportFixture(t)
	otherPackage := exportOptions(t)
	_, e := Export(context.Background(), other.config, otherPackage)
	must(t, e)
	before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
	_, e = applyAdoption(context.Background(), f.config, o, &adoptionHooks{event: func(s string, _ *sql.Tx) error {
		if s != "checkpoint-published" {
			return nil
		}
		// Replace only owned disposable test-package members with a different valid
		// completed generation. The byte checker must pass; source binding must not.
		entries, e := os.ReadDir(o.Export.Destination)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			if e = os.Remove(filepath.Join(o.Export.Destination, entry.Name())); e != nil {
				return e
			}
		}
		entries, e = os.ReadDir(otherPackage.Destination)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			b, e := os.ReadFile(filepath.Join(otherPackage.Destination, entry.Name()))
			if e != nil {
				return e
			}
			if e = os.WriteFile(filepath.Join(o.Export.Destination, entry.Name()), b, 0600); e != nil {
				return e
			}
		}
		_, e = Check(context.Background(), o.Export.Destination, o.Export.Limits)
		return e
	}})
	if !errors.Is(e, ErrPackage) {
		t.Fatal("wrong-source complete generation admitted", e)
	}
	if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
		t.Fatal("wrong-source checkpoint advanced source")
	}
}
func TestAdoptionStalePlanAndBounds(t *testing.T) {
	for _, kind := range []string{"stale-plan", "aggregate", "deadline", "operation-length", "sqlite-file"} {
		t.Run(kind, func(t *testing.T) {
			f, o := adoptionFixture(t)
			ctx := context.Background()
			var cancel context.CancelFunc
			var sqlitePath string
			var sqliteBytes []byte
			switch kind {
			case "stale-plan":
				execSQL(t, f.db, "UPDATE snapshot SET label='changed-after-preview'")
			case "aggregate":
				o.Export.InventoryLimits.CatalogBytes = 1
			case "deadline":
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "operation-length":
				o.OperationID = "adopt_" + strings.Repeat("a", 4097)
			case "sqlite-file":
				sqlitePath = filepath.Join(t.TempDir(), "legacy.db")
				var e error
				sqliteBytes, e = os.ReadFile("../storage/testdata/compress_v1_18_supplemental/catalog.db")
				must(t, e)
				must(t, os.WriteFile(sqlitePath, sqliteBytes, 0600))
				f.config.Backend = "sqlite"
				f.config.Database = sqlitePath
			}
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			_, e := ApplyAdoption(ctx, f.config, o)
			if e == nil {
				t.Fatal("expected no-write refusal")
			}
			if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("stale/bounded input changed source")
			}
			if sqlitePath != "" {
				b, e := os.ReadFile(sqlitePath)
				must(t, e)
				if string(b) != string(sqliteBytes) {
					t.Fatal("SQLite adoption changed catalog")
				}
			}
		})
	}
}
