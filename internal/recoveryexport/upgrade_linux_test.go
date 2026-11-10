package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

func upgradeOptions(t *testing.T, f *fixture, b BindingOptions) UpgradeOptions {
	t.Helper()
	q := UpgradeRequest{InstallationDirectory: b.Request.InstallationDirectory, ExpectedSchema: 19}
	var e error
	q.OperationID, e = db.NewIdentity("upgrade")
	must(t, e)
	must(t, f.db.QueryRow(`SELECT repository_id FROM repository_identity`).Scan(&q.RepositoryID))
	must(t, f.db.QueryRow(`SELECT root_id,revision FROM root_binding_head`).Scan(&q.RootID, &q.ExpectedRevision))
	p, e := PreviewUpgrade(context.Background(), f.config, q, DefaultLimits())
	must(t, e)
	return UpgradeOptions{Request: q, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)}
}
func upgradeFixture(t *testing.T) (*fixture, BindingOptions, UpgradeOptions) {
	t.Helper()
	f, b := bindingFixture(t)
	_, e := ApplyBinding(context.Background(), f.config, b)
	must(t, e)
	return f, b, upgradeOptions(t, f, b)
}
func TestRecoveryUpgradeMigration(t *testing.T) {
	f, b, o := upgradeFixture(t)
	legacy := legacyRowsFingerprint(t, f.db)
	var lineage string
	must(t, f.db.QueryRow(bindingLineageQuery()).Scan(&lineage))
	before := treeFingerprint(t, f.root)
	descriptor, e := os.ReadFile(filepath.Join(b.Request.InstallationDirectory, bindingDescriptorName))
	must(t, e)
	r, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	if r.Status != "complete; recovery-only schema20" || r.OrdinaryWriterReady {
		t.Fatal(r)
	}
	var after string
	must(t, f.db.QueryRow(bindingLineageQuery()).Scan(&after))
	if after != lineage || legacyRowsFingerprint(t, f.db) != legacy || treeFingerprint(t, f.root) != before {
		t.Fatal("upgrade changed historical or physical identity")
	}
	stored, e := os.ReadFile(filepath.Join(b.Request.InstallationDirectory, bindingDescriptorName))
	must(t, e)
	if string(stored) != string(descriptor) {
		t.Fatal("descriptor changed")
	}
	again, e := ReconcileUpgrade(context.Background(), f.config, o)
	must(t, e)
	if again != r {
		t.Fatal("retry changed committed identity")
	}
	if _, e = Check(context.Background(), o.Export.Destination, o.Export.Limits); e != nil {
		t.Fatal("pre19 checkpoint", e)
	}
	report, e := Inspect(context.Background(), f.config, DefaultLimits(), noConsume)
	must(t, e)
	if !report.Complete {
		t.Fatal(report)
	}
}
func TestRecoveryUpgradeFaults(t *testing.T) {
	for _, stage := range []string{"before-checkpoint", "before-intent", "file-rename:" + bindingIntentName, "after-intent", "before-ddl", "during-ddl", "during-data", "before-commit", "after-commit", "before-intent-removal", "cleanup"} {
		t.Run(stage, func(t *testing.T) {
			f, _, o := upgradeFixture(t)
			before := catalogFingerprint(t, f.db)
			fault := errors.New("upgrade cut")
			_, e := applyUpgrade(context.Background(), f.config, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
				if s == stage {
					return fault
				}
				return nil
			}})
			if !errors.Is(e, fault) {
				t.Fatal("fault not reached", e)
			}
			var version int
			must(t, f.db.QueryRow("SELECT catalog_version FROM schema_version").Scan(&version))
			if stage != "after-commit" && stage != "before-intent-removal" && stage != "cleanup" {
				if version != 19 || catalogFingerprint(t, f.db) != before {
					t.Fatal("precommit mutation")
				}
			} else if version != 20 {
				t.Fatal("lost commit")
			}
			if stage == "before-intent" {
				o.Export = exportOptions(t)
			}
			r, e := ReconcileUpgrade(context.Background(), f.config, o)
			must(t, e)
			if r.OrdinaryWriterReady {
				t.Fatal("writer ready")
			}
		})
	}
}
func TestRecoveryUpgradeIdentityRefusals(t *testing.T) {
	for _, kind := range []string{"schema", "repository", "root", "revision", "plan", "foreign-intent", "partial-staging", "missing-marker", "missing-descriptor", "marker-staging", "descriptor-staging", "import-v4", "import-future"} {
		t.Run(kind, func(t *testing.T) {
			f, b, o := upgradeFixture(t)
			switch kind {
			case "schema":
				o.Request.ExpectedSchema = 18
			case "repository":
				o.Request.RepositoryID = "repo_" + strings.Repeat("a", 32)
			case "root":
				o.Request.RootID = "root_" + strings.Repeat("a", 32)
			case "revision":
				o.Request.ExpectedRevision++
			case "plan":
				o.PlanSHA256 = strings.Repeat("0", 64)
			case "foreign-intent":
				must(t, os.WriteFile(filepath.Join(b.Request.InstallationDirectory, bindingIntentName), []byte(`{"kind":"foreign"}`), 0600))
			case "partial-staging":
				must(t, os.WriteFile(filepath.Join(b.Request.InstallationDirectory, bindingIntentName+".pending"), []byte(`{"kind":`), 0600))
			case "missing-marker":
				must(t, os.Remove(filepath.Join(f.root, ".coldkeep-control", bindingMarkerName)))
			case "missing-descriptor":
				must(t, os.Remove(filepath.Join(b.Request.InstallationDirectory, bindingDescriptorName)))
			case "marker-staging":
				must(t, os.WriteFile(filepath.Join(f.root, ".coldkeep-control", bindingMarkerName+".pending"), []byte("foreign"), 0600))
			case "descriptor-staging":
				must(t, os.WriteFile(filepath.Join(b.Request.InstallationDirectory, bindingDescriptorName+".pending"), []byte("foreign"), 0600))
			case "import-v4", "import-future":
				must(t, os.WriteFile(filepath.Join(f.root, ".coldkeep-control", "IMPORT_READY"), []byte(kind), 0600))
			}
			before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			if _, e := ApplyUpgrade(context.Background(), f.config, o); e == nil {
				t.Fatal("identity admitted")
			}
			if catalogFingerprint(t, f.db) != before || treeFingerprint(t, f.root) != tree {
				t.Fatal("refusal mutated source")
			}
		})
	}
}
func TestRecoveryUpgradeOldIntentFence(t *testing.T) {
	for _, stage := range []string{"file-rename:" + bindingIntentName, "after-intent"} {
		t.Run(stage, func(t *testing.T) {
			f, b, o := upgradeFixture(t)
			id, e := db.NewIdentity("source")
			must(t, e)
			conflict := nextBinding(t, f, b, BindingSourceRegister, id, t.TempDir(), 0)
			old := filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), "pre3c-reader")
			config := readerConfig(t, Destination(f.config))
			positive := append(r1BindingArgs("binding-preview", config, conflict), "-source-id", conflict.Request.SourceID, "-source-path-base64", conflict.Request.SourcePathBase64)
			if out, e := runReader(t, old, positive...); e != nil {
				t.Fatal("pre3C positive control", e, string(out))
			}

			_, e = applyUpgrade(context.Background(), f.config, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
				if s == stage {
					return errors.New("retained upgrade intent")
				}
				return nil
			}})
			if e == nil {
				t.Fatal("cut not reached")
			}
			if _, e = os.Stat(old); e != nil {
				t.Fatal("accepted pre3C reader required", e)
			}
			before := catalogFingerprint(t, f.db)
			for _, args := range [][]string{r1BindingArgs("binding-preview", config, conflict), r1BindingArgs("binding-apply", config, conflict), {"binding-check", "-config", config, "-installation-directory", b.Request.InstallationDirectory}} {
				if args[0] != "binding-check" {
					args = append(args, "-source-id", conflict.Request.SourceID, "-source-path-base64", conflict.Request.SourcePathBase64)
				}
				out, e := runReader(t, old, args...)
				if e == nil {
					t.Fatal("old tool admitted pending upgrade", args[0], string(out))
				}
			}
			if catalogFingerprint(t, f.db) != before {
				t.Fatal("old maintenance changed catalog")
			}
			_, e = ReconcileUpgrade(context.Background(), f.config, o)
			must(t, e)
		})
	}
}
func TestRecoveryUpgradeJoinedCleanup(t *testing.T) {
	for _, kind := range []string{"rollback", "close", "cancel", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			f, _, o := upgradeFixture(t)
			gates := newCleanupDriver(t, nil, nil)
			adapter := &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}
			h := &bindingHooks{migration: adapter, event: func(stage string, _ *sql.Tx) error {
				if stage == "before-commit" {
					return errors.New("transaction cut")
				}
				return nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
			}
			done := make(chan struct{})
			var opErr error
			go func() { _, opErr = applyUpgrade(ctx, f.config, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			bindingContenders(t, f)
			if kind == "cancel" {
				cancel()
			}
			if kind == "deadline" {
				<-ctx.Done()
			}
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			bindingContenders(t, f)
			select {
			case <-done:
				t.Fatal("returned before physical close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if opErr == nil {
				t.Fatal("failed transaction succeeded")
			}
		})
	}
}
func TestRecoveryUpgradeExportCleanup(t *testing.T) {
	for _, kind := range []string{"credential", "child"} {
		t.Run(kind, func(t *testing.T) {
			f, _, o := upgradeFixture(t)
			before := catalogFingerprint(t, f.db)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, allow := newCleanupSignal(), newCleanupSignal()
			eh := &exportHooks{}
			h := &bindingHooks{export: eh}
			if kind == "credential" {
				eh.credentials = func(clean func() error) error { started.fire(); <-allow.ch; return clean() }
			} else {
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
			done := make(chan struct{})
			var opErr error
			go func() { _, opErr = applyUpgrade(ctx, f.config, o, h); close(done) }()
			defer func() { allow.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, started.ch)
			bindingContenders(t, f)
			noMarker(t, o.Export.Destination)
			cancel()
			allow.fire()
			awaitCleanup(t, done)
			if opErr == nil || catalogFingerprint(t, f.db) != before {
				t.Fatal("cleanup failed open", opErr)
			}
		})
	}
}

type lostCommitConnector struct{ driver.Connector }

func (c lostCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	v, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	raw, ok := v.(catalogDriverConn)
	if !ok {
		return nil, errors.Join(ErrUnsupported, v.Close())
	}
	return lostCommitConn{raw}, nil
}

type lostCommitConn struct{ catalogDriverConn }

func (c lostCommitConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	v, e := c.catalogDriverConn.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return lostCommitTx{v}, nil
}

type lostCommitTx struct{ driver.Tx }

func (t lostCommitTx) Commit() error {
	if e := t.Tx.Commit(); e != nil {
		return e
	}
	return errors.New("lost actual commit response")
}
func TestRecoveryUpgradeLostCommitResponse(t *testing.T) {
	f, _, o := upgradeFixture(t)
	_, e := applyUpgrade(context.Background(), f.config, o, &bindingHooks{migration: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return lostCommitConnector{c} }}})
	if e == nil || !strings.Contains(e.Error(), "lost actual commit response") {
		t.Fatal("response cut not exercised", e)
	}
	var count int
	must(t, f.db.QueryRow(`SELECT count(*) FROM schema_upgrade_operation WHERE operation_id=$1`, o.Request.OperationID).Scan(&count))
	if count != 1 {
		t.Fatal("not committed")
	}
	r, e := ReconcileUpgrade(context.Background(), f.config, o)
	must(t, e)
	if r.OperationID != o.Request.OperationID {
		t.Fatal("new operation")
	}
}
func TestRecoveryUpgradeAliasReservation(t *testing.T) {
	f, _, o := upgradeFixture(t)
	alias := filepath.Join(t.TempDir(), "alias")
	must(t, os.CopyFS(alias, os.DirFS(f.root)))
	selected := f.config
	selected.PayloadRoot = alias
	r, e := resolve(context.Background(), selected)
	must(t, e)
	defer clear(r.key)
	seen := map[string]bool{}
	_, e = applyUpgrade(context.Background(), f.config, o, &bindingHooks{event: func(stage string, _ *sql.Tx) error {
		switch stage {
		case "after-intent", "before-commit", "after-commit", "cleanup":
			bindingContenders(t, f)
			reservation, e := reserveImport(context.Background(), r)
			if reservation != nil {
				_ = reservation.close()
			}
			if e == nil {
				t.Fatal("distinct-root same-DB contender admitted")
			}
			seen[stage] = true
		}
		return nil
	}})
	must(t, e)
	if len(seen) != 4 {
		t.Fatal(seen)
	}
}
func TestRecoveryUpgradeCheckpointRepository(t *testing.T) {
	staleUpgradeCheckpoint(t)
	f, b, o := upgradeFixture(t)
	_, e := applyUpgrade(context.Background(), f.config, o, &bindingHooks{event: func(stage string, _ *sql.Tx) error {
		if stage == "after-intent" {
			return errors.New("checkpoint cut")
		}
		return nil
	}})
	if e == nil {
		t.Fatal("missing cut")
	}
	io := importOptionsFor(t, o.Export)
	mutateImportManifest(t, &io, func(m *Manifest) { m.RepositoryID = "repo_" + strings.Repeat("a", 32) })
	path := filepath.Join(b.Request.InstallationDirectory, bindingIntentName)
	raw, e := os.ReadFile(path)
	must(t, e)
	var intent upgradeIntent
	must(t, json.Unmarshal(raw, &intent))
	intent.Result.ManifestSHA256 = io.ManifestSHA256
	raw, e = json.Marshal(intent)
	must(t, e)
	must(t, os.WriteFile(path, raw, 0600))
	before := catalogFingerprint(t, f.db)
	if _, e = ReconcileUpgrade(context.Background(), f.config, o); e == nil {
		t.Fatal("foreign repository checkpoint admitted")
	}
	if catalogFingerprint(t, f.db) != before {
		t.Fatal("changed source")
	}
}

// This scenario is part of the mandatory repository/checkpoint identity case.
func staleUpgradeCheckpoint(t *testing.T) {
	t.Helper()
	f, b, _ := upgradeFixture(t)
	stale := exportOptions(t)
	old, e := Export(context.Background(), f.config, stale)
	must(t, e)
	execSQL(t, f.db, `INSERT INTO repository_config(key,value) VALUES('compression','zstd') ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value`)
	o := upgradeOptions(t, f, b)
	_, e = applyUpgrade(context.Background(), f.config, o, &bindingHooks{event: func(stage string, _ *sql.Tx) error {
		if stage == "after-intent" {
			return errors.New("checkpoint cut")
		}
		return nil
	}})
	if e == nil {
		t.Fatal("missing checkpoint cut")
	}
	path := filepath.Join(b.Request.InstallationDirectory, bindingIntentName)
	intent, e := os.ReadFile(path)
	must(t, e)
	saved := filepath.Join(t.TempDir(), "fresh-checkpoint")
	must(t, os.Rename(o.Export.Destination, saved))
	must(t, copyTreeForImport(stale.Destination, o.Export.Destination))
	before := catalogFingerprint(t, f.db)
	if _, e = ReconcileUpgrade(context.Background(), f.config, o); !errors.Is(e, ErrPackage) {
		t.Fatal("stale same-repository checkpoint admitted", e)
	}
	after, e := os.ReadFile(path)
	must(t, e)
	if string(after) != string(intent) || catalogFingerprint(t, f.db) != before {
		t.Fatal("stale checkpoint changed intent or source")
	}
	t.Logf("SCENARIO stale same-repository checkpoint generation=%s with intact fresh intent: refused before schema mutation", old.Generation)
}

func TestRecoveryUpgradeOperator(t *testing.T) {
	f, _, o := upgradeFixture(t)
	config := readerConfig(t, Destination(f.config))
	q := o.Request
	args := []string{"-config", config, "-installation-directory", q.InstallationDirectory, "-expected-schema", "19", "-repository-id", q.RepositoryID, "-root-id", q.RootID, "-expected-revision", fmt.Sprint(q.ExpectedRevision), "-operation-id", q.OperationID}
	reader := os.Getenv("COLDKEEP_RECOVERY_READER")
	out, e := runReader(t, reader, append([]string{"upgrade-preview"}, args...)...)
	if e != nil {
		t.Fatal(e, string(out))
	}
	var preview UpgradeResult
	must(t, json.Unmarshal(out, &preview))
	if preview.PlanSHA256 != o.PlanSHA256 {
		t.Fatal("operator preview drift")
	}
	args = append(args, "-plan-sha256", o.PlanSHA256, "-checkpoint", o.Export.Destination)
	for _, cmd := range []string{"upgrade-apply", "upgrade-reconcile"} {
		out, e = runReader(t, reader, append([]string{cmd}, args...)...)
		if e != nil {
			t.Fatal(e, string(out))
		}
		var result UpgradeResult
		must(t, json.Unmarshal(out, &result))
		if result.OrdinaryWriterReady || result.OperationID != q.OperationID {
			t.Fatal(result)
		}
	}
}
