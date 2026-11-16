package recoveryexport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/container"
	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/generation"
	"github.com/franchoy/coldkeep/v2/internal/storage"
)

func generationLineageQuery() string {
	parts := []string{}
	for _, s := range profileTables(20)[len(catalogTables):] {
		parts = append(parts, "'"+s.name+"',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM public."+pqIdent(s.name)+" t)")
	}
	return "SELECT jsonb_build_object(" + strings.Join(parts, ",") + ")::text"
}
func generationID(t *testing.T, kind string) string {
	t.Helper()
	id, e := db.NewIdentity(kind)
	must(t, e)
	return id
}
func registerGenerationSources(t *testing.T, f *fixture, b BindingOptions) []string {
	t.Helper()
	ids := []string{}
	for range 2 {
		id := generationID(t, "source")
		o := nextBinding(t, f, b, BindingSourceRegister, id, t.TempDir(), 0)
		_, e := ApplyBinding(context.Background(), f.config, o)
		must(t, e)
		ids = append(ids, id)
	}
	o := nextBinding(t, f, b, BindingSourceRebind, ids[0], t.TempDir(), 1)
	_, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	return ids
}

// All fixture record creation remains test-private. No product capture command.
func fixtureGenerationPolicy(t *testing.T, f *fixture, id, source, intent string) {
	t.Helper()
	p := generation.Policy{Version: 1, Intent: intent, Scope: "source-root", SelectionVersion: 1, Rules: []generation.Rule{}}
	var repository string
	must(t, f.db.QueryRow(`SELECT repository_id FROM repository_identity`).Scan(&repository))
	tx, e := f.db.Begin()
	must(t, e)
	defer func() { _ = tx.Rollback() }()
	must(t, insertGenerationPolicy(context.Background(), tx, id, repository, source, p))
	must(t, tx.Commit())
}
func insertGeneration(t *testing.T, f *fixture, id, source, policy string, revisions ...int64) {
	t.Helper()
	var repository string
	must(t, f.db.QueryRow(`SELECT repository_id FROM repository_identity`).Scan(&repository))
	started, ended := int64(1), int64(2)
	mode, mtime := uint32(448), int64(123456789)
	revision := int64(1)
	if len(revisions) > 0 {
		revision = revisions[0]
	}
	g := generation.Record{ID: id, RepositoryID: repository, SourceID: source, SourceRevision: revision, PolicyID: policy, StartedNS: &started, EndedNS: &ended, ScanState: "enumerated", Outcome: "incomplete"}
	tx, e := f.db.Begin()
	must(t, e)
	defer func() { _ = tx.Rollback() }()
	must(t, insertGenerationRecords(context.Background(), tx, g, generation.Root{GenerationID: id, Metadata: generation.Metadata{Mode: &mode, MtimeNS: &mtime}}, nil, DefaultLimits()))
	must(t, tx.Commit())
}
func insertGenerationEntry(t *testing.T, f *fixture, gid, id string, parent any, name []byte, kind, outcome string, file any) {
	t.Helper()
	execSQL(t, f.db, `INSERT INTO generation_entry VALUES($1,$2,$3,'directory',$4,$5,$6,$7,416,123456789)`, gid, id, parent, name, kind, outcome, file)
}
func generationFixture(t *testing.T) (*fixture, BindingOptions, UpgradeOptions, []string, []string) {
	t.Helper()
	f, b, _ := upgradeFixture(t)
	sources := registerGenerationSources(t, f, b)
	o := upgradeOptions(t, f, b)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	gens := []string{}
	for n, source := range sources {
		p := generationID(t, "policy")
		intent := "capture"
		if n == 1 {
			intent = "archival-import"
		}
		fixtureGenerationPolicy(t, f, p, source, intent)
		g := generationID(t, "generation")
		insertGeneration(t, f, g, source, p)
		gens = append(gens, g)
	}
	return f, b, o, sources, gens
}
func generationInventory(t *testing.T, f *fixture) *Inventory {
	t.Helper()
	var inv *Inventory
	_, e := Inspect(context.Background(), f.config, DefaultLimits(), func(_ context.Context, i *Inventory) error { inv = i; return nil })
	must(t, e)
	return inv
}
func TestRecoveryGenerationModel(t *testing.T) {
	f, _, _, sources, gens := generationFixture(t)
	before := legacyRowsFingerprint(t, f.db)
	for _, g := range gens {
		root := generationID(t, "entry")
		insertGenerationEntry(t, f, g, root, nil, []byte("dir"), "directory", "stored", nil)
		for _, name := range [][]byte{[]byte("Same"), []byte("same"), []byte(" white "), []byte("é"), []byte("é"), []byte("\\\n"), []byte("�"), {0xff, 0xfe}, incompressibleComponent()} {
			insertGenerationEntry(t, f, g, generationID(t, "entry"), root, name, "file", "stored", 2)
		}
		insertGenerationEntry(t, f, g, generationID(t, "entry"), nil, []byte("empty directory"), "directory", "stored", nil)
		insertGenerationEntry(t, f, g, generationID(t, "entry"), nil, []byte("empty file"), "file", "stored", 1)
		for _, outcome := range []string{"excluded", "missing", "unreadable", "unstable", "unsupported"} {
			insertGenerationEntry(t, f, g, generationID(t, "entry"), nil, []byte(outcome), "file", outcome, nil)
		}
	}
	p := generationID(t, "policy")
	fixtureGenerationPolicy(t, f, p, sources[0], "archival-import")
	insertGeneration(t, f, generationID(t, "generation"), sources[0], p, 2)
	var version, blockSize string
	var keyBytes, componentBytes int
	var uncompressed bool
	must(t, f.db.QueryRow(`SELECT current_setting('server_version'),current_setting('block_size'),pg_column_size(ROW(generation_id,parent_id,component)),octet_length(component),pg_column_compression(component) IS NULL FROM generation_entry WHERE octet_length(component)=255 LIMIT 1`).Scan(&version, &blockSize, &keyBytes, &componentBytes, &uncompressed))
	if componentBytes != 255 || !uncompressed || keyBytes >= 2704 {
		t.Fatal("maximum component key not proven", keyBytes, uncompressed)
	}
	t.Logf("maximum composite component key tuple=%d raw_component=%d uncompressed=%v PostgreSQL=%s block_size=%s", keyBytes, componentBytes, uncompressed, version, blockSize)
	generationInventory(t, f)
	if before != legacyRowsFingerprint(t, f.db) {
		t.Fatal("legacy counters/rows changed")
	}
	// Duplicate insert never replaces a revision or its meaning.
	if _, e := f.db.Exec(`INSERT INTO generation_policy SELECT policy_id,repository_id,source_id,descriptor,descriptor_sha256 FROM generation_policy WHERE policy_id=$1`, p); e == nil {
		t.Fatal("policy rewritten")
	}
}
func TestRecoveryGenerationConstraints(t *testing.T) {
	f, _, _, sources, gens := generationFixture(t)
	g := gens[0]
	dir := generationID(t, "entry")
	insertGenerationEntry(t, f, g, dir, nil, []byte("parent"), "directory", "stored", nil)
	file := generationID(t, "entry")
	insertGenerationEntry(t, f, g, file, nil, []byte("file"), "file", "stored", 2)
	cases := map[string]struct {
		parent        any
		name          []byte
		kind, outcome string
		file          any
	}{
		"duplicate-root": {nil, []byte("parent"), "directory", "stored", nil}, "file-parent": {file, []byte("child"), "directory", "stored", nil}, "directory-recipe": {nil, []byte("d"), "directory", "stored", 2}, "file-missing-recipe": {nil, []byte("f"), "file", "stored", nil}, "dangling-recipe": {nil, []byte("f"), "file", "stored", 999999}, "excluded-recipe": {nil, []byte("f"), "file", "excluded", 2}, "component-over": {nil, bytes.Repeat([]byte("x"), 256), "directory", "stored", nil}, "traversal": {nil, []byte(".."), "directory", "stored", nil}, "slash": {nil, []byte("a/b"), "directory", "stored", nil}, "nul": {nil, []byte{0}, "directory", "stored", nil},
	}
	other := generationID(t, "entry")
	insertGenerationEntry(t, f, gens[1], other, nil, []byte("foreign"), "directory", "stored", nil)
	cases["foreign-parent"] = struct {
		parent        any
		name          []byte
		kind, outcome string
		file          any
	}{other, []byte("x"), "directory", "stored", nil}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := f.db.Exec(`INSERT INTO generation_entry VALUES($1,$2,$3,'directory',$4,$5,$6,$7,NULL,NULL)`, g, generationID(t, "entry"), c.parent, c.name, c.kind, c.outcome, c.file); e == nil {
				t.Fatal("constraint admitted invalid row")
			}
		})
	}
	for name, q := range map[string]string{
		"policy-mutation":     `UPDATE generation_policy SET descriptor=convert_to('{}','UTF8')`,
		"source-substitution": fmt.Sprintf(`UPDATE capture_generation SET source_id='%s' WHERE generation_id='%s'`, sources[1], g),
		"published":           `UPDATE capture_generation SET publication_receipt='invented'`,
		"successful-capture":  `UPDATE capture_generation SET outcome='complete'`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := f.db.Exec(q); e == nil {
				t.Fatal("constraint admitted substitution")
			}
		})
	}
	generationInventory(t, f)
}
func TestRecoveryGenerationHierarchyLimits(t *testing.T) {
	f, _, _, _, gens := generationFixture(t)
	g := gens[0]
	var parent any
	first := ""
	for n := 0; n < 128; n++ {
		id := generationID(t, "entry")
		if n == 0 {
			first = id
		}
		insertGenerationEntry(t, f, g, id, parent, []byte("x"), "directory", "stored", nil)
		parent = id
	}
	generationInventory(t, f)
	extra := generationID(t, "entry")
	insertGenerationEntry(t, f, g, extra, parent, []byte("x"), "directory", "stored", nil)
	if _, e := Inspect(context.Background(), f.config, DefaultLimits(), noConsume); e == nil {
		t.Fatal("depth overrun")
	}
	execSQL(t, f.db, `DELETE FROM generation_entry WHERE entry_id=$1`, extra)
	execSQL(t, f.db, `UPDATE generation_entry SET parent_id=$1 WHERE entry_id=$2`, parent, first)
	if _, e := Inspect(context.Background(), f.config, DefaultLimits(), noConsume); e == nil {
		t.Fatal("cycle")
	}
	execSQL(t, f.db, `UPDATE generation_entry SET parent_id=NULL WHERE entry_id=$1`, first)
	// 12*255 + 11 separators + 1 separator + 1 byte = 3073. Lower one byte.
	parent = nil
	last := ""
	for n := 0; n < 12; n++ {
		name := bytes.Repeat([]byte{0xfe}, 255)
		if n == 11 {
			name = name[:254]
		}
		last = generationID(t, "entry")
		insertGenerationEntry(t, f, gens[1], last, parent, name, "directory", "stored", nil)
		parent = last
	}
	leaf := generationID(t, "entry")
	insertGenerationEntry(t, f, gens[1], leaf, parent, []byte("x"), "file", "stored", 2)
	generationInventory(t, f)
	execSQL(t, f.db, `UPDATE generation_entry SET component=$1 WHERE entry_id=$2`, bytes.Repeat([]byte{0xfe}, 255), last)
	if _, e := Inspect(context.Background(), f.config, DefaultLimits(), noConsume); e == nil {
		t.Fatal("path overrun")
	}
}
func TestRecoveryGenerationAdmissionBounds(t *testing.T) {
	f, _, _, _, gens := generationFixture(t)
	insertGenerationEntry(t, f, gens[0], generationID(t, "entry"), nil, []byte("x"), "file", "stored", 2)
	inv := generationInventory(t, f)
	for _, limit := range []Limits{func() Limits { v := DefaultLimits(); v.Rows = inv.Rows - 1; return v }(), func() Limits { v := DefaultLimits(); v.CatalogBytes = inv.CatalogBytes - 1; return v }()} {
		if _, e := Inspect(context.Background(), f.config, limit, noConsume); e == nil {
			t.Fatal("lower budget ignored")
		}
	}
	for name, q := range map[string]string{"zero-column-table": `CREATE TABLE public.hidden()`, "optional-not-null": `ALTER TABLE generation_root ALTER COLUMN mode SET NOT NULL`, "constraint-removed": `ALTER TABLE generation_policy DROP CONSTRAINT generation_policy_source_id_fkey`, "extra-column": `ALTER TABLE generation_entry ADD COLUMN extra TEXT`, "substituted-check": `ALTER TABLE capture_generation DROP CONSTRAINT capture_generation_outcome_check; ALTER TABLE capture_generation ADD CONSTRAINT capture_generation_outcome_check CHECK (outcome <> 'success')`, "oversized-constraint": `ALTER TABLE generation_policy ADD CONSTRAINT oversized CHECK (policy_id <> '` + strings.Repeat("x", 5000) + `')`} {
		t.Run(name, func(t *testing.T) {
			tx, e := f.db.Begin()
			must(t, e)
			defer func() { must(t, tx.Rollback()) }()
			_, e = tx.Exec(q)
			must(t, e)
			got := &Inventory{Limits: DefaultLimits(), Catalog: map[string][]Row{}}
			if e = readCatalog(context.Background(), tx, got, nil); e == nil {
				t.Fatal("structural corruption admitted")
			}
		})
	}
}
func TestRecoveryGenerationRoundTrip(t *testing.T) {
	for _, plain := range []bool{true, false} {
		t.Run(fmt.Sprintf("plaintext=%v", plain), func(t *testing.T) {
			f, files := recoveryHistory(t, plain)
			key := stageRecoveryKey(t, f)
			// Arrange a generation-only recipe before the migration baseline.
			execSQL(t, f.db, `DELETE FROM snapshot_file WHERE logical_file_id=100`)
			files = files[:len(files)-1]
			b := bindHistory(t, f)
			sources := registerGenerationSources(t, f, b)
			o := upgradeOptions(t, f, b)
			_, e := ApplyUpgrade(context.Background(), f.config, o)
			must(t, e)
			policy, gid := generationID(t, "policy"), generationID(t, "generation")
			fixtureGenerationPolicy(t, f, policy, sources[0], "capture")
			insertGeneration(t, f, gid, sources[0], policy)
			// New recipe has no physical-file or historical-snapshot reference.
			var refs int
			must(t, f.db.QueryRow(`SELECT (SELECT count(*) FROM physical_file WHERE logical_file_id=100)+(SELECT count(*) FROM snapshot_file WHERE logical_file_id=100)`).Scan(&refs))
			if refs != 0 {
				t.Fatal("recipe not generation-only")
			}
			insertGenerationEntry(t, f, gid, generationID(t, "entry"), nil, []byte{0xff, 'x'}, "file", "stored", 100)
			insertGenerationEntry(t, f, gid, generationID(t, "entry"), nil, []byte("empty-directory"), "directory", "stored", nil)
			var lineage string
			must(t, f.db.QueryRow(generationLineageQuery()).Scan(&lineage))
			generationInventory(t, f)
			post := exportOptions(t)
			_, e = Export(context.Background(), f.config, post)
			must(t, e)
			io := importOptionsFor(t, post)
			d, open := importTarget(t, f)
			result, e := Import(context.Background(), d, io)
			must(t, e)
			if !result.Ready || result.Receipt.Kind != "coldkeep-import-ready/v4" {
				t.Fatal(result)
			}
			c := open()
			var got string
			must(t, c.QueryRow(generationLineageQuery()).Scan(&got))
			if got != lineage {
				t.Fatal("generation lineage changed")
			}
			id, e := coordination.ResolveIdentity(d.PayloadRoot)
			must(t, e)
			lease, e := coordination.AcquireExisting(context.Background(), id)
			must(t, e)
			rc, e := OpenRecoverySession(context.Background(), Config(d), lease, *result.Receipt)
			must(t, e)
			if !plain {
				t.Setenv("COLDKEEP_KEY", hex.EncodeToString(f.key))
			}
			output := filepath.Join(t.TempDir(), "generation-only.bin")
			must(t, storage.RestoreFileWithStorageContext(storage.StorageContext{DB: rc.DB, ContainerDir: d.PayloadRoot}, 100, output))
			must(t, rc.Close())
			must(t, lease.Release())
			data, e := os.ReadFile(output)
			must(t, e)
			hash := sha256.Sum256(data)
			var expected string
			must(t, f.db.QueryRow(`SELECT file_hash FROM logical_file WHERE id=100`).Scan(&expected))
			if hex.EncodeToString(hash[:]) != expected {
				t.Fatal("generation-only content differs")
			}
			if _, e = ApplyUpgrade(context.Background(), Config(d), o); e == nil {
				t.Fatal("import activated")
			}
			clear(f.key)
			t.Setenv("COLDKEEP_KEY", "")
			isolatedRecovery(t, f, post, files, key, fmt.Sprintf("phase3c1-schema20-plain-%v", plain), b.Request.InstallationDirectory)
		})
	}
}
func TestRecoveryGenerationWriters(t *testing.T) {
	f, _, o := upgradeFixture(t)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	for _, label := range []string{"d19", "pre3c", "current"} {
		for _, borrowed := range []string{"0", "1"} {
			t.Run(label+"/borrowed="+borrowed, func(t *testing.T) {
				before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
				cmd := exec.Command(filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), label+"-writer"))
				cmd.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true", "COLDKEEP_PROBE_BORROWED="+borrowed)
				out, e := cmd.CombinedOutput()
				if e == nil || (label != "d19" || borrowed != "1") && !strings.Contains(string(out), "future version") {
					t.Fatal("ordinary writer admitted", e, string(out))
				}
				if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
					t.Fatal("writer mutated schema20")
				}
			})
		}
	}
}

func incompressibleComponent() []byte {
	b := make([]byte, 255)
	for n := range b {
		b[n] = byte(n + 1)
		if b[n] == '/' {
			b[n] = 0xfe
		}
	}
	return b
}

func TestRecoveryGenerationImmutableRevisions(t *testing.T) {
	f, _, _, sources, gens := generationFixture(t)
	policy := generationID(t, "policy")
	fixtureGenerationPolicy(t, f, policy, sources[0], "archival-import")
	raw, e := (generation.Policy{Version: 1, Intent: "archival-import", Scope: "source-root", SelectionVersion: 1, Rules: []generation.Rule{}}).Canonical()
	must(t, e)
	if _, e = f.db.Exec(`UPDATE generation_policy SET descriptor=$1,descriptor_sha256=$2 WHERE policy_id=(SELECT policy_id FROM capture_generation WHERE generation_id=$3)`, raw, generation.Digest(raw), gens[0]); e == nil {
		t.Fatal("referenced canonical policy rewritten")
	}
	before := catalogFingerprint(t, f.db)
	tx, e := f.db.Begin()
	must(t, e)
	var repo string
	must(t, tx.QueryRow(`SELECT repository_id FROM repository_identity`).Scan(&repo))
	g := generation.Record{ID: gens[0], RepositoryID: repo, SourceID: sources[0], SourceRevision: 1, PolicyID: policy, ScanState: "unknown", Outcome: "unknown"}
	e = insertGenerationRecords(context.Background(), tx, g, generation.Root{GenerationID: g.ID}, nil, DefaultLimits())
	if e == nil {
		t.Fatal("same-source policy ref substituted")
	}
	must(t, tx.Rollback())
	if catalogFingerprint(t, f.db) != before {
		t.Fatal("revision mutation")
	}
}
func TestRecoveryGenerationMaximumPolicy(t *testing.T) {
	f, _, _, sources, _ := generationFixture(t)
	p := generation.Policy{Version: 1, Intent: "capture", Scope: "source-root", SelectionVersion: 1, Rules: []generation.Rule{}}
	for n := 0; n < 10; n++ {
		size := 255
		if n == 9 {
			size = 105
		}
		p.Rules = append(p.Rules, generation.Rule{Action: "include", Selection: generation.Selector{Kind: "posix-bytes-base64", Components: []string{base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), size))}}})
	}
	raw, e := p.Canonical()
	must(t, e)
	if len(raw) != 4096 {
		t.Fatal(len(raw))
	}
	tx, e := f.db.Begin()
	must(t, e)
	var repo string
	must(t, tx.QueryRow(`SELECT repository_id FROM repository_identity`).Scan(&repo))
	must(t, insertGenerationPolicy(context.Background(), tx, generationID(t, "policy"), repo, sources[0], p))
	must(t, tx.Commit())
	inv := generationInventory(t, f)
	t.Logf("maximum canonical policy raw=%d catalog JSON bytes=%d rows=%d", len(raw), inv.CatalogBytes, inv.Rows)
}
func TestRecoveryGenerationRLSAndFieldGuards(t *testing.T) {
	f, _, _, _, gens := generationFixture(t)
	insertGenerationEntry(t, f, gens[0], generationID(t, "entry"), nil, []byte("x"), "file", "stored", 2)
	execSQL(t, f.db, `ALTER TABLE generation_entry ENABLE ROW LEVEL SECURITY; ALTER TABLE generation_entry FORCE ROW LEVEL SECURITY; CREATE POLICY hidden_generation ON generation_entry USING(false)`)
	if _, e := Inspect(context.Background(), f.config, DefaultLimits(), noConsume); e == nil {
		t.Fatal("filtered catalog admitted")
	}
	execSQL(t, f.db, `DROP POLICY hidden_generation ON generation_entry; ALTER TABLE generation_entry DISABLE ROW LEVEL SECURITY; ALTER TABLE generation_entry NO FORCE ROW LEVEL SECURITY`)
	for _, kind := range []string{"component", "policy"} {
		t.Run(kind, func(t *testing.T) {
			tx, e := f.db.Begin()
			must(t, e)
			defer func() { _ = tx.Rollback() }()
			q := `ALTER TABLE generation_entry DROP CONSTRAINT generation_entry_component_check; UPDATE generation_entry SET component=decode(repeat('ff',256),'hex')`
			if kind == "policy" {
				q = `ALTER TABLE generation_policy DISABLE TRIGGER aaaa_coldkeep_schema20_fence; ALTER TABLE generation_policy DROP CONSTRAINT generation_policy_descriptor_check; ALTER TABLE generation_policy DROP CONSTRAINT generation_policy_check; UPDATE generation_policy SET descriptor=decode(repeat('61',5000),'hex')`
			}
			_, e = tx.Exec(q)
			must(t, e)
			inv := &Inventory{Limits: DefaultLimits(), Catalog: map[string][]Row{}}
			if e = readCatalog(context.Background(), tx, inv, nil); !errors.Is(e, ErrLimit) {
				t.Fatal("field guard failed", e)
			}
		})
	}
}
func TestRecoveryGenerationProfileRefusals(t *testing.T) {
	f, _, o := upgradeFixture(t)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
	for _, kind := range []string{"package", "reader", "future-schema", "repository"} {
		t.Run(kind, func(t *testing.T) {
			io := importOptionsFor(t, post)
			copy := filepath.Join(t.TempDir(), "bad-package")
			must(t, copyTreeForImport(io.Package, copy))
			io.Package = copy
			mutateImportManifest(t, &io, func(m *Manifest) {
				switch kind {
				case "package":
					m.Version = 3
				case "reader":
					m.Reader = bindingReader
				case "future-schema":
					m.CatalogVersion = 21
				case "repository":
					m.RepositoryID = ""
				}
			})
			d, _ := importTarget(t, f)
			r, e := Import(context.Background(), d, io)
			if e == nil || r.Ready {
				t.Fatal("profile admitted")
			}
			if _, e = os.Stat(d.PayloadRoot); !os.IsNotExist(e) {
				t.Fatal("premature target allocation")
			}
		})
	}
}

// Retain the originally reproduced D19 mutation as a required negative control.
func TestRecoveryGenerationD19BorrowedMutation(t *testing.T) {
	f, b, _ := upgradeFixture(t)
	execSQL(t, f.db, `INSERT INTO repository_config(key,value) VALUES('compression','zstd') ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value`)
	o := upgradeOptions(t, f, b)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	before := catalogFingerprint(t, f.db)
	cmd := exec.Command(filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), "d19-writer"))
	cmd.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true", "COLDKEEP_PROBE_BORROWED=1")
	out, callErr := cmd.CombinedOutput()
	var value string
	must(t, f.db.QueryRow(`SELECT value FROM repository_config WHERE key='compression'`).Scan(&value))
	changed := before != catalogFingerprint(t, f.db)
	t.Logf("untouched D19 supplied-handle call error=%v compression before=zstd after=%s catalog_changed=%v output=%s", callErr, value, changed, out)
	if callErr == nil || changed || !strings.Contains(string(out), "SCHEMA20_FENCE_CONFIRMED") {
		t.Fatal("required D19 supplied-handle refusal failed")
	}
}

func TestRecoveryGenerationFenceAdmission(t *testing.T) {
	f, _, o := upgradeFixture(t)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	cases := map[string]string{
		"missing":        `DROP TRIGGER aaaa_coldkeep_schema20_fence ON chunk`,
		"disabled":       `ALTER TABLE chunk DISABLE TRIGGER aaaa_coldkeep_schema20_fence`,
		"extra-earlier":  `CREATE TRIGGER a000_extra BEFORE UPDATE ON chunk FOR EACH STATEMENT EXECUTE FUNCTION coldkeep_schema20_writer_fence()`,
		"changed-body":   `CREATE OR REPLACE FUNCTION coldkeep_schema20_writer_fence() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'`,
		"oversized-body": `CREATE OR REPLACE FUNCTION coldkeep_schema20_writer_fence() RETURNS trigger LANGUAGE plpgsql AS $x$BEGIN /*` + strings.Repeat("x", 5000) + `*/ RETURN NEW; END$x$`,
		"legacy-body":    `CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'`,
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			tx, e := f.db.Begin()
			must(t, e)
			defer func() { _ = tx.Rollback() }()
			_, e = tx.Exec(q)
			must(t, e)
			if e = validateGenerationFence(context.Background(), tx); e == nil {
				t.Fatal("changed fence admitted")
			}
		})
	}
	generationInventory(t, f)
}
func generationSequenceFingerprint(t *testing.T, f *fixture) string {
	t.Helper()
	var value string
	must(t, f.db.QueryRow(`SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY sequencename),'[]'::jsonb)::text FROM pg_sequences s WHERE schemaname='public'`).Scan(&value))
	return value
}
func TestRecoveryGenerationFenceStatements(t *testing.T) {
	f, _, o := upgradeFixture(t)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	before, seq, tree := catalogFingerprint(t, f.db), generationSequenceFingerprint(t, f), treeFingerprint(t, f.root)
	for name, q := range map[string]string{
		"empty-update":          `UPDATE chunk SET pin_count=pin_count+1 WHERE false`,
		"pin-without-authority": `UPDATE chunk SET pin_count=pin_count+1`,
		"sequence-insert":       `INSERT INTO container(filename,current_size,max_size) VALUES('denied.bin',0,1024)`,
		"truncate":              `TRUNCATE generation_policy CASCADE`,
		"delete":                `DELETE FROM logical_file WHERE false`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := f.db.Exec(q); e == nil {
				t.Fatal("mutation admitted")
			}
		})
	}
	if before != catalogFingerprint(t, f.db) || seq != generationSequenceFingerprint(t, f) || tree != treeFingerprint(t, f.root) {
		t.Fatal("denied statement mutated catalog, sequence or payload")
	}
	// Exercise only the private SQL transition; product authority comes from the
	// dedicated validated import reader, qualified by the two round trips.
	for name, q := range map[string]string{
		"jump":                  `UPDATE chunk SET pin_count=pin_count+2`,
		"other-column":          `UPDATE chunk SET pin_count=pin_count+1,live_ref_count=live_ref_count+1`,
		"unchanged":             `UPDATE chunk SET pin_count=pin_count`,
		"delete-with-authority": `DELETE FROM chunk WHERE false`,
	} {
		t.Run(name, func(t *testing.T) {
			tx, e := f.db.Begin()
			must(t, e)
			defer func() { _ = tx.Rollback() }()
			_, e = tx.Exec(`SET LOCAL coldkeep.recovery_pin_authority='schema20-v1'`)
			must(t, e)
			if _, e = tx.Exec(q); e == nil {
				t.Fatal("overbroad pin exception")
			}
		})
	}
	tx, e := f.db.Begin()
	must(t, e)
	_, e = tx.Exec(`SET LOCAL coldkeep.recovery_pin_authority='schema20-v1'`)
	must(t, e)
	_, e = tx.Exec(`UPDATE chunk SET pin_count=pin_count+1 WHERE id=1`)
	must(t, e)
	_, e = tx.Exec(`UPDATE chunk SET pin_count=pin_count-1 WHERE id=1`)
	must(t, e)
	must(t, tx.Rollback())
	if before != catalogFingerprint(t, f.db) || seq != generationSequenceFingerprint(t, f) {
		t.Fatal("private pin transition rollback changed source")
	}
}

func TestRecoveryGenerationD19Operations(t *testing.T) {
	f, b, _ := upgradeFixture(t)
	var original string
	must(t, f.db.QueryRow(`SELECT filename FROM container ORDER BY id LIMIT 1`).Scan(&original))
	header, e := os.ReadFile(filepath.Join(f.root, original))
	must(t, e)
	must(t, os.WriteFile(filepath.Join(f.root, "catalog-orphan.bin"), header[:64], 0600))
	reuse := filepath.Join(t.TempDir(), "restored-nonempty.bin")
	must(t, storage.RestoreFileWithStorageContext(storage.StorageContext{DB: f.db, ContainerDir: f.root}, 2, reuse))
	reusedBytes, e := os.ReadFile(reuse)
	must(t, e)
	if len(reusedBytes) == 0 {
		t.Fatal("reuse must exercise nonempty recipe pins")
	}
	execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) VALUES(999,'catalog-orphan.bin',64,67108864)`)
	must(t, container.SealContainerInDir(f.db, 999, "catalog-orphan.bin", f.root))
	o := upgradeOptions(t, f, b)
	_, e = ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	for _, action := range []string{"store-new", "store-reuse", "gc", "repair"} {
		t.Run(action, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "source.bin")
			data := []byte("phase3c1 denied fresh content")
			if action == "store-reuse" {
				data = reusedBytes
			}
			must(t, os.WriteFile(source, data, 0600))
			if action == "repair" {
				// Deliberate privileged fault after migration forces both public repair
				// targets to attempt updates; this is not an admitted recovery profile.
				execSQL(t, f.db, `ALTER TABLE logical_file DISABLE TRIGGER aaaa_coldkeep_schema20_fence; UPDATE logical_file SET ref_count=ref_count+1; ALTER TABLE logical_file ENABLE TRIGGER aaaa_coldkeep_schema20_fence; ALTER TABLE chunk DISABLE TRIGGER aaaa_coldkeep_schema20_fence; ALTER TABLE chunk DISABLE TRIGGER aaaa_coldkeep_schema20_pin_rows; UPDATE chunk SET live_ref_count=live_ref_count+1; ALTER TABLE chunk ENABLE TRIGGER aaaa_coldkeep_schema20_pin_rows; ALTER TABLE chunk ENABLE TRIGGER aaaa_coldkeep_schema20_fence`)
			}
			before, seq, tree := catalogFingerprint(t, f.db), generationSequenceFingerprint(t, f), treeFingerprint(t, f.root)
			cmd := exec.Command(filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), "d19-writer"))
			cmd.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true", "COLDKEEP_PROBE_BORROWED=1", "COLDKEEP_PROBE_ACTION="+action, "COLDKEEP_PROBE_SOURCE="+source)
			out, e := cmd.CombinedOutput()
			t.Logf("D19 action=%s error=%v output=%s", action, e, out)
			if e == nil || !strings.Contains(string(out), "SCHEMA20_FENCE_CONFIRMED") {
				t.Fatal("required fence refusal missing")
			}
			if before != catalogFingerprint(t, f.db) || seq != generationSequenceFingerprint(t, f) || tree != treeFingerprint(t, f.root) {
				t.Fatal("mutator changed catalog, sequence or payload")
			}
		})
	}
}

func TestRecoveryGenerationD19PositiveOperations(t *testing.T) {
	for _, action := range []string{"store-new", "store-reuse", "gc", "repair"} {
		t.Run(action, func(t *testing.T) {
			f := exportFixture(t)
			source := filepath.Join(t.TempDir(), "source.bin")
			must(t, os.WriteFile(source, []byte("phase3c1 positive old writer content"), 0600))
			if action == "store-reuse" {
				must(t, os.Remove(source))
				must(t, storage.RestoreFileWithStorageContext(storage.StorageContext{DB: f.db, ContainerDir: f.root}, 2, source))
			}
			if action == "gc" {
				var name string
				must(t, f.db.QueryRow(`SELECT filename FROM container ORDER BY id LIMIT 1`).Scan(&name))
				data, e := os.ReadFile(filepath.Join(f.root, name))
				must(t, e)
				must(t, os.WriteFile(filepath.Join(f.root, "catalog-orphan.bin"), data[:64], 0600))
				execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) VALUES(999,'catalog-orphan.bin',64,67108864)`)
				must(t, container.SealContainerInDir(f.db, 999, "catalog-orphan.bin", f.root))
			}
			if action == "repair" {
				execSQL(t, f.db, `UPDATE logical_file SET ref_count=ref_count+1; UPDATE chunk SET live_ref_count=live_ref_count+1`)
			}
			before := catalogFingerprint(t, f.db)
			cmd := exec.Command(filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), "d19-writer"))
			cmd.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true", "COLDKEEP_PROBE_BORROWED=1", "COLDKEEP_PROBE_ACTION="+action, "COLDKEEP_PROBE_SOURCE="+source, "COLDKEEP_KEY="+hex.EncodeToString(f.key))
			out, e := cmd.CombinedOutput()
			t.Logf("D19 schema17 positive action=%s error=%v output=%s", action, e, out)
			if e != nil || before == catalogFingerprint(t, f.db) || action == "repair" && !strings.Contains(string(out), "OK:2 Failed:0") {
				t.Fatal("actual old-writer mutation positive control failed")
			}
		})
	}
}

func TestRecoveryGenerationReaderCleanup(t *testing.T) {
	f, _, o := upgradeFixture(t)
	_, e := ApplyUpgrade(context.Background(), f.config, o)
	must(t, e)
	post := exportOptions(t)
	_, e = Export(context.Background(), f.config, post)
	must(t, e)
	d, open := importTarget(t, f)
	result, e := Import(context.Background(), d, importOptionsFor(t, post))
	must(t, e)
	target := open()
	id, e := coordination.ResolveIdentity(d.PayloadRoot)
	must(t, e)
	for _, kind := range []string{"cancellation", "rollback-error", "close-error"} {
		t.Run(kind, func(t *testing.T) {
			lease, e := coordination.AcquireExisting(context.Background(), id)
			must(t, e)
			defer func() { _ = lease.Release() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("recovery reader cleanup fault")
			gates := newCleanupDriver(t, nil, nil)
			if kind == "rollback-error" {
				gates.rollbackErr = injected
			}
			if kind == "close-error" {
				gates.closeErr = injected
			}
			h := &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}
			reader, e := openRecoverySession(ctx, Config(d), lease, *result.Receipt, h)
			must(t, e)
			before := catalogFingerprint(t, target)
			tx, e := reader.BeginTx(ctx, nil)
			must(t, e)
			_, e = tx.ExecContext(ctx, `UPDATE chunk SET pin_count=pin_count+1 WHERE id=1`)
			must(t, e)
			cancel()
			done := make(chan struct{})
			var closeErr error
			go func() { closeErr = reader.Close(); _ = lease.Release(); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			awaitCleanup(t, gates.rollbackStarted.ch)
			assertCleanupExclusion(t, id)
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			assertCleanupExclusion(t, id)
			select {
			case <-done:
				t.Fatal("reader released ownership before physical close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if kind != "cancellation" && !errors.Is(closeErr, injected) {
				t.Fatal("cleanup error lost", closeErr)
			}
			if before != catalogFingerprint(t, target) {
				t.Fatal("canceled recovery pin survived rollback")
			}
			must(t, waitBindingNoSessions(context.Background(), func(ctx context.Context) (int, error) { return countBindingSessions(ctx, target) }))
		})
	}
}
