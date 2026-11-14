package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

type fixture struct {
	config Config
	db     *sql.DB
	root   string
	key    []byte
}

var fixtureSeq atomic.Int64

func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func execSQL(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	_, e := db.Exec(q, args...)
	must(t, e)
}

// This fixture creates and drops only uniquely named databases on the explicitly
// selected disposable server. Production opening never imports or bootstraps.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	if os.Getenv("COLDKEEP_PHASE1B_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL: COLDKEEP_PHASE1B_TEST=1 and DB_*; run scripts/run_passive_inventory_tests.sh")
	}
	adminDSN, e := db.BuildPostgresConnStringFromEnv("postgres")
	must(t, e)
	admin, e := sql.Open("postgres", adminDSN)
	must(t, e)
	name := fmt.Sprintf("ck_inventory_%d_%d", os.Getpid(), fixtureSeq.Add(1))
	role := name + "_reader"
	execSQL(t, admin, "CREATE DATABASE "+pqIdent(name))
	var conn *sql.DB
	t.Cleanup(func() {
		if conn != nil {
			must(t, conn.Close())
		}
		execSQL(t, admin, "DROP DATABASE "+pqIdent(name)+" WITH (FORCE)")
		execSQL(t, admin, "DROP ROLE IF EXISTS "+pqIdent(role))
		must(t, admin.Close())
	})
	dsn, e := db.BuildPostgresConnStringFromEnv(name)
	must(t, e)
	conn, e = sql.Open("postgres", dsn)
	must(t, e)
	// Apply the checked-in schema directly only during disposable fixture setup.
	schema, e := os.ReadFile("../../db/schema_postgres.sql")
	must(t, e)
	execSQL(t, conn, string(schema))
	names := []string{}
	for _, spec := range catalogTables {
		names = append(names, pqIdent(spec.name))
	}
	execSQL(t, conn, "TRUNCATE "+strings.Join(names, ",")+" CASCADE")
	source := "../storage/testdata/compress_v1_18_supplemental"
	src, e := sql.Open("sqlite3", "file:"+source+"/catalog.db?mode=ro")
	must(t, e)
	defer func() { must(t, src.Close()) }()
	for _, spec := range catalogTables {
		columns := strings.Fields(spec.columns)
		quoted := []string{}
		markers := []string{}
		for k, col := range columns {
			quoted = append(quoted, pqIdent(col))
			markers = append(markers, fmt.Sprint("$", k+1))
		}
		rows, e := src.Query("SELECT " + strings.Join(quoted, ",") + " FROM " + pqIdent(spec.name))
		must(t, e)
		for rows.Next() {
			values := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for k := range values {
				ptrs[k] = &values[k]
			}
			must(t, rows.Scan(ptrs...))
			for k, col := range columns {
				if col == "sealed" || col == "sealing" || col == "quarantine" || col == "is_metadata_complete" {
					values[k] = values[k].(int64) != 0
				}
			}
			execSQL(t, conn, "INSERT INTO "+pqIdent(spec.name)+" ("+strings.Join(quoted, ",")+") VALUES ("+strings.Join(markers, ",")+")", values...)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	root := filepath.Join(t.TempDir(), "payload")
	must(t, os.Mkdir(root, 0700))
	entries, e := os.ReadDir(source + "/containers")
	must(t, e)
	for _, ent := range entries {
		b, e := os.ReadFile(source + "/containers/" + ent.Name())
		must(t, e)
		must(t, os.WriteFile(filepath.Join(root, ent.Name()), b, 0600))
	}
	raw, e := os.ReadFile(source + "/manifest.json")
	must(t, e)
	var km struct {
		Key string `json:"aes_key_hex"`
	}
	must(t, json.Unmarshal(raw, &km))
	key, e := hex.DecodeString(km.Key)
	must(t, e)
	id, e := coordination.ResolveIdentity(root)
	must(t, e)
	owner, e := coordination.NewOwner(coordination.OperationInspect, id, "test", time.Now())
	must(t, e)
	lease, e := coordination.NewCoordinator().Acquire(context.Background(), id, coordination.Request{Operation: coordination.OperationInspect, Mode: coordination.ModeExclusive, Owner: owner})
	must(t, e)
	must(t, lease.Release())
	// Stale metadata is non-authoritative and must survive passive access exactly.
	must(t, os.WriteFile(filepath.Join(root, ".coldkeep-control", "owner.json"), []byte("retained diagnostic bytes\n"), 0600))
	execSQL(t, conn, "CREATE ROLE "+pqIdent(role)+" LOGIN PASSWORD 'inventory_fixture_only'")
	execSQL(t, conn, "GRANT CONNECT ON DATABASE "+pqIdent(name)+" TO "+pqIdent(role))
	execSQL(t, conn, "GRANT USAGE ON SCHEMA public TO "+pqIdent(role))
	execSQL(t, conn, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO "+pqIdent(role))
	port, e := strconv.ParseUint(os.Getenv("DB_PORT"), 10, 16)
	must(t, e)
	c := Config{Backend: "postgres", Profile: "manual-postgresql-docker", PayloadRoot: root, Host: os.Getenv("DB_HOST"), Port: uint16(port),
		Database: name, User: role, Transport: "local-plaintext", Password: func(context.Context) (string, error) { return "inventory_fixture_only", nil },
		KeyReference: "retained-public-test-key", Key: func(context.Context) ([]byte, error) { return key, nil }}
	return &fixture{c, conn, root, key}
}

func catalogFingerprint(t *testing.T, conn *sql.DB) string {
	t.Helper()
	hash := sha256.New()
	// Include schema declarations as well as all table rows. Sorting occurs only
	// in this deliberately small test oracle, never in production enumeration.
	rows, e := conn.Query(`SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='public' ORDER BY 1,2`)
	must(t, e)
	for rows.Next() {
		var a, b, c, d string
		must(t, rows.Scan(&a, &b, &c, &d))
		fmt.Fprintln(hash, a, b, c, d)
	}
	must(t, rows.Err())
	must(t, rows.Close())
	// Include visibility/ownership metadata; SELECT permission alone is not a
	// full-view guarantee. These must be identical across passive inspection.
	for _, query := range []string{
		`SELECT json_build_array(c.relname,c.relowner,c.relacl,c.relrowsecurity,c.relforcerowsecurity)::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' ORDER BY c.relname`,
		`SELECT row_to_json(p)::text FROM pg_policies p WHERE schemaname='public' ORDER BY tablename,policyname`,
		`SELECT json_build_array(nspowner,nspacl)::text FROM pg_namespace WHERE nspname='public'`,
		`SELECT json_build_array(rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconfig)::text FROM pg_roles ORDER BY rolname`,
		`SELECT row_to_json(m)::text FROM pg_auth_members m ORDER BY roleid,member,grantor`,
		`SELECT json_build_array(conname,contype,convalidated,condeferrable,pg_get_constraintdef(oid))::text FROM pg_constraint WHERE connamespace='public'::regnamespace ORDER BY conname`,
	} {
		rows, e := conn.Query(query)
		must(t, e)
		for rows.Next() {
			var value string
			must(t, rows.Scan(&value))
			fmt.Fprintln(hash, value)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	names, e := conn.Query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
	must(t, e)
	tables := []string{}
	for names.Next() {
		var name string
		must(t, names.Scan(&name))
		tables = append(tables, name)
	}
	must(t, names.Err())
	must(t, names.Close())
	for _, name := range tables {
		rows, e := conn.Query("SELECT row_to_json(t)::text FROM public." + pqIdent(name) + " t ORDER BY row_to_json(t)::text")
		must(t, e)
		for rows.Next() {
			var s string
			must(t, rows.Scan(&s))
			fmt.Fprintln(hash, s)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	return hex.EncodeToString(hash.Sum(nil))
}
func treeFingerprint(t *testing.T, root string) string {
	t.Helper()
	h := sha256.New()
	must(t, filepath.WalkDir(root, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		fmt.Fprintln(h, rel, info.Mode(), info.Size())
		if info.Mode().IsRegular() {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			h.Write(b)
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, e := os.Readlink(path)
			if e != nil {
				return e
			}
			fmt.Fprintln(h, target)
		}
		return nil
	}))
	return hex.EncodeToString(h.Sum(nil))
}
func runPreserved(t *testing.T, f *fixture, ctx context.Context, l Limits, h *hooks, consume func(context.Context, *Inventory) error) (Result, error) {
	t.Helper()
	beforeCatalog := catalogFingerprint(t, f.db)
	beforeTree := treeFingerprint(t, f.root)
	r, e := inspect(ctx, f.config, l, consume, h)
	assertNoInventorySessions(t, f.db)
	if got := catalogFingerprint(t, f.db); got != beforeCatalog {
		t.Fatal("catalog/schema changed")
	}
	if got := treeFingerprint(t, f.root); got != beforeTree {
		t.Fatal("payload/control paths or bytes changed")
	}
	return r, e
}
func noConsume(context.Context, *Inventory) error { return nil }
