//go:build linux && cgo

package proofdaemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/container"
	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/snapshot"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"github.com/franchoy/coldkeep/v2/internal/webui"
)

func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func fixture(t *testing.T, dir string) Config {
	t.Helper()
	must(t, os.Chmod(dir, 0700))
	source := filepath.Join("..", "storage", "testdata", "compress_v1_18_supplemental")
	must(t, filepath.WalkDir(source, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(source, p)
		if e != nil {
			return e
		}
		out := filepath.Join(dir, rel)
		if d.IsDir() {
			return os.MkdirAll(out, 0700)
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		return os.WriteFile(out, b, 0600)
	}))
	for _, n := range []string{"output", "runtime"} {
		must(t, os.Mkdir(filepath.Join(dir, n), 0700))
	}
	var keys struct {
		Key string `json:"aes_key_hex"`
	}
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	must(t, e)
	must(t, json.Unmarshal(b, &keys))
	must(t, os.WriteFile(filepath.Join(dir, "key"), []byte(keys.Key), 0600))
	conn, e := sql.Open("sqlite3", filepath.Join(dir, "catalog.db"))
	must(t, e)
	must(t, snapshot.InsertSnapshot(context.Background(), conn, snapshot.Snapshot{ID: "proof-history", Type: "full", CreatedAt: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)}))
	for _, entry := range []struct {
		name     string
		id, size int64
	}{{"history/Case.txt", 2, 4}, {"history/empty.txt", 1, 0}, {"history/encrypted.bin", 3, 4096}} {
		_, e := snapshot.InsertSnapshotFile(context.Background(), conn, snapshot.SnapshotFile{SnapshotID: "proof-history", Path: entry.name, LogicalFileID: entry.id, Size: sql.NullInt64{Int64: entry.size, Valid: true}, Mode: sql.NullInt64{Int64: 0640, Valid: true}, MTime: sql.NullTime{Time: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), Valid: true}})
		must(t, e)
	}
	must(t, conn.Close())
	must(t, coordination.PrepareExisting(context.Background(), filepath.Join(dir, "containers")))
	return Config{FixtureOnly: true, Backend: "sqlite", Catalog: filepath.Join(dir, "catalog.db"), PayloadRoot: filepath.Join(dir, "containers"), Snapshot: "proof-history", KeyFile: filepath.Join(dir, "key"), RestoreRoot: filepath.Join(dir, "output"), RuntimeDir: filepath.Join(dir, "runtime"), Listen: "127.0.0.1:18766", PublicURL: "http://127.0.0.1:18766"}
}
func TestProofFixture(t *testing.T) {
	dir := os.Getenv("COLDKEEP_PROOF_FIXTURE")
	if dir == "" {
		dir = t.TempDir()
	} else {
		must(t, os.Mkdir(dir, 0700))
	}
	c := fixture(t, dir)
	if addr := os.Getenv("COLDKEEP_PROOF_ADDRESS"); addr != "" {
		c.Listen = addr
		c.PublicURL = "http://" + addr
	}
	b, e := json.MarshalIndent(c, "", "  ")
	must(t, e)
	must(t, os.WriteFile(filepath.Join(dir, "config.json"), b, 0600))
	t.Log("PROOF_EVENT fixture_old_writer_copied")
}
func TestProofRuntimeHistoryRestore(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	c := fixture(t, t.TempDir())
	r, e := Open(c)
	must(t, e)
	defer func() { must(t, r.Close()) }()
	expected := map[string]string{"history/Case.txt": "8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c", "history/empty.txt": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "history/encrypted.bin": "6b1d4bfe25f0144686cade6a6f77f222e6ef929b949a9d688e418d56dd3af609"}
	if len(r.entries) != 3 {
		t.Fatal("history count")
	}
	for _, entry := range r.entries {
		result := r.restore(entry.ID)
		if result.Outcome != "completed" {
			t.Fatal(result)
		}
		b, e := os.ReadFile(filepath.Join(c.RestoreRoot, entry.Name))
		must(t, e)
		h := sha256.Sum256(b)
		if hex.EncodeToString(h[:]) != expected[entry.Name] {
			t.Fatal("oracle mismatch")
		}
		st, e := os.Stat(filepath.Join(c.RestoreRoot, entry.Name))
		must(t, e)
		if st.Mode().Perm() != 0640 || st.ModTime().Unix() != 1577934245 {
			t.Fatal("metadata mismatch")
		}
	}
	if r.restore(r.entries[0].ID).Outcome != "failed_or_partial" {
		t.Fatal("collision was not refused")
	}
	r.gate <- struct{}{}
	if r.restore(r.entries[0].ID).Code != "operation_busy" {
		t.Fatal("admission")
	}
	<-r.gate
	t.Log("PROOF_EVENT engine_history_restore_and_collision")
}
func TestProofLeaseBeforeCatalogAndProcess(t *testing.T) {
	if path := os.Getenv("COLDKEEP_PROOF_CONTENDER"); path != "" {
		c, e := ReadConfig(path)
		must(t, e)
		r, e := Open(c)
		if r != nil {
			_ = r.Close()
		}
		if !errors.Is(e, coordination.ErrRepositoryBusy) {
			t.Fatalf("want busy before invalid catalog, got %v", e)
		}
		return
	}
	t.Setenv("COLDKEEP_KEY", "")
	dir := t.TempDir()
	c := fixture(t, dir)
	r, e := Open(c)
	must(t, e)
	// Contender points to an existing invalid catalog. Busy must win, proving
	// lock acquisition precedes database/schema construction in another process.
	bad := filepath.Join(dir, "invalid.db")
	must(t, os.WriteFile(bad, []byte("invalid sqlite"), 0600))
	other := c
	other.Catalog = bad
	b, e := json.Marshal(other)
	must(t, e)
	p := filepath.Join(dir, "contender.json")
	must(t, os.WriteFile(p, b, 0600))
	cmd := exec.Command(os.Args[0], "-test.run=^TestProofLeaseBeforeCatalogAndProcess$")
	cmd.Env = append(os.Environ(), "COLDKEEP_PROOF_CONTENDER="+p)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("contender: %v %s", e, out)
	}
	must(t, r.Close())
	id, e := coordination.ResolveIdentity(c.PayloadRoot)
	must(t, e)
	lease, e := coordination.AcquireExisting(context.Background(), id)
	must(t, e)
	must(t, lease.Release())
	if _, e = Open(other); e == nil {
		t.Fatal("invalid catalog opened")
	}
	lease, e = coordination.AcquireExisting(context.Background(), id)
	must(t, e)
	must(t, lease.Release())
	t.Log("PROOF_EVENT lease_before_catalog_process_and_failure_cleanup")
}
func TestProofEnvelope(t *testing.T) {
	for _, kind := range []string{"rows", "size", "name", "backend", "missing", "output", "unprepared", "wrong-key", "wrong-root"} {
		t.Run(kind, func(t *testing.T) {
			c := fixture(t, t.TempDir())
			conn, e := sql.Open("sqlite3", c.Catalog)
			must(t, e)
			switch kind {
			case "rows":
				for i := 0; i < MaxEntries; i++ {
					_, e = conn.Exec("INSERT INTO snapshot_path(path) VALUES(?)", randomToken())
					must(t, e)
				}
			case "size":
				_, e = conn.Exec("UPDATE logical_file SET total_size=16777217 WHERE id=2")
				must(t, e)
			case "name":
				_, e = conn.Exec("UPDATE snapshot_path SET path='trailing ' WHERE path='history/Case.txt'")
				must(t, e)
			case "backend":
				c.Backend = ""
			case "missing":
				c.Catalog += ".absent"
			case "output":
				must(t, os.WriteFile(filepath.Join(c.RestoreRoot, "existing"), []byte("preserve"), 0600))
			case "wrong-key":
				must(t, os.WriteFile(c.KeyFile, []byte(strings.Repeat("cd", 32)), 0600))
			case "wrong-root":
				entries, e := os.ReadDir(c.PayloadRoot)
				must(t, e)
				for _, f := range entries {
					if !f.IsDir() {
						must(t, os.Remove(filepath.Join(c.PayloadRoot, f.Name())))
					}
				}
			case "unprepared":
				must(t, os.Remove(filepath.Join(c.PayloadRoot, coordination.ControlDirectoryName, coordination.LockArtifactName)))
			}
			must(t, conn.Close())
			r, e := Open(c)
			if e == nil {
				_ = r.Close()
				t.Fatal("unsafe fixture admitted")
			}
		})
	}
	t.Log("PROOF_EVENT catalog_actual_read_bounds_and_tuple_refusals")
}
func TestProofHTTP(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	c := fixture(t, t.TempDir())
	r, e := Open(c)
	must(t, e)
	defer func() { must(t, r.Close()) }()
	s, e := NewService(r, &webui.Bundle{})
	must(t, e)
	defer func() { must(t, s.Close()) }()
	request := func(method, path, body, token, host, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, c.PublicURL+path, strings.NewReader(body))
		req.Host = host
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		return w
	}
	for _, v := range []struct {
		name, host, origin, token string
		status                    int
	}{{"missing", c.Listen, c.PublicURL, "", 401}, {"wrong", c.Listen, c.PublicURL, strings.Repeat("x", 64), 401}, {"host", "evil.invalid", c.PublicURL, "", 403}, {"origin", c.Listen, "http://evil.invalid", "", 403}, {"no_origin", c.Listen, "", "", 403}} {
		t.Run(v.name, func(t *testing.T) {
			w := request("POST", "/api/v1/restore", `{"entry_id":"anything"}`, v.token, v.host, v.origin)
			if w.Code != v.status {
				t.Fatal(w.Code, w.Body)
			}
		})
	}
	entries, e := os.ReadDir(c.RestoreRoot)
	must(t, e)
	if len(entries) != 0 {
		t.Fatal("unauthenticated mutation")
	}
	boot := s.bootstrap
	s.bootstrapUntil = time.Now().Add(-time.Second)
	body := `{"secret":"` + boot + `"}`
	if request("POST", "/api/v1/session", body, "", c.Listen, c.PublicURL).Code != 401 {
		t.Fatal("expiry")
	}
	s.bootstrapUntil = time.Now().Add(time.Minute)
	w := request("POST", "/api/v1/session", body, "", c.Listen, c.PublicURL)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	var session SessionResult
	must(t, json.Unmarshal(w.Body.Bytes(), &session))
	if request("POST", "/api/v1/session", body, "", c.Listen, c.PublicURL).Code != 401 {
		t.Fatal("replay")
	}
	if _, e = os.Stat(s.bootstrapFile); !os.IsNotExist(e) {
		t.Fatal("bootstrap not retired")
	}
	for _, v := range []struct {
		method, path, body string
		status             int
	}{{"GET", "/api/v1/status", "", 200}, {"GET", "/api/v1/capabilities", "", 200}, {"GET", "/api/v1/history", "", 200}, {"GET", "/api/v1/history?limit=0", "", 400}, {"GET", "/api/v1/history?cursor=bogus", "", 400}, {"GET", "/api/v1/jobs", "", 501}, {"GET", "/api/v1/search", "", 501}, {"GET", "/api/v1/unknown", "", 404}, {"DELETE", "/api/v1/history", "", 405}, {"POST", "/api/v1/restore", strings.Repeat("x", MaxBody+1), 400}} {
		w := request(v.method, v.path, v.body, session.Token, c.Listen, c.PublicURL)
		if w.Code != v.status || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
			t.Fatalf("%s: %d %s", v.path, w.Code, w.Body)
		}
	}
	for i := 0; i < MaxRequests; i++ {
		s.requests <- struct{}{}
	}
	if request("GET", "/api/v1/status", "", session.Token, c.Listen, c.PublicURL).Code != 503 {
		t.Fatal("request bound")
	}
	for i := 0; i < MaxRequests; i++ {
		<-s.requests
	}
	s.sessionUntil = time.Now().Add(-time.Second)
	if request("GET", "/api/v1/status", "", session.Token, c.Listen, c.PublicURL).Code != 401 {
		t.Fatal("session expiry")
	}
	t.Log("PROOF_EVENT auth_origin_host_expiry_replay_no_mutation_bounds")
}
func TestProofNativeSQLitePersistence(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload")
	must(t, os.Mkdir(payload, 0700))
	must(t, coordination.PrepareExisting(context.Background(), payload))
	id, e := coordination.ResolveIdentity(payload)
	must(t, e)
	lease, e := coordination.AcquireExisting(context.Background(), id)
	must(t, e)
	conn, e := sql.Open("sqlite3", filepath.Join(dir, "native.db")+"?_foreign_keys=on&_busy_timeout=1000")
	must(t, e)
	conn.SetMaxOpenConns(1)
	must(t, db.RunMigrations(conn))
	var sqlite string
	must(t, conn.QueryRow("SELECT sqlite_version()").Scan(&sqlite))
	t.Logf("PROOF_SQLITE_RUNTIME %s", sqlite)
	source := filepath.Join(dir, "known.txt")
	must(t, os.WriteFile(source, []byte("Coldkeep phase 2 native persistence\n"), 0600))
	writer := container.NewLocalWriterWithDirAndDB(payload, 1<<20, conn)
	sc := storage.StorageContext{DB: conn, Writer: writer, ContainerDir: payload}
	eng, e := engine.New(engine.Config{DB: conn, ContainerDir: payload, StoreContext: &sc})
	must(t, e)
	stored, e := eng.Store(context.Background(), engine.StoreRequest{SourcePath: source, Codec: "plain"})
	must(t, e)
	must(t, sc.Close())
	must(t, lease.Release())
	lease, e = coordination.AcquireExisting(context.Background(), id)
	must(t, e)
	defer func() { must(t, lease.Release()) }()
	conn, e = sql.Open("sqlite3", filepath.Join(dir, "native.db")+"?mode=rw&_foreign_keys=on")
	must(t, e)
	defer func() { must(t, conn.Close()) }()
	conn.SetMaxOpenConns(1)
	sc = storage.StorageContext{DB: conn, ContainerDir: payload}
	eng, e = engine.New(engine.Config{DB: conn, ContainerDir: payload, StoreContext: &sc})
	must(t, e)
	outputRoot := filepath.Join(dir, "restored")
	must(t, os.Mkdir(outputRoot, 0700))
	restored, e := eng.Restore(context.Background(), engine.RestoreRequest{FileIDs: []int64{stored.LogicalFileID}, DestinationRoot: outputRoot})
	must(t, e)
	if len(restored.Items) != 1 {
		t.Fatal("restore result count")
	}
	output := restored.Items[0].DestinationPath
	b, e := os.ReadFile(output)
	must(t, e)
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != "64ab254c8aba6ce0e03aedb1f29fbbe2f6bbe07372e0a8527712dfd07925bd4f" {
		t.Fatal("native persisted output mismatch")
	}
	t.Logf("PROOF_EVENT sqlite_real_localwriter_close_reopen_restore hash=%x", sum)
}

var _ http.Handler = (*Service)(nil)

// This explicit fixture setup copies existing catalog rows and payload bytes
// into a disposable PostgreSQL database. It is not a product conversion API.
func TestProofPostgresFixture(t *testing.T) {
	target := os.Getenv("COLDKEEP_PROOF_PG_CONFIG")
	if target == "" {
		t.Skip("explicit fixture preparation helper; Docker runner supplies the private configuration and requires its event")
	}
	raw, e := os.ReadFile(target)
	must(t, e)
	var config Config
	must(t, json.Unmarshal(raw, &config))
	source := fixture(t, t.TempDir())
	sqlite, e := sql.Open("sqlite3", source.Catalog)
	must(t, e)
	defer func() { must(t, sqlite.Close()) }()
	pg, e := sql.Open("postgres", config.Catalog)
	must(t, e)
	defer func() { must(t, pg.Close()) }()
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true") // explicit disposable fixture creation only
	must(t, db.EnsurePostgresSchema(pg))
	// Copy order follows existing foreign keys. Only fixed table/column names
	// from the immutable retained fixture are used by this test-only adapter.
	for _, table := range []string{"container", "chunk", "logical_file", "physical_file", "file_chunk", "blocks", "storage_blocks", "chunk_block_refs", "snapshot", "snapshot_path", "snapshot_file"} {
		rows, e := sqlite.Query("SELECT * FROM " + table)
		must(t, e)
		columns, e := rows.Columns()
		must(t, e)
		placeholders := make([]string, len(columns))
		for i := range columns {
			placeholders[i] = "$" + fmt.Sprint(i+1)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			must(t, rows.Scan(pointers...))
			for i, col := range columns {
				if col == "sealed" || col == "sealing" || col == "quarantine" {
					if v, ok := values[i].(int64); ok {
						values[i] = v != 0
					}
				}
			}
			_, e = pg.Exec("INSERT INTO "+table+" ("+strings.Join(columns, ",")+") VALUES ("+strings.Join(placeholders, ",")+")", values...)
			must(t, e)
		}
		must(t, rows.Err())
		must(t, rows.Close())
	}
	entries, e := os.ReadDir(source.PayloadRoot)
	must(t, e)
	for _, f := range entries {
		if f.IsDir() {
			continue
		}
		b, e := os.ReadFile(filepath.Join(source.PayloadRoot, f.Name()))
		must(t, e)
		must(t, os.WriteFile(filepath.Join(config.PayloadRoot, f.Name()), b, 0600))
	}
	key, e := os.ReadFile(source.KeyFile)
	must(t, e)
	must(t, os.WriteFile(config.KeyFile, key, 0600))
	must(t, coordination.PrepareExisting(context.Background(), config.PayloadRoot))
	t.Log("PROOF_EVENT postgres_old_writer_fixture_prepared")
}

func TestProofDisconnectDrain(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	c := fixture(t, t.TempDir())
	r, e := Open(c)
	must(t, e)
	s, e := NewService(r, &webui.Bundle{})
	must(t, e)
	defer func() { must(t, s.Close()) }()
	s.session = randomToken()
	s.sessionUntil = time.Now().Add(time.Minute)
	entered, release := make(chan struct{}), make(chan struct{})
	storage.ConfigureRestoreTestHooksForTesting(r.storage, func(*sql.DB, int64) error { close(entered); <-release; return nil }, nil)
	server := httptest.NewUnstartedServer(s)
	server.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var selected string
	for _, entry := range r.entries {
		if entry.Name == "history/Case.txt" {
			selected = entry.ID
		}
	}
	req, e := http.NewRequestWithContext(ctx, "POST", server.URL+"/api/v1/restore", strings.NewReader(`{"entry_id":"`+selected+`"}`))
	must(t, e)
	req.Host = c.Listen
	req.Header.Set("Origin", c.PublicURL)
	req.Header.Set("Authorization", "Bearer "+s.session)
	req.Header.Set("Content-Type", "application/json")
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, _ := server.Client().Do(req)
		if response != nil {
			_ = response.Body.Close()
		}
	}()
	<-entered
	cancel()
	<-clientDone
	done := make(chan error, 1)
	go func() { e := server.Config.Shutdown(context.Background()); done <- errors.Join(e, r.Close()) }()
	select {
	case <-done:
		t.Fatal("shutdown escaped active operation")
	case <-time.After(30 * time.Millisecond):
	}
	id, e := coordination.ResolveIdentity(c.PayloadRoot)
	must(t, e)
	lease, e := coordination.AcquireExisting(context.Background(), id)
	if e == nil {
		_ = lease.Release()
		t.Fatal("ownership escaped active handler")
	}
	close(release)
	must(t, <-done)
	server.Close()
	bytes, e := os.ReadFile(filepath.Join(c.RestoreRoot, "history/Case.txt"))
	must(t, e)
	if string(bytes) != "tiny" {
		t.Fatal("disconnect lost controlled completion")
	}
	lease, e = coordination.AcquireExisting(context.Background(), id)
	must(t, e)
	must(t, lease.Release())
	t.Log("PROOF_EVENT disconnect_continues_shutdown_joins_before_release")
}
