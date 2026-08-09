package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G6 — Safe concurrent storage operations
//
// Adversarial goals:
//   - repository coordination must reject known independent-process overlap
//   - repeated same-file CLI stores must converge on deterministic chunk graphs
//   - lower-layer concurrent store/remove/GC activity must not corrupt metadata or graph shape
//   - mixed concurrent operations must preserve healthy restores for surviving files
//   - verification invariants must remain true after each stress phase
//
// Notes:
//   - This file uses the current Postgres-backed adversarial harness.
//   - It runs a codec matrix for plain + aes-gcm where data-path behavior matters.
//   - Independent-process contention uses an explicit holder READY protocol;
//     storage-level interleaving tests retain the internal concurrency evidence.

func adversarialG6Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func requireDeterministicG6Env(name string) bool {
	return strings.TrimSpace(os.Getenv(name)) == "1"
}

type g6DeterministicInterleavingGate struct {
	eventCh   chan storage.TestStoreInterleavingHookEvent
	releaseCh chan struct{}
	once      sync.Once
}

func newG6DeterministicInterleavingGate() *g6DeterministicInterleavingGate {
	return &g6DeterministicInterleavingGate{
		eventCh:   make(chan storage.TestStoreInterleavingHookEvent, 1),
		releaseCh: make(chan struct{}),
	}
}

func (g *g6DeterministicInterleavingGate) await(t *testing.T) storage.TestStoreInterleavingHookEvent {
	t.Helper()
	select {
	case event := <-g.eventCh:
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for deterministic G6 interleaving gate")
		return storage.TestStoreInterleavingHookEvent{}
	}
}

func (g *g6DeterministicInterleavingGate) release() {
	g.once.Do(func() {
		close(g.releaseCh)
	})
}

func assertDeterministicG6ChunkState(t *testing.T, dbconn *sql.DB, chunkHash string, size int, wantLogicalRefs int) {
	t.Helper()
	chunkID, chunkStatus := loadDeterministicG6Chunk(t, dbconn, chunkHash, size)
	if chunkStatus != "COMPLETED" {
		t.Fatalf("expected deterministic G6 chunk to be COMPLETED, got %s", chunkStatus)
	}
	assertDeterministicG6MappingCounts(t, dbconn, chunkID)
	assertDeterministicG6ReferenceCounts(t, dbconn, chunkID, wantLogicalRefs)
}

func loadDeterministicG6Chunk(t *testing.T, dbconn *sql.DB, chunkHash string, size int) (int64, string) {
	t.Helper()
	var chunkID int64
	var chunkStatus string
	if err := dbconn.QueryRow(
		`SELECT id, status FROM chunk WHERE chunk_hash = $1 AND size = $2`,
		chunkHash,
		size,
	).Scan(&chunkID, &chunkStatus); err != nil {
		t.Fatalf("load deterministic G6 chunk state: %v", err)
	}
	return chunkID, chunkStatus
}

func assertDeterministicG6MappingCounts(t *testing.T, dbconn *sql.DB, chunkID int64) {
	t.Helper()
	var packedRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1`, chunkID).Scan(&packedRows); err != nil {
		t.Fatalf("count packed rows: %v", err)
	}
	if packedRows != 1 {
		t.Fatalf("expected one packed row for deterministic G6 chunk, got %d", packedRows)
	}

	var legacyRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, chunkID).Scan(&legacyRows); err != nil {
		t.Fatalf("count legacy rows: %v", err)
	}
	if legacyRows != 1 {
		t.Fatalf("expected one legacy companion row for deterministic G6 chunk, got %d", legacyRows)
	}
}

func assertDeterministicG6ReferenceCounts(t *testing.T, dbconn *sql.DB, chunkID int64, wantLogicalRefs int) {
	t.Helper()
	var logicalRefs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, chunkID).Scan(&logicalRefs); err != nil {
		t.Fatalf("count logical refs: %v", err)
	}
	if logicalRefs != wantLogicalRefs {
		t.Fatalf("expected logical refs=%d for deterministic G6 chunk, got %d", wantLogicalRefs, logicalRefs)
	}

	var physicalRefs int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM physical_file pf
		 WHERE pf.logical_file_id IN (
		   SELECT DISTINCT logical_file_id FROM file_chunk WHERE chunk_id = $1
		 )`,
		chunkID,
	).Scan(&physicalRefs); err != nil {
		t.Fatalf("count physical refs: %v", err)
	}
	if physicalRefs != 1 {
		t.Fatalf("expected one physical-file ref for deterministic G6 chunk, got %d", physicalRefs)
	}
}

func configureAdversarialG6Codec(t *testing.T, codec string) {
	t.Helper()
	t.Setenv("COLDKEEP_CODEC", codec)
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG6Env(t *testing.T) (*sql.DB, map[string]string, string, string, string, string) {
	t.Helper()

	tmp, err := os.MkdirTemp("", "coldkeep-adversarial-g6-*")
	if err != nil {
		t.Fatalf("mkdir temp root: %v", err)
	}
	origContainersDir := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainersDir })
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	cfg := loadAdversarialG6PostgresTestConfig()
	adminDB := openAdversarialG6PostgresConnection(t, cfg, getenvOrDefaultAdversarialG6("COLDKEEP_TEST_DB_MAINTENANCE", "postgres"), "admin")
	testDBName := fmt.Sprintf("coldkeep_adversarial_g6_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		t.Fatalf("create temporary postgres adversarial g6 database %s: %v", testDBName, err)
	}
	t.Setenv("DB_NAME", testDBName)

	env := testutils.DefaultCLIEnv(container.ContainersDir)
	for k, v := range env {
		t.Setenv(k, v)
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	t.Cleanup(func() {
		_ = dbconn.Close()
		preserveFailureState := t.Failed() && testutils.PreserveFailureStateEnabled()
		if preserveFailureState {
			t.Logf("preserving G6 diagnostic state: db=%s containers_dir=%s temp_root=%s", testDBName, container.ContainersDir, tmp)
			_ = adminDB.Close()
			return
		}
		_, _ = adminDB.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, testDBName)
		_, _ = adminDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
		_ = adminDB.Close()
		_ = os.RemoveAll(tmp)
	})

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)

	return dbconn, env, repoRoot, binPath, tmp, testDBName
}

type adversarialG6DeterministicFixture struct {
	dbconn    *sql.DB
	tmp       string
	inPath    string
	payload   []byte
	chunkHash string
}

func setupAdversarialG6DeterministicFixture(t *testing.T, codec, suffix string) adversarialG6DeterministicFixture {
	t.Helper()

	dbconn, _, _, _, tmp, _ := setupAdversarialG6Env(t)
	inputDir := filepath.Join(tmp, "input-deterministic")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir deterministic input dir: %v", err)
	}

	payload := []byte("g6-deterministic-controlled-interleaving-payload")
	inPath := filepath.Join(inputDir, fmt.Sprintf("g6-deterministic-%s-%s.bin", codec, suffix))
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write deterministic input: %v", err)
	}

	sum := sha256.Sum256(payload)
	return adversarialG6DeterministicFixture{
		dbconn:    dbconn,
		tmp:       tmp,
		inPath:    inPath,
		payload:   payload,
		chunkHash: hex.EncodeToString(sum[:]),
	}
}

func adversarialG6StoreCodec(codec string) blocks.Codec {
	if codec == "aes-gcm" {
		return blocks.CodecAESGCM
	}
	return blocks.CodecPlain
}

func runAdversarialG6DeterministicCase(
	t *testing.T,
	name string,
	codec string,
	target storage.TestStoreInterleavingEvent,
) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		fixture := setupAdversarialG6DeterministicFixture(t, codec, name)
		defer fixture.dbconn.Close()

		sgctx, err := storage.LoadDefaultStorageContext()
		if err != nil {
			t.Fatalf("load default storage context: %v", err)
		}
		defer func() { _ = sgctx.Close() }()
		gate := installAdversarialG6DeterministicGate(t, &sgctx, target, fixture.chunkHash)
		done := startAdversarialG6DeterministicStore(sgctx, fixture.inPath, codec)

		event := gate.await(t)
		if event.StoreOpID == "" {
			t.Fatal("expected deterministic G6 event to include store op id")
		}
		assertDeterministicG6PreCommitRows(t, fixture.dbconn)

		gate.release()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("deterministic postgres store failed: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("timeout waiting for deterministic postgres store")
		}

		assertDeterministicG6ChunkState(t, fixture.dbconn, fixture.chunkHash, len(fixture.payload), 1)
		if err := verify.VerifyRepository(fixture.dbconn, container.ContainersDir); err != nil {
			t.Fatalf("full verify repository after deterministic postgres interleaving: %v", err)
		}
	})
}

func installAdversarialG6DeterministicGate(
	t *testing.T,
	sgctx *storage.StorageContext,
	target storage.TestStoreInterleavingEvent,
	chunkHash string,
) *g6DeterministicInterleavingGate {
	t.Helper()

	gate := newG6DeterministicInterleavingGate()
	var fired bool
	resetHooks := storage.InstallTestStoreInterleavingHooks(sgctx, func(_ context.Context, event storage.TestStoreInterleavingHookEvent) error {
		if fired || event.Event != target || event.ChunkHash != chunkHash {
			return nil
		}
		fired = true
		gate.eventCh <- event
		<-gate.releaseCh
		return nil
	})
	t.Cleanup(resetHooks)
	t.Cleanup(gate.release)
	return gate
}

func startAdversarialG6DeterministicStore(sgctx storage.StorageContext, inPath, codec string) chan error {
	done := make(chan error, 1)
	go func() {
		_, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, adversarialG6StoreCodec(codec))
		done <- err
	}()
	return done
}

func assertDeterministicG6PreCommitRows(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var packedRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&packedRows); err != nil {
		t.Fatalf("count chunk_block_refs before commit: %v", err)
	}
	if packedRows != 0 {
		t.Fatalf("expected no committed packed refs before release, got %d", packedRows)
	}

	var legacyRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&legacyRows); err != nil {
		t.Fatalf("count blocks before commit: %v", err)
	}
	if legacyRows != 0 {
		t.Fatalf("expected no committed legacy rows before release, got %d", legacyRows)
	}
}

func runAdversarialG6DeterministicRetryCase(t *testing.T, codec string) {
	t.Helper()

	t.Run("retry_path_remains_packed", func(t *testing.T) {
		fixture := setupAdversarialG6DeterministicFixture(t, codec, "retry")
		defer fixture.dbconn.Close()

		resetDeterministicRetryChunkState(t, fixture.dbconn)
		chunkID := seedDeterministicRetryChunk(t, fixture.dbconn, fixture.chunkHash, len(fixture.payload))
		events := runDeterministicRetryStore(t, codec, fixture.inPath)
		assertDeterministicRetryEvents(t, chunkID, events)
		assertDeterministicG6ChunkState(t, fixture.dbconn, fixture.chunkHash, len(fixture.payload), 1)
		if err := verify.VerifyRepository(fixture.dbconn, container.ContainersDir); err != nil {
			t.Fatalf("full verify repository after deterministic postgres retry case: %v", err)
		}
	})
}

func resetDeterministicRetryChunkState(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	for _, query := range []string{
		`DELETE FROM file_chunk`,
		`DELETE FROM logical_file`,
		`DELETE FROM chunk_block_refs`,
		`DELETE FROM blocks`,
		`DELETE FROM storage_blocks`,
		`DELETE FROM chunk`,
	} {
		if _, err := dbconn.Exec(query); err != nil {
			t.Fatalf("reset deterministic retry chunk state: %v", err)
		}
	}
}

func seedDeterministicRetryChunk(t *testing.T, dbconn *sql.DB, chunkHash string, size int) int64 {
	t.Helper()

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, 0, 0, $4)
		 RETURNING id`,
		chunkHash,
		size,
		"ABORTED",
		"v2-fastcdc",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert aborted retry chunk: %v", err)
	}
	return chunkID
}

func runDeterministicRetryStore(t *testing.T, codec, inPath string) []storage.TestStoreInterleavingHookEvent {
	t.Helper()

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		t.Fatalf("load default storage context for retry case: %v", err)
	}
	defer func() { _ = sgctx.Close() }()

	var events []storage.TestStoreInterleavingHookEvent
	resetHooks := storage.InstallTestStoreInterleavingHooks(&sgctx, func(_ context.Context, event storage.TestStoreInterleavingHookEvent) error {
		events = append(events, event)
		return nil
	})
	t.Cleanup(resetHooks)

	if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, adversarialG6StoreCodec(codec)); err != nil {
		t.Fatalf("store retry-case file: %v", err)
	}
	return events
}

func assertDeterministicRetryEvents(t *testing.T, chunkID int64, events []storage.TestStoreInterleavingHookEvent) {
	t.Helper()
	seen := deterministicRetryEventSet(events, chunkID)
	if !seen.retryCAS || !seen.packedMetadata || !seen.legacyCompanion {
		t.Fatalf(
			"expected retry path to remain packed on postgres, got retry=%t packed=%t companion=%t events=%+v",
			seen.retryCAS,
			seen.packedMetadata,
			seen.legacyCompanion,
			events,
		)
	}
}

type deterministicRetryEvents struct {
	retryCAS        bool
	packedMetadata  bool
	legacyCompanion bool
}

func deterministicRetryEventSet(events []storage.TestStoreInterleavingHookEvent, chunkID int64) deterministicRetryEvents {
	var seen deterministicRetryEvents
	for _, event := range events {
		if event.ChunkID != chunkID {
			continue
		}
		switch event.Event {
		case storage.TestStoreInterleavingEventBeforeChunkRetryCAS:
			seen.retryCAS = true
		case storage.TestStoreInterleavingEventAfterPackedMetadata:
			seen.packedMetadata = true
		case storage.TestStoreInterleavingEventAfterLegacyCompanionInsert:
			seen.legacyCompanion = true
		}
	}
	return seen
}

type adversarialG6PostgresTestConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	SSLMode  string
}

func loadAdversarialG6PostgresTestConfig() adversarialG6PostgresTestConfig {
	return adversarialG6PostgresTestConfig{
		Host:     getenvOrDefaultAdversarialG6("DB_HOST", "127.0.0.1"),
		Port:     getenvOrDefaultAdversarialG6("DB_PORT", "5432"),
		User:     getenvOrDefaultAdversarialG6("DB_USER", "coldkeep"),
		Password: getenvOrDefaultAdversarialG6("DB_PASSWORD", "coldkeep"),
		SSLMode:  getenvOrDefaultAdversarialG6("DB_SSLMODE", "disable"),
	}
}

func openAdversarialG6PostgresConnection(t *testing.T, cfg adversarialG6PostgresTestConfig, databaseName, purpose string) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("postgres", adversarialG6PostgresConnString(cfg, databaseName))
	if err != nil {
		t.Fatalf("open postgres %s connection: %v", purpose, err)
	}
	if err := dbconn.Ping(); err != nil {
		_ = dbconn.Close()
		t.Fatalf("ping postgres %s connection: %v", purpose, err)
	}
	return dbconn
}

func adversarialG6PostgresConnString(cfg adversarialG6PostgresTestConfig, databaseName string) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, databaseName, cfg.SSLMode,
	)
}

func getenvOrDefaultAdversarialG6(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func storeFileWithCodecCLIG6(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json")

	payload := testutils.AssertCLIJSONOK(
		t,
		res,
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

const (
	g6LeaseHolderEnv     = "COLDKEEP_G6_REPOSITORY_LEASE_HOLDER"
	g6LeaseHolderReady   = "READY"
	g6LeaseHolderRelease = "RELEASE"
	g6LeaseHolderDone    = "RELEASED"
)

// TestAdversarialG6RepositoryLeaseHolderProcess is a helper-process entry
// point. The parent test executes this test binary in a separate OS process
// with g6LeaseHolderEnv set, so acquisition uses the production Coordinator
// while synchronization remains test-only.
func TestAdversarialG6RepositoryLeaseHolderProcess(t *testing.T) {
	if strings.TrimSpace(os.Getenv(g6LeaseHolderEnv)) != "1" {
		return
	}

	identity, err := coordination.ResolveIdentity(os.Getenv("COLDKEEP_STORAGE_DIR"))
	if err != nil {
		t.Fatalf("resolve holder repository identity: %v", err)
	}
	owner, err := coordination.NewOwner(coordination.OperationStore, identity, "phase13a-test", time.Now())
	if err != nil {
		t.Fatalf("create holder owner metadata: %v", err)
	}
	lease, err := coordination.NewCoordinator().Acquire(context.Background(), identity, coordination.Request{
		Operation: coordination.OperationStore,
		Mode:      coordination.ModeExclusive,
		Owner:     owner,
	})
	if err != nil {
		t.Fatalf("acquire holder repository Lease: %v", err)
	}
	released := false
	defer func() {
		if !released {
			_ = lease.Release()
		}
	}()

	if _, err := fmt.Fprintln(os.Stdout, g6LeaseHolderReady); err != nil {
		t.Fatalf("signal holder readiness: %v", err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			t.Fatalf("read holder release signal: %v", err)
		}
		t.Fatal("holder release signal stream closed before RELEASE")
	}
	if signal := strings.TrimSpace(scanner.Text()); signal != g6LeaseHolderRelease {
		t.Fatalf("holder release signal=%q want %q", signal, g6LeaseHolderRelease)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("release holder repository Lease: %v", err)
	}
	released = true
	if _, err := fmt.Fprintln(os.Stdout, g6LeaseHolderDone); err != nil {
		t.Fatalf("signal holder release: %v", err)
	}
}

type g6RepositoryLeaseHolder struct {
	command  *exec.Cmd
	stdin    io.WriteCloser
	lines    chan string
	wait     chan error
	stderr   *bytes.Buffer
	finished bool
}

func startG6RepositoryLeaseHolder(t *testing.T, env map[string]string) *g6RepositoryLeaseHolder {
	t.Helper()

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve G6 test binary: %v", err)
	}
	holderEnv := make(map[string]string, len(env)+1)
	for key, value := range env {
		holderEnv[key] = value
	}
	// The holder does not open the database. Removing this gate prevents the
	// adversarial package TestMain from creating a second isolated PostgreSQL
	// database inside the helper process.
	holderEnv["COLDKEEP_TEST_DB"] = ""
	holderEnv[g6LeaseHolderEnv] = "1"

	command := exec.Command(testBinary, "-test.run=^TestAdversarialG6RepositoryLeaseHolderProcess$", "-test.count=1")
	command.Env = testutils.BuildCommandEnv(holderEnv)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("create holder stdin pipe: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		t.Fatalf("create holder stdout pipe: %v", err)
	}
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		t.Fatalf("start repository Lease holder: %v", err)
	}

	holder := &g6RepositoryLeaseHolder{
		command: command,
		stdin:   stdin,
		lines:   make(chan string, 16),
		wait:    make(chan error, 1),
		stderr:  stderr,
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			holder.lines <- strings.TrimSpace(scanner.Text())
		}
		close(holder.lines)
	}()
	go func() {
		holder.wait <- command.Wait()
	}()
	t.Cleanup(func() { holder.cleanup(t) })
	holder.waitForLine(t, g6LeaseHolderReady)
	return holder
}

func (holder *g6RepositoryLeaseHolder) waitForLine(t *testing.T, want string) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case line, ok := <-holder.lines:
			if !ok {
				select {
				case err := <-holder.wait:
					holder.finished = true
					t.Fatalf("repository Lease holder exited before %q: %v; stderr=%s", want, err, holder.stderr.String())
				case <-time.After(2 * time.Second):
					t.Fatalf("repository Lease holder output closed before %q", want)
				}
			}
			if line == want {
				return
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for repository Lease holder signal %q", want)
		}
	}
}

func (holder *g6RepositoryLeaseHolder) release(t *testing.T) {
	t.Helper()
	if holder.finished {
		t.Fatal("repository Lease holder exited before release")
	}
	if _, err := fmt.Fprintln(holder.stdin, g6LeaseHolderRelease); err != nil {
		t.Fatalf("send repository Lease holder release: %v", err)
	}
	if err := holder.stdin.Close(); err != nil {
		t.Fatalf("close repository Lease holder stdin: %v", err)
	}
	holder.waitForLine(t, g6LeaseHolderDone)
	select {
	case err := <-holder.wait:
		holder.finished = true
		if err != nil {
			t.Fatalf("repository Lease holder exit: %v; stderr=%s", err, holder.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for repository Lease holder exit")
	}
}

func (holder *g6RepositoryLeaseHolder) kill(t *testing.T) {
	t.Helper()
	if holder.finished {
		t.Fatal("repository Lease holder exited before intentional kill")
	}
	if holder.command.Process == nil {
		t.Fatal("repository Lease holder has no process to kill")
	}
	if err := holder.command.Process.Kill(); err != nil {
		t.Fatalf("kill repository Lease holder: %v", err)
	}
	_ = holder.stdin.Close()

	select {
	case err := <-holder.wait:
		holder.finished = true
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("repository Lease holder wait error=%T %v want killed-process ExitError; stderr=%s", err, err, holder.stderr.String())
		}
		if exitErr.ProcessState == nil || exitErr.ProcessState.Success() {
			t.Fatalf("repository Lease holder process state=%v want unsuccessful killed exit; stderr=%s", exitErr.ProcessState, holder.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for killed repository Lease holder exit")
	}
}

func (holder *g6RepositoryLeaseHolder) cleanup(t *testing.T) {
	t.Helper()
	if holder == nil || holder.finished {
		return
	}
	_, _ = fmt.Fprintln(holder.stdin, g6LeaseHolderRelease)
	_ = holder.stdin.Close()
	select {
	case <-holder.wait:
		holder.finished = true
		return
	case <-time.After(2 * time.Second):
	}
	if holder.command.Process != nil {
		_ = holder.command.Process.Kill()
	}
	select {
	case <-holder.wait:
		holder.finished = true
	case <-time.After(5 * time.Second):
		t.Log("repository Lease holder did not exit after cleanup kill")
	}
}

func runColdkeepCommandWithTimeoutG6(
	t *testing.T,
	repoRoot string,
	binPath string,
	env map[string]string,
	args ...string,
) testutils.CLIExecResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, binPath, args...)
	command.Dir = repoRoot
	command.Env = testutils.BuildCommandEnv(env)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatalf("coldkeep command %v timed out: %v", args, ctx.Err())
	}
	if err == nil {
		return testutils.CLIExecResult{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: 0}
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return testutils.CLIExecResult{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exitErr.ExitCode()}
	}
	t.Fatalf("run coldkeep command %v: %v", args, err)
	return testutils.CLIExecResult{}
}

func assertRepositoryBusyCLIResultG6(t *testing.T, result testutils.CLIExecResult) {
	t.Helper()
	if result.ExitCode != 1 {
		t.Fatalf("busy contender exit=%d want 1; stdout=%s stderr=%s", result.ExitCode, result.Stdout, result.Stderr)
	}
	if strings.TrimSpace(result.Stdout) != "" {
		t.Fatalf("busy contender stdout=%q want empty", result.Stdout)
	}
	payload, ok := testutils.TryParseLastJSONLine(result.Stderr)
	if !ok {
		t.Fatalf("busy contender produced no JSON error; stderr=%s", result.Stderr)
	}
	if got, _ := payload["status"].(string); got != "error" {
		t.Fatalf("busy status=%q want error; payload=%v", got, payload)
	}
	if got, _ := payload["error_class"].(string); got != "GENERAL" {
		t.Fatalf("busy error_class=%q want GENERAL; payload=%v", got, payload)
	}
	if got, _ := payload["exit_code"].(float64); int(got) != 1 {
		t.Fatalf("busy JSON exit_code=%v want 1; payload=%v", payload["exit_code"], payload)
	}
	if got, _ := payload["message"].(string); got != "repository is busy" {
		t.Fatalf("busy message=%q want %q; payload=%v", got, "repository is busy", payload)
	}
	errorNode, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("busy error node=%T want object; payload=%v", payload["error"], payload)
	}
	if got, _ := errorNode["code"].(string); got != "REPOSITORY_BUSY" {
		t.Fatalf("busy error code=%q want REPOSITORY_BUSY; payload=%v", got, payload)
	}
	if got, _ := errorNode["message"].(string); got != "repository is busy" {
		t.Fatalf("busy nested message=%q want %q; payload=%v", got, "repository is busy", payload)
	}
}

// storeFileWithCodecCLIG6Async is safe to call from goroutines because it
// returns an error instead of calling t.Fatal/t.FailNow.
func storeFileWithCodecCLIG6Async(repoRoot, binPath string, env map[string]string, codec, path string) (int64, error) {
	result, err := storeFileWithCodecCLIG6AsyncDiagnostics(repoRoot, binPath, env, codec, path)
	return result.FileID, err
}

type g6CLIStoreCommandResult struct {
	FileID         int64
	LifecycleTrace []string
	StartedUTC     time.Time
	FinishedUTC    time.Time
}

func storeFileWithCodecCLIG6AsyncDiagnostics(repoRoot, binPath string, env map[string]string, codec, path string) (g6CLIStoreCommandResult, error) {
	result := g6CLIStoreCommandResult{StartedUTC: time.Now().UTC()}
	cmd := exec.Command(binPath, "store", "--codec", codec, path, "--output", "json")
	cmd.Dir = repoRoot
	cmd.Env = testutils.BuildCommandEnv(env)
	out, err := cmd.CombinedOutput()
	result.FinishedUTC = time.Now().UTC()
	result.LifecycleTrace = filterG6LifecycleTrace(string(out), env)
	if err != nil {
		return result, fmt.Errorf("store command: %w; output=%s", err, sanitizeG6DiagnosticText(string(out), env))
	}
	payload, ok := testutils.TryParseLastJSONLine(string(out))
	if !ok {
		return result, fmt.Errorf("no JSON in store output: %s", sanitizeG6DiagnosticText(string(out), env))
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return result, fmt.Errorf("store payload missing data: %v", payload)
	}
	idF, ok := data["file_id"].(float64)
	if !ok {
		return result, fmt.Errorf("store payload missing file_id: %v", data)
	}
	result.FileID = int64(idF)
	return result, nil
}

var g6LifecycleEventMarkers = []string{
	"event=store_reuse_claim_graph_invalid",
	"event=store_reuse_validation_failed",
	"event=chunk_reuse_validation_failed",
	"event=store_chunk_reclaim",
}

func filterG6LifecycleTrace(output string, env map[string]string) []string {
	const maxTraceLines = 256
	trace := make([]string, 0)
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || !containsG6LifecycleMarker(line) {
			continue
		}
		trace = append(trace, sanitizeG6DiagnosticText(line, env))
		if len(trace) == maxTraceLines {
			break
		}
	}
	return trace
}

func containsG6LifecycleMarker(line string) bool {
	for _, marker := range g6LifecycleEventMarkers {
		if strings.Contains(line, marker) {
			return true
		}
	}
	return false
}

func sanitizeG6DiagnosticText(value string, env map[string]string) string {
	for _, key := range []string{"COLDKEEP_KEY", "DB_PASSWORD"} {
		secret := strings.TrimSpace(env[key])
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

func restoreMustMatchHashG6(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func countInt64QueryG6(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed (%s): %v", query, err)
	}
	return n
}

func verifyConcurrentInvariantsG6(t *testing.T, dbconn *sql.DB, diag *g6FailureDiagnosticContext) {
	t.Helper()

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		logConcurrentInvariantFailureG6(t, dbconn, err, diag)
		t.Fatalf("verify full: %v", err)
	}
	testutils.AssertNoProcessingRows(t, dbconn)
	testutils.AssertUniqueFileChunkOrders(t, dbconn)

	negativeLiveRefs := countInt64QueryG6(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`)
	if negativeLiveRefs != 0 {
		t.Fatalf("expected no negative live_ref_count rows, got %d", negativeLiveRefs)
	}

	negativePinRefs := countInt64QueryG6(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE pin_count < 0`)
	if negativePinRefs != 0 {
		t.Fatalf("expected no negative pin_count rows, got %d", negativePinRefs)
	}
}

var g6ChunkIDPattern = regexp.MustCompile(`chunk(?:[ =])(\d+)`)

type g6FailureDiagnosticContext struct {
	TestName      string
	Backend       string
	OuterJobCodec string
	InnerSubtest  string
	GOMAXPROCS    int
	Concurrency   int
	IsolatedDB    string
	TempRoot      string
	StoreResults  []g6StoreOperationResult
}

type g6StoreOperationResult struct {
	Worker         int       `json:"worker"`
	FileID         int64     `json:"file_id,omitempty"`
	Error          string    `json:"error,omitempty"`
	LifecycleTrace []string  `json:"lifecycle_trace,omitempty"`
	StartedUTC     time.Time `json:"started_utc"`
	FinishedUTC    time.Time `json:"finished_utc"`
}

type g6ChunkFailureDiagnosticManifest struct {
	Kind                    string                   `json:"kind"`
	TestName                string                   `json:"test_name"`
	TimestampUTC            time.Time                `json:"timestamp_utc"`
	Backend                 string                   `json:"backend"`
	OuterJobCodec           string                   `json:"outer_job_codec"`
	InnerSubtestCodec       string                   `json:"inner_subtest_codec"`
	GOMAXPROCS              int                      `json:"gomaxprocs"`
	ConcurrencyCount        int                      `json:"concurrency_count"`
	IsolatedDatabaseName    string                   `json:"isolated_database_name"`
	PreservedTemporaryRoot  string                   `json:"preserved_temporary_root"`
	OffendingChunkID        *int64                   `json:"offending_chunk_id,omitempty"`
	OffendingChunkHash      string                   `json:"offending_chunk_hash,omitempty"`
	VerifyError             string                   `json:"verify_error"`
	SchemaVersion           int64                    `json:"schema_version"`
	StoreResults            []g6StoreOperationResult `json:"store_results"`
	RelevantConfiguration   map[string]string        `json:"relevant_configuration,omitempty"`
	MigrationCompanionState g6ChunkMetadataRecord    `json:"migration_companion_state"`
	PackedBlocks            []g6PackedBlockRecord    `json:"packed_blocks,omitempty"`
	PhysicalFiles           []g6PhysicalFileRecord   `json:"physical_files,omitempty"`
}

type g6PackedBlockRecord struct {
	BlockID                 int64                  `json:"block_id"`
	FormatVersion           int64                  `json:"format_version"`
	Codec                   string                 `json:"codec"`
	CompressionCodec        string                 `json:"compression_codec"`
	CompressionLevel        *int64                 `json:"compression_level,omitempty"`
	PlaintextSize           int64                  `json:"plaintext_size"`
	CompressedSize          *int64                 `json:"compressed_size,omitempty"`
	StoredSize              int64                  `json:"stored_size"`
	ContainerID             int64                  `json:"container_id"`
	ContainerFilename       string                 `json:"container_filename"`
	ContainerMaxSize        int64                  `json:"container_max_size"`
	ContainerOffset         int64                  `json:"container_offset"`
	BlockHash               string                 `json:"block_hash,omitempty"`
	PayloadHash             string                 `json:"payload_hash,omitempty"`
	CompressedHash          string                 `json:"compressed_hash,omitempty"`
	PhysicalHash            string                 `json:"physical_hash,omitempty"`
	ActualPhysicalHash      string                 `json:"actual_physical_hash,omitempty"`
	ActualPhysicalHashError string                 `json:"actual_physical_hash_error,omitempty"`
	Members                 []g6PackedBlockMember  `json:"members"`
	EncodedMembers          []g6EncodedBlockMember `json:"encoded_members,omitempty"`
	EncodedMembersError     string                 `json:"encoded_members_error,omitempty"`
}

type g6PackedBlockMember struct {
	ChunkID           int64  `json:"chunk_id"`
	ChunkHash         string `json:"chunk_hash"`
	ChunkStatus       string `json:"chunk_status"`
	OffsetInBlock     int64  `json:"offset_in_block"`
	SizeInBlock       int64  `json:"size_in_block"`
	LegacyMappingID   *int64 `json:"legacy_mapping_id,omitempty"`
	LegacyCodec       string `json:"legacy_codec,omitempty"`
	LegacyContainerID *int64 `json:"legacy_container_id,omitempty"`
	LegacyOffset      *int64 `json:"legacy_offset,omitempty"`
	LegacyStoredSize  *int64 `json:"legacy_stored_size,omitempty"`
	LegacyNonceLength *int64 `json:"legacy_nonce_length,omitempty"`
}

type g6EncodedBlockMember struct {
	ChunkID uint64 `json:"chunk_id"`
	Offset  uint64 `json:"offset"`
	Size    uint64 `json:"size"`
}

type g6PhysicalFileRecord struct {
	ID            int64  `json:"id"`
	Path          string `json:"path"`
	LogicalFileID int64  `json:"logical_file_id"`
}

type g6ChunkMetadataRecord struct {
	LegacyMappingID         *int64 `json:"legacy_mapping_id,omitempty"`
	LegacyCodec             string `json:"legacy_codec,omitempty"`
	LegacyFormatVersion     *int64 `json:"legacy_format_version,omitempty"`
	LegacyPlaintextSize     *int64 `json:"legacy_plaintext_size,omitempty"`
	LegacyStoredSize        *int64 `json:"legacy_stored_size,omitempty"`
	LegacyNonceLength       *int64 `json:"legacy_nonce_length,omitempty"`
	LegacyContainerID       *int64 `json:"legacy_container_id,omitempty"`
	LegacyOffset            *int64 `json:"legacy_offset,omitempty"`
	PackedBlockID           *int64 `json:"packed_block_id,omitempty"`
	PackedOffsetInBlock     *int64 `json:"packed_offset_in_block,omitempty"`
	PackedSizeInBlock       *int64 `json:"packed_size_in_block,omitempty"`
	PackedContainerID       *int64 `json:"packed_container_id,omitempty"`
	PackedContainerOffset   *int64 `json:"packed_container_offset,omitempty"`
	PackedPlaintextSize     *int64 `json:"packed_plaintext_size,omitempty"`
	TotalReferencedBytes    *int64 `json:"packed_total_referenced_bytes,omitempty"`
	PayloadPrefixBytes      *int64 `json:"payload_prefix_bytes,omitempty"`
	LegacyContainerFilename string `json:"legacy_container_filename,omitempty"`
	PackedContainerFilename string `json:"packed_container_filename,omitempty"`
}

func logConcurrentInvariantFailureG6(t *testing.T, dbconn *sql.DB, verifyErr error, diag *g6FailureDiagnosticContext) {
	t.Helper()
	t.Logf("G6 verify failure diagnostics: db=%s containers_dir=%s err=%v", os.Getenv("DB_NAME"), container.ContainersDir, verifyErr)
	manifest := buildG6FailureManifest(t, verifyErr, diag)
	loadG6FailureSchemaVersion(t, dbconn, &manifest)
	attachG6OffendingChunkMetadata(t, dbconn, verifyErr, &manifest)
	attachG6RepositoryState(t, dbconn, &manifest)
	writeConcurrentInvariantManifestG6(t, manifest)
}

func buildG6FailureManifest(t *testing.T, verifyErr error, diag *g6FailureDiagnosticContext) g6ChunkFailureDiagnosticManifest {
	t.Helper()
	manifest := g6ChunkFailureDiagnosticManifest{
		Kind:                   "g6_concurrent_store_failure",
		TestName:               t.Name(),
		TimestampUTC:           time.Now().UTC(),
		VerifyError:            verifyErr.Error(),
		StoreResults:           append([]g6StoreOperationResult(nil), diagStoreResultsG6(diag)...),
		RelevantConfiguration:  g6RelevantConfiguration(),
		Backend:                diagStringG6(diag, func(v *g6FailureDiagnosticContext) string { return v.Backend }),
		OuterJobCodec:          diagStringG6(diag, func(v *g6FailureDiagnosticContext) string { return v.OuterJobCodec }),
		InnerSubtestCodec:      diagStringG6(diag, func(v *g6FailureDiagnosticContext) string { return v.InnerSubtest }),
		GOMAXPROCS:             diagIntG6(diag, func(v *g6FailureDiagnosticContext) int { return v.GOMAXPROCS }),
		ConcurrencyCount:       diagIntG6(diag, func(v *g6FailureDiagnosticContext) int { return v.Concurrency }),
		IsolatedDatabaseName:   diagStringG6(diag, func(v *g6FailureDiagnosticContext) string { return v.IsolatedDB }),
		PreservedTemporaryRoot: diagStringG6(diag, func(v *g6FailureDiagnosticContext) string { return v.TempRoot }),
	}
	if manifest.TestName == "" && diag != nil && diag.TestName != "" {
		manifest.TestName = diag.TestName
	}
	if manifest.GOMAXPROCS == 0 {
		manifest.GOMAXPROCS = runtime.GOMAXPROCS(0)
	}
	return manifest
}

func loadG6FailureSchemaVersion(t *testing.T, dbconn *sql.DB, manifest *g6ChunkFailureDiagnosticManifest) {
	t.Helper()
	if v, err := g6SchemaVersion(dbconn); err == nil {
		manifest.SchemaVersion = v
	} else {
		t.Logf("G6 verify diagnostics: schema version query failed: %v", err)
	}
}

func attachG6OffendingChunkMetadata(t *testing.T, dbconn *sql.DB, verifyErr error, manifest *g6ChunkFailureDiagnosticManifest) {
	t.Helper()
	matches := g6ChunkIDPattern.FindStringSubmatch(verifyErr.Error())
	if len(matches) != 2 {
		logMixedChunkShapesG6(t, dbconn)
		return
	}

	var chunkID int64
	if _, err := fmt.Sscanf(matches[1], "%d", &chunkID); err != nil {
		t.Logf("G6 verify diagnostics: parse chunk id from %q: %v", matches[1], err)
		logMixedChunkShapesG6(t, dbconn)
		return
	}

	manifest.OffendingChunkID = &chunkID
	chunkMeta, chunkHash, err := logChunkMetadataG6(t, dbconn, chunkID)
	if err != nil {
		t.Logf("G6 verify diagnostics: collect chunk metadata chunk_id=%d: %v", chunkID, err)
		return
	}
	manifest.OffendingChunkHash = chunkHash
	manifest.MigrationCompanionState = chunkMeta
}

func logMixedChunkShapesG6(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT c.id,
		       EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = c.id) AS has_packed,
		       (SELECT COUNT(*) FROM blocks b WHERE b.chunk_id = c.id) AS legacy_rows
		FROM chunk c
		WHERE EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = c.id)
		   OR EXISTS (SELECT 1 FROM blocks b WHERE b.chunk_id = c.id)
		ORDER BY c.id
	`)
	if err != nil {
		t.Logf("G6 verify diagnostics: query mixed chunk shapes: %v", err)
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var chunkID int64
		var hasPacked bool
		var legacyRows int64
		if err := rows.Scan(&chunkID, &hasPacked, &legacyRows); err != nil {
			t.Logf("G6 verify diagnostics: scan mixed chunk shape: %v", err)
			return
		}
		if hasPacked && legacyRows > 0 {
			t.Logf("G6 verify diagnostics: mixed mapping chunk_id=%d has_packed=%t legacy_rows=%d", chunkID, hasPacked, legacyRows)
			_, _, _ = logChunkMetadataG6(t, dbconn, chunkID)
		}
	}
	if err := rows.Err(); err != nil {
		t.Logf("G6 verify diagnostics: iterate mixed chunk shapes: %v", err)
	}
}

type g6ChunkMetadata struct {
	chunkSize            int64
	chunkHash            string
	chunkStatus          string
	legacyMappingID      sql.NullInt64
	legacyCodec          sql.NullString
	legacyFormatVersion  sql.NullInt64
	legacyPlaintextSize  sql.NullInt64
	legacyStoredSize     sql.NullInt64
	legacyNonceLen       sql.NullInt64
	legacyContainerID    sql.NullInt64
	legacyOffset         sql.NullInt64
	packedBlockID        sql.NullInt64
	packedOffsetInBlock  sql.NullInt64
	packedSizeInBlock    sql.NullInt64
	packedContainerID    sql.NullInt64
	packedContainerOff   sql.NullInt64
	packedPlaintextSize  sql.NullInt64
	totalReferencedBytes sql.NullInt64
}

const g6ChunkMetadataQuery = `
			SELECT
				c.size,
			c.chunk_hash,
			c.status,
			b.id,
			b.codec,
			b.format_version,
			b.plaintext_size,
			b.stored_size,
			OCTET_LENGTH(b.nonce),
			b.container_id,
			b.block_offset,
			r.block_id,
			r.offset_in_block,
			r.size_in_block,
			sb.container_id,
			sb.container_offset,
			sb.plaintext_size,
			(
				SELECT COALESCE(SUM(size_in_block), 0)
				FROM chunk_block_refs
				WHERE block_id = r.block_id
			)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
			LEFT JOIN storage_blocks sb ON sb.id = r.block_id
			WHERE c.id = $1
`

func logChunkMetadataG6(t *testing.T, dbconn *sql.DB, chunkID int64) (g6ChunkMetadataRecord, string, error) {
	t.Helper()
	meta, err := loadG6ChunkMetadata(dbconn, chunkID)
	if err != nil {
		return g6ChunkMetadataRecord{}, "", err
	}
	payloadPrefixBytes := g6PayloadPrefixBytes(meta)
	logG6ChunkMetadata(t, chunkID, meta, payloadPrefixBytes)
	legacyFilename := logContainerMetadataG6(t, dbconn, "legacy", meta.legacyContainerID)
	packedFilename := logContainerMetadataG6(t, dbconn, "packed", meta.packedContainerID)
	return g6ChunkMetadataRecordFrom(meta, payloadPrefixBytes, legacyFilename, packedFilename), meta.chunkHash, nil
}

func loadG6ChunkMetadata(dbconn *sql.DB, chunkID int64) (g6ChunkMetadata, error) {
	var meta g6ChunkMetadata
	err := dbconn.QueryRow(g6ChunkMetadataQuery, chunkID).Scan(
		&meta.chunkSize,
		&meta.chunkHash,
		&meta.chunkStatus,
		&meta.legacyMappingID,
		&meta.legacyCodec,
		&meta.legacyFormatVersion,
		&meta.legacyPlaintextSize,
		&meta.legacyStoredSize,
		&meta.legacyNonceLen,
		&meta.legacyContainerID,
		&meta.legacyOffset,
		&meta.packedBlockID,
		&meta.packedOffsetInBlock,
		&meta.packedSizeInBlock,
		&meta.packedContainerID,
		&meta.packedContainerOff,
		&meta.packedPlaintextSize,
		&meta.totalReferencedBytes,
	)
	return meta, err
}

func g6PayloadPrefixBytes(meta g6ChunkMetadata) *int64 {
	if meta.packedPlaintextSize.Valid && meta.totalReferencedBytes.Valid {
		v := meta.packedPlaintextSize.Int64 - meta.totalReferencedBytes.Int64
		return &v
	}
	return nil
}

func logG6ChunkMetadata(t *testing.T, chunkID int64, meta g6ChunkMetadata, payloadPrefixBytes *int64) {
	t.Helper()
	t.Logf(
		"G6 verify diagnostics: chunk_id=%d status=%s chunk_size=%d legacy_codec=%v legacy_format=%v legacy_plaintext=%v legacy_stored=%v legacy_nonce_len=%v legacy_container=%v legacy_offset=%v packed_block=%v packed_offset_in_block=%v packed_size_in_block=%v packed_container=%v packed_container_offset=%v packed_plaintext=%v packed_total_referenced=%v payload_prefix_bytes=%v",
		chunkID,
		meta.chunkStatus,
		meta.chunkSize,
		nullStringValueG6(meta.legacyCodec),
		nullInt64ValueG6(meta.legacyFormatVersion),
		nullInt64ValueG6(meta.legacyPlaintextSize),
		nullInt64ValueG6(meta.legacyStoredSize),
		nullInt64ValueG6(meta.legacyNonceLen),
		nullInt64ValueG6(meta.legacyContainerID),
		nullInt64ValueG6(meta.legacyOffset),
		nullInt64ValueG6(meta.packedBlockID),
		nullInt64ValueG6(meta.packedOffsetInBlock),
		nullInt64ValueG6(meta.packedSizeInBlock),
		nullInt64ValueG6(meta.packedContainerID),
		nullInt64ValueG6(meta.packedContainerOff),
		nullInt64ValueG6(meta.packedPlaintextSize),
		nullInt64ValueG6(meta.totalReferencedBytes),
		nullInt64PointerValueG6(payloadPrefixBytes),
	)
}

func g6ChunkMetadataRecordFrom(meta g6ChunkMetadata, payloadPrefixBytes *int64, legacyContainerFilename, packedContainerFilename string) g6ChunkMetadataRecord {
	return g6ChunkMetadataRecord{
		LegacyMappingID:         nullInt64PointerG6(meta.legacyMappingID),
		LegacyCodec:             nullStringPlainG6(meta.legacyCodec),
		LegacyFormatVersion:     nullInt64PointerG6(meta.legacyFormatVersion),
		LegacyPlaintextSize:     nullInt64PointerG6(meta.legacyPlaintextSize),
		LegacyStoredSize:        nullInt64PointerG6(meta.legacyStoredSize),
		LegacyNonceLength:       nullInt64PointerG6(meta.legacyNonceLen),
		LegacyContainerID:       nullInt64PointerG6(meta.legacyContainerID),
		LegacyOffset:            nullInt64PointerG6(meta.legacyOffset),
		PackedBlockID:           nullInt64PointerG6(meta.packedBlockID),
		PackedOffsetInBlock:     nullInt64PointerG6(meta.packedOffsetInBlock),
		PackedSizeInBlock:       nullInt64PointerG6(meta.packedSizeInBlock),
		PackedContainerID:       nullInt64PointerG6(meta.packedContainerID),
		PackedContainerOffset:   nullInt64PointerG6(meta.packedContainerOff),
		PackedPlaintextSize:     nullInt64PointerG6(meta.packedPlaintextSize),
		TotalReferencedBytes:    nullInt64PointerG6(meta.totalReferencedBytes),
		PayloadPrefixBytes:      payloadPrefixBytes,
		LegacyContainerFilename: legacyContainerFilename,
		PackedContainerFilename: packedContainerFilename,
	}
}

func logContainerMetadataG6(t *testing.T, dbconn *sql.DB, label string, containerID sql.NullInt64) string {
	t.Helper()

	if !containerID.Valid {
		return ""
	}

	var filename string
	var currentSize int64
	var maxSize int64
	var sealed bool
	var sealing bool
	var quarantine bool
	if err := dbconn.QueryRow(`
		SELECT filename, current_size, max_size, sealed, sealing, quarantine
		FROM container
		WHERE id = $1
	`, containerID.Int64).Scan(&filename, &currentSize, &maxSize, &sealed, &sealing, &quarantine); err != nil {
		t.Logf("G6 verify diagnostics: query %s container %d: %v", label, containerID.Int64, err)
		return ""
	}

	t.Logf(
		"G6 verify diagnostics: %s_container_id=%d filename=%s current_size=%d max_size=%d sealed=%t sealing=%t quarantine=%t",
		label,
		containerID.Int64,
		filename,
		currentSize,
		maxSize,
		sealed,
		sealing,
		quarantine,
	)
	return filename
}

func attachG6RepositoryState(t *testing.T, dbconn *sql.DB, manifest *g6ChunkFailureDiagnosticManifest) {
	t.Helper()
	blocks, err := loadG6PackedBlocks(dbconn)
	if err != nil {
		t.Logf("G6 verify diagnostics: collect packed-block state: %v", err)
	} else {
		for i := range blocks {
			attachG6ActualPhysicalHash(&blocks[i])
			attachG6EncodedBlockMembers(&blocks[i])
		}
		manifest.PackedBlocks = blocks
	}

	physicalFiles, err := loadG6PhysicalFiles(dbconn)
	if err != nil {
		t.Logf("G6 verify diagnostics: collect physical-file state: %v", err)
	} else {
		manifest.PhysicalFiles = physicalFiles
	}
}

func loadG6PackedBlocks(dbconn *sql.DB) ([]g6PackedBlockRecord, error) {
	rows, err := dbconn.Query(`
		SELECT sb.id, sb.format_version, sb.codec, sb.compression_codec,
		       sb.compression_level, sb.plaintext_size, sb.compressed_size,
		       sb.stored_size, sb.container_id, c.filename, c.max_size, sb.container_offset,
		       sb.block_hash, sb.payload_hash, sb.compressed_hash, sb.physical_hash
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		ORDER BY sb.id
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	records := make([]g6PackedBlockRecord, 0)
	for rows.Next() {
		var record g6PackedBlockRecord
		var compressionLevel sql.NullInt64
		var compressedSize sql.NullInt64
		var payloadHash sql.NullString
		var blockHash []byte
		var compressedHash []byte
		var physicalHash []byte
		if err := rows.Scan(
			&record.BlockID,
			&record.FormatVersion,
			&record.Codec,
			&record.CompressionCodec,
			&compressionLevel,
			&record.PlaintextSize,
			&compressedSize,
			&record.StoredSize,
			&record.ContainerID,
			&record.ContainerFilename,
			&record.ContainerMaxSize,
			&record.ContainerOffset,
			&blockHash,
			&payloadHash,
			&compressedHash,
			&physicalHash,
		); err != nil {
			return nil, err
		}
		record.CompressionLevel = nullInt64PointerG6(compressionLevel)
		record.CompressedSize = nullInt64PointerG6(compressedSize)
		record.BlockHash = hex.EncodeToString(blockHash)
		record.PayloadHash = nullStringPlainG6(payloadHash)
		record.CompressedHash = hex.EncodeToString(compressedHash)
		record.PhysicalHash = hex.EncodeToString(physicalHash)
		record.Members, err = loadG6PackedBlockMembers(dbconn, record.BlockID)
		if err != nil {
			return nil, fmt.Errorf("load members for block %d: %w", record.BlockID, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func loadG6PackedBlockMembers(dbconn *sql.DB, blockID int64) ([]g6PackedBlockMember, error) {
	rows, err := dbconn.Query(`
		SELECT r.chunk_id, c.chunk_hash, c.status, r.offset_in_block, r.size_in_block,
		       b.id, b.codec, b.container_id, b.block_offset, b.stored_size,
		       OCTET_LENGTH(b.nonce)
		FROM chunk_block_refs r
		JOIN chunk c ON c.id = r.chunk_id
		LEFT JOIN blocks b ON b.chunk_id = r.chunk_id
		WHERE r.block_id = $1
		ORDER BY r.offset_in_block, r.chunk_id
	`, blockID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	members := make([]g6PackedBlockMember, 0)
	for rows.Next() {
		var member g6PackedBlockMember
		var legacyMappingID sql.NullInt64
		var legacyCodec sql.NullString
		var legacyContainerID sql.NullInt64
		var legacyOffset sql.NullInt64
		var legacyStoredSize sql.NullInt64
		var legacyNonceLength sql.NullInt64
		if err := rows.Scan(
			&member.ChunkID,
			&member.ChunkHash,
			&member.ChunkStatus,
			&member.OffsetInBlock,
			&member.SizeInBlock,
			&legacyMappingID,
			&legacyCodec,
			&legacyContainerID,
			&legacyOffset,
			&legacyStoredSize,
			&legacyNonceLength,
		); err != nil {
			return nil, err
		}
		member.LegacyMappingID = nullInt64PointerG6(legacyMappingID)
		member.LegacyCodec = nullStringPlainG6(legacyCodec)
		member.LegacyContainerID = nullInt64PointerG6(legacyContainerID)
		member.LegacyOffset = nullInt64PointerG6(legacyOffset)
		member.LegacyStoredSize = nullInt64PointerG6(legacyStoredSize)
		member.LegacyNonceLength = nullInt64PointerG6(legacyNonceLength)
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return members, nil
}

func attachG6ActualPhysicalHash(record *g6PackedBlockRecord) {
	if record.StoredSize < 0 || record.ContainerOffset < 0 {
		record.ActualPhysicalHashError = fmt.Sprintf(
			"invalid stored payload bounds: offset=%d size=%d",
			record.ContainerOffset,
			record.StoredSize,
		)
		return
	}
	if record.ContainerMaxSize > 0 && (record.ContainerOffset > record.ContainerMaxSize || record.StoredSize > record.ContainerMaxSize-record.ContainerOffset) {
		record.ActualPhysicalHashError = fmt.Sprintf(
			"stored payload exceeds container bounds: offset=%d size=%d max=%d",
			record.ContainerOffset,
			record.StoredSize,
			record.ContainerMaxSize,
		)
		return
	}

	path, err := container.SafeContainerPath(container.ContainersDir, record.ContainerFilename)
	if err != nil {
		record.ActualPhysicalHashError = err.Error()
		return
	}
	f, err := os.Open(path)
	if err != nil {
		record.ActualPhysicalHashError = err.Error()
		return
	}
	defer func() { _ = f.Close() }()

	payload := make([]byte, record.StoredSize)
	n, err := f.ReadAt(payload, record.ContainerOffset)
	if err != nil {
		record.ActualPhysicalHashError = fmt.Sprintf("read stored payload: read=%d expected=%d: %v", n, record.StoredSize, err)
		return
	}
	sum := sha256.Sum256(payload)
	record.ActualPhysicalHash = hex.EncodeToString(sum[:])
}

func attachG6EncodedBlockMembers(record *g6PackedBlockRecord) {
	logicalHash, err := hex.DecodeString(record.BlockHash)
	if err != nil {
		record.EncodedMembersError = fmt.Sprintf("decode block hash: %v", err)
		return
	}
	compressedHash, err := hex.DecodeString(record.CompressedHash)
	if err != nil {
		record.EncodedMembersError = fmt.Sprintf("decode compressed hash: %v", err)
		return
	}
	physicalHash, err := hex.DecodeString(record.PhysicalHash)
	if err != nil {
		record.EncodedMembersError = fmt.Sprintf("decode physical hash: %v", err)
		return
	}
	var compressionLevel *int
	if record.CompressionLevel != nil {
		value := int(*record.CompressionLevel)
		compressionLevel = &value
	}
	verified, err := verify.VerifyStoredBlock(context.Background(), verify.BlockStorageMetadata{
		BlockID:          record.BlockID,
		ContainerID:      record.ContainerID,
		ContainerOffset:  record.ContainerOffset,
		ContainerName:    record.ContainerFilename,
		ContainerMaxSize: record.ContainerMaxSize,
		FormatVersion:    record.FormatVersion,
		Codec:            record.Codec,
		PlaintextSize:    record.PlaintextSize,
		CompressedSize:   record.CompressedSize,
		StoredSize:       record.StoredSize,
		CompressionCodec: record.CompressionCodec,
		CompressionLevel: compressionLevel,
		LogicalHash:      logicalHash,
		CompressedHash:   compressedHash,
		PhysicalHash:     physicalHash,
	}, verify.FilesystemContainerReader{ContainersDir: container.ContainersDir})
	if err != nil {
		record.EncodedMembersError = err.Error()
		return
	}
	if verified == nil || verified.DecodedBlock == nil {
		record.EncodedMembersError = "verified block did not include decoded membership"
		return
	}
	for _, entry := range verified.DecodedBlock.Entries {
		record.EncodedMembers = append(record.EncodedMembers, g6EncodedBlockMember{
			ChunkID: entry.ChunkID,
			Offset:  entry.Offset,
			Size:    entry.Size,
		})
	}
}

func loadG6PhysicalFiles(dbconn *sql.DB) ([]g6PhysicalFileRecord, error) {
	rows, err := dbconn.Query(`SELECT id, path, logical_file_id FROM physical_file ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	records := make([]g6PhysicalFileRecord, 0)
	for rows.Next() {
		var record g6PhysicalFileRecord
		if err := rows.Scan(&record.ID, &record.Path, &record.LogicalFileID); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func nullInt64ValueG6(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func nullStringValueG6(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}

func nullInt64PointerG6(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	out := v.Int64
	return &out
}

func nullInt64PointerValueG6(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullStringPlainG6(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func diagStringG6(ctx *g6FailureDiagnosticContext, getter func(*g6FailureDiagnosticContext) string) string {
	if ctx == nil {
		return ""
	}
	return getter(ctx)
}

func diagIntG6(ctx *g6FailureDiagnosticContext, getter func(*g6FailureDiagnosticContext) int) int {
	if ctx == nil {
		return 0
	}
	return getter(ctx)
}

func diagStoreResultsG6(ctx *g6FailureDiagnosticContext) []g6StoreOperationResult {
	if ctx == nil {
		return nil
	}
	return ctx.StoreResults
}

func g6SchemaVersion(dbconn *sql.DB) (int64, error) {
	var version int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func g6RelevantConfiguration() map[string]string {
	keys := []string{
		"COLDKEEP_DB_AUTO_BOOTSTRAP",
		"COLDKEEP_CODEC",
		"COLDKEEP_COMPRESSION",
		"COLDKEEP_BLOCK_TARGET_SIZE_MB",
		"COLDKEEP_PACKED_BLOCK_SIZE_MIB",
		"COLDKEEP_CONTAINER_LOCK_RETRY_ATTEMPTS",
		"COLDKEEP_CONTAINER_LOCK_RETRY_BASE_WAIT_MS",
		"COLDKEEP_CONTAINER_LOCK_RETRY_MAX_WAIT_MS",
		"COLDKEEP_LONG_RUN",
	}
	cfg := make(map[string]string)
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			cfg[key] = v
		}
	}
	return cfg
}

func writeConcurrentInvariantManifestG6(t *testing.T, manifest g6ChunkFailureDiagnosticManifest) {
	t.Helper()

	if !testutils.DiagnosticManifestEnabled() {
		return
	}
	path, err := testutils.WriteDiagnosticJSON("g6-failure", manifest)
	if err != nil {
		t.Logf("G6 verify diagnostics: write failure manifest: %v", err)
		return
	}
	t.Logf("G6 verify diagnostics: wrote failure manifest %s", path)
}

func TestAdversarialG6IndependentProcessRepositoryContention(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g6-contention.bin", 256*1024+313)
			fileHash := testutils.SHA256File(t, inPath)
			holder := startG6RepositoryLeaseHolder(t, env)

			busyResult := runColdkeepCommandWithTimeoutG6(
				t,
				repoRoot,
				binPath,
				env,
				"store", "--codec", codec, inPath, "--output", "json",
			)
			assertRepositoryBusyCLIResultG6(t, busyResult)

			var storedRows int
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&storedRows); err != nil {
				t.Fatalf("count logical files after Busy contender: %v", err)
			}
			if storedRows != 0 {
				t.Fatalf("Busy contender stored %d logical-file rows; want 0", storedRows)
			}

			holder.release(t)
			fileID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, inPath)
			verifyConcurrentInvariantsG6(t, dbconn, nil)
			restoreMustMatchHashG6(t, dbconn, fileID, filepath.Join(restoreDir, "g6-contention-restored.bin"), fileHash)
		})
	}
}

func TestAdversarialG6KilledLeaseHolderReleasesRepository(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("killed-process repository Lease release proof is required on Linux")
	}
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	codec := getenvOrDefaultAdversarialG6("COLDKEEP_CODEC", "plain")
	if codec != "plain" && codec != "aes-gcm" {
		t.Fatalf("unsupported killed-holder proof codec %q", codec)
	}
	configureAdversarialG6Codec(t, codec)

	dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore: %v", err)
	}

	inPath := testutils.CreateTempFile(t, inputDir, "g6-killed-holder.bin", 256*1024+313)
	fileHash := testutils.SHA256File(t, inPath)
	holder := startG6RepositoryLeaseHolder(t, env)

	prepared, err := coordination.PrepareControlNamespace(env["COLDKEEP_STORAGE_DIR"])
	if err != nil {
		t.Fatalf("prepare killed-holder control namespace: %v", err)
	}
	assertG6PersistentRepositoryLock(t, prepared.LockArtifactPath, "holder acquisition")
	ownerBeforeKill := readG6OwnerMetadata(t, prepared.OwnerMetadataPath, "before holder kill")
	if ownerBeforeKill.PID != holder.command.Process.Pid {
		t.Fatalf("holder owner PID=%d want process PID=%d", ownerBeforeKill.PID, holder.command.Process.Pid)
	}
	if ownerBeforeKill.Operation != coordination.OperationStore || ownerBeforeKill.IdentityHash != prepared.Identity.Hash {
		t.Fatalf("holder owner metadata=%+v want store owner for repository identity %s", ownerBeforeKill, prepared.Identity.Hash)
	}

	busyResult := runColdkeepCommandWithTimeoutG6(
		t,
		repoRoot,
		binPath,
		env,
		"store", "--codec", codec, inPath, "--output", "json",
	)
	assertRepositoryBusyCLIResultG6(t, busyResult)

	var storedRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&storedRows); err != nil {
		t.Fatalf("count logical files after killed-holder Busy contender: %v", err)
	}
	if storedRows != 0 {
		t.Fatalf("killed-holder Busy contender stored %d logical-file rows; want 0", storedRows)
	}

	holder.kill(t)
	assertG6PersistentRepositoryLock(t, prepared.LockArtifactPath, "holder death")
	ownerAfterKill := readG6OwnerMetadata(t, prepared.OwnerMetadataPath, "after holder kill")
	if ownerAfterKill != ownerBeforeKill {
		t.Fatalf("stale owner metadata after holder kill=%+v want %+v", ownerAfterKill, ownerBeforeKill)
	}

	fileID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, inPath)
	assertG6PersistentRepositoryLock(t, prepared.LockArtifactPath, "successful reacquisition and release")
	if _, err := os.Lstat(prepared.OwnerMetadataPath); !os.IsNotExist(err) {
		t.Fatalf("owner metadata exists after successful reacquisition and release, stat err=%v", err)
	}
	verifyConcurrentInvariantsG6(t, dbconn, nil)
	restoreMustMatchHashG6(t, dbconn, fileID, filepath.Join(restoreDir, "g6-killed-holder-restored.bin"), fileHash)
}

func assertG6PersistentRepositoryLock(t *testing.T, path, stage string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat repository.lock after %s: %v", stage, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("repository.lock mode after %s=%v want regular", stage, info.Mode())
	}
}

func readG6OwnerMetadata(t *testing.T, path, stage string) coordination.Owner {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read owner metadata %s: %v", stage, err)
	}
	owner, err := coordination.DecodeOwner(data)
	if err != nil {
		t.Fatalf("decode owner metadata %s: %v", stage, err)
	}
	return owner
}

func TestAdversarialG6SequentialStoresSameFileConvergeDeterministically(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g6-same-file.bin", 2*1024*1024+313)
			wantHash := testutils.SHA256File(t, inPath)

			const stores = 6
			ids := make([]int64, stores)
			for i := range ids {
				ids[i] = storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, inPath)
			}
			verifyConcurrentInvariantsG6(t, dbconn, nil)

			baseGraph := testutils.QueryChunkGraph(t, dbconn, ids[0])
			if len(baseGraph) == 0 {
				t.Fatalf("expected non-empty chunk graph for first stored file")
			}
			for i := 1; i < len(ids); i++ {
				graph := testutils.QueryChunkGraph(t, dbconn, ids[i])
				if len(graph) != len(baseGraph) {
					t.Fatalf("graph length mismatch for file %d: want %d got %d", i, len(baseGraph), len(graph))
				}
				for j := range baseGraph {
					if baseGraph[j] != graph[j] {
						t.Fatalf("chunk graph drift between sequential stores at file=%d index=%d: base=%+v got=%+v", i, j, baseGraph[j], graph[j])
					}
				}
			}

			for i, id := range ids {
				outPath := filepath.Join(restoreDir, fmt.Sprintf("g6-same-file-%02d.bin", i))
				restoreMustMatchHashG6(t, dbconn, id, outPath, wantHash)
			}
		})
	}
}

func TestAdversarialG6DeterministicStoreInterleavingPostgres(t *testing.T) {
	if requireDeterministicG6Env("COLDKEEP_REQUIRE_DETERMINISTIC_G6_POSTGRES") && os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Fatal("deterministic G6 PostgreSQL regression requires COLDKEEP_TEST_DB=1")
	}
	testgate.RequireDB(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)
			runAdversarialG6DeterministicCase(t, "packed_metadata", codec, storage.TestStoreInterleavingEventAfterPackedMetadata)
			runAdversarialG6DeterministicCase(t, "legacy_companion", codec, storage.TestStoreInterleavingEventAfterLegacyCompanionInsert)
			runAdversarialG6DeterministicRetryCase(t, codec)
		})
	}
}

func TestAdversarialG6SequentialStoresSharedChunksPreserveHealthyRestores(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			sharedPrefix := make([]byte, chunk.MaxChunkSize)
			for i := range sharedPrefix {
				sharedPrefix[i] = byte((i*31 + 7) % 251)
			}
			tailA := make([]byte, 64*1024)
			tailB := make([]byte, 64*1024)
			for i := range tailA {
				tailA[i] = byte((i*17 + 3) % 251)
				tailB[i] = byte((i*29 + 11) % 251)
			}
			hybridA := filepath.Join(inputDir, "hybrid_a.bin")
			hybridB := filepath.Join(inputDir, "hybrid_b.bin")
			if err := os.WriteFile(hybridA, append(append([]byte{}, sharedPrefix...), tailA...), 0o600); err != nil {
				t.Fatalf("write hybrid_a: %v", err)
			}
			if err := os.WriteFile(hybridB, append(append([]byte{}, sharedPrefix...), tailB...), 0o600); err != nil {
				t.Fatalf("write hybrid_b: %v", err)
			}
			hashA := testutils.SHA256File(t, hybridA)
			hashB := testutils.SHA256File(t, hybridB)

			fileAID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, hybridA)
			fileBID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, hybridB)

			var sharedChunks int
			if err := dbconn.QueryRow(`
				SELECT COUNT(DISTINCT fc_a.chunk_id)
				FROM file_chunk fc_a
				JOIN file_chunk fc_b ON fc_b.chunk_id = fc_a.chunk_id
				WHERE fc_a.logical_file_id = $1 AND fc_b.logical_file_id = $2
			`, fileAID, fileBID).Scan(&sharedChunks); err != nil {
				t.Fatalf("count shared chunks: %v", err)
			}
			if sharedChunks == 0 {
				t.Fatal("expected hybrid inputs to reference at least one shared chunk")
			}

			verifyConcurrentInvariantsG6(t, dbconn, nil)

			outA := filepath.Join(restoreDir, "hybrid_a.restored.bin")
			outB := filepath.Join(restoreDir, "hybrid_b.restored.bin")
			restoreMustMatchHashG6(t, dbconn, fileAID, outA, hashA)
			restoreMustMatchHashG6(t, dbconn, fileBID, outB, hashB)
		})
	}
}

func TestAdversarialG6ConcurrentStoreAndGCDoNotLoseReachableData(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g6-anchor.bin", 768*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, anchorPath)

			newPath := filepath.Join(inputDir, "g6-new.bin")
			newData := make([]byte, 1024*1024+411)
			for i := range newData {
				newData[i] = byte((i*29 + 13) % 251)
			}
			if err := os.WriteFile(newPath, newData, 0o644); err != nil {
				t.Fatalf("write new file: %v", err)
			}
			newHash := testutils.SHA256File(t, newPath)

			var newID int64
			storeCh := make(chan error, 1)
			gcErrCh := make(chan error, 1)
			go func() {
				var err error
				newID, err = storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, newPath)
				storeCh <- err
			}()
			go func() {
				gcErrCh <- maintenance.RunGCWithContainersDir(false, container.ContainersDir)
			}()
			if storeErr := <-storeCh; storeErr != nil {
				t.Fatalf("concurrent store failed: %v", storeErr)
			}
			if gcErr := <-gcErrCh; gcErr != nil {
				t.Fatalf("concurrent gc failed: %v", gcErr)
			}

			verifyConcurrentInvariantsG6(t, dbconn, nil)

			restoreMustMatchHashG6(t, dbconn, anchorID, filepath.Join(restoreDir, "anchor.restored.bin"), anchorHash)
			restoreMustMatchHashG6(t, dbconn, newID, filepath.Join(restoreDir, "new.restored.bin"), newHash)
		})
	}
}

func TestAdversarialG6ConcurrentRemoveAndGCPreserveOtherLiveFiles(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			firstPath := testutils.CreateTempFile(t, inputDir, "g6-first.bin", 1024*1024+123)
			firstHash := testutils.SHA256File(t, firstPath)
			firstID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, firstPath)

			secondPath := filepath.Join(inputDir, "g6-second.bin")
			secondData := make([]byte, 1024*1024+777)
			for i := range secondData {
				secondData[i] = byte((i*17 + 9) % 251)
			}
			if err := os.WriteFile(secondPath, secondData, 0o644); err != nil {
				t.Fatalf("write second file: %v", err)
			}
			secondHash := testutils.SHA256File(t, secondPath)
			secondID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, secondPath)

			var removeErr, gcErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				removeErr = storage.RemoveFileWithDB(dbconn, firstID)
			}()
			go func() {
				defer wg.Done()
				gcErr = maintenance.RunGCWithContainersDir(false, container.ContainersDir)
			}()
			wg.Wait()

			if removeErr != nil {
				t.Fatalf("concurrent remove failed: %v", removeErr)
			}
			if gcErr != nil {
				t.Fatalf("concurrent gc failed: %v", gcErr)
			}

			verifyConcurrentInvariantsG6(t, dbconn, nil)

			restoreMustMatchHashG6(t, dbconn, secondID, filepath.Join(restoreDir, "second.restored.bin"), secondHash)

			if err := storage.RestoreFileWithDB(dbconn, firstID, filepath.Join(restoreDir, "first.restored.bin")); err == nil {
				t.Fatalf("removed first file unexpectedly restored successfully")
			}

			var completedFirst int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1 AND status = 'COMPLETED'`, firstHash).Scan(&completedFirst); err != nil {
				t.Fatalf("count completed logical_file rows for first file: %v", err)
			}
			if completedFirst != 0 {
				t.Fatalf("expected no COMPLETED logical_file rows for removed first file hash, got %d", completedFirst)
			}
		})
	}
}

func TestAdversarialG6ConcurrentSnapshotCreateAndGCPreserveRetainedData(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			retainedPath := testutils.CreateTempFile(t, inputDir, "g6-snap-retained.bin", 512*1024)
			retainedHash := testutils.SHA256File(t, retainedPath)
			retainedID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, retainedPath)
			snapshotID := fmt.Sprintf("g6-concurrent-snap-%s", codec)

			var snapErr, gcErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						snapErr = fmt.Errorf("panic: %v", r)
					}
				}()
				args := []string{"snapshot", "create", "--id", snapshotID, "--output", "json"}
				res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, args...)
				if res.ExitCode != 0 {
					snapErr = fmt.Errorf("snapshot create exited %d\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
				}
			}()
			go func() {
				defer wg.Done()
				gcErr = maintenance.RunGCWithContainersDir(false, container.ContainersDir)
			}()
			wg.Wait()

			if snapErr != nil {
				t.Fatalf("concurrent snapshot create failed: %v", snapErr)
			}
			if gcErr != nil {
				t.Fatalf("concurrent gc failed: %v", gcErr)
			}

			verifyConcurrentInvariantsG6(t, dbconn, nil)

			// snapshot must still exist in the DB
			var snapCount int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = $1`, snapshotID).Scan(&snapCount); err != nil {
				t.Fatalf("query snapshot after concurrent ops: %v", err)
			}
			if snapCount == 0 {
				t.Fatalf("expected snapshot %q to exist after concurrent GC, not found", snapshotID)
			}

			// retained file must still restore correctly
			restoreMustMatchHashG6(t, dbconn, retainedID, filepath.Join(restoreDir, "retained.restored.bin"), retainedHash)
		})
	}
}

var _ = dbschema.PostgresSchema
