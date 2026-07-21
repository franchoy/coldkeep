package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
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
	"github.com/franchoy/coldkeep/internal/container"
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
//   - concurrent store/remove/GC activity must not corrupt metadata or graph shape
//   - identical concurrent stores must converge on deterministic chunk graphs
//   - mixed concurrent operations must preserve healthy restores for surviving files
//   - verification invariants must remain true after each stress phase
//
// Notes:
//   - This file uses the current Postgres-backed adversarial harness.
//   - It runs a codec matrix for plain + aes-gcm where data-path behavior matters.
//   - It intentionally validates semantics after concurrency rather than imposing
//     scheduler-specific timing assumptions.

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

func TestAdversarialG6ConcurrentStoresSameFileConvergeDeterministically(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			outerJobCodec := os.Getenv("COLDKEEP_CODEC")
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp, testDBName := setupAdversarialG6Env(t)
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

			const workers = 6
			type g6storeResult struct {
				idx         int
				id          int64
				trace       []string
				startedUTC  time.Time
				finishedUTC time.Time
				err         error
			}
			resultCh := make(chan g6storeResult, workers)
			for i := 0; i < workers; i++ {
				i := i
				go func() {
					storeResult, err := storeFileWithCodecCLIG6AsyncDiagnostics(repoRoot, binPath, env, codec, inPath)
					resultCh <- g6storeResult{
						idx:         i,
						id:          storeResult.FileID,
						trace:       storeResult.LifecycleTrace,
						startedUTC:  storeResult.StartedUTC,
						finishedUTC: storeResult.FinishedUTC,
						err:         err,
					}
				}()
			}
			ids := make([]int64, workers)
			storeResults := make([]g6StoreOperationResult, workers)
			for i := 0; i < workers; i++ {
				res := <-resultCh
				storeResults[res.idx] = g6StoreOperationResult{
					Worker:         res.idx,
					FileID:         res.id,
					LifecycleTrace: append([]string(nil), res.trace...),
					StartedUTC:     res.startedUTC,
					FinishedUTC:    res.finishedUTC,
				}
				if res.err != nil {
					storeResults[res.idx].Error = res.err.Error()
				}
				if res.err != nil {
					t.Fatalf("concurrent store worker %d failed: %v", res.idx, res.err)
				}
				ids[res.idx] = res.id
			}
			verifyConcurrentInvariantsG6(t, dbconn, &g6FailureDiagnosticContext{
				TestName:      t.Name(),
				Backend:       "postgres",
				OuterJobCodec: outerJobCodec,
				InnerSubtest:  codec,
				GOMAXPROCS:    runtime.GOMAXPROCS(0),
				Concurrency:   workers,
				IsolatedDB:    testDBName,
				TempRoot:      tmp,
				StoreResults:  storeResults,
			})

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
						t.Fatalf("chunk graph drift between concurrent stores at file=%d index=%d: base=%+v got=%+v", i, j, baseGraph[j], graph[j])
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

func TestAdversarialG6ConcurrentStoresSharedChunkInputsPreserveHealthyRestores(t *testing.T) {
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

			paths := testutils.CreateSampleDataset(t, inputDir)
			hybridA := paths["hybrid_a.bin"]
			hybridB := paths["hybrid_b.bin"]
			hashA := testutils.SHA256File(t, hybridA)
			hashB := testutils.SHA256File(t, hybridB)

			resultACh := make(chan error, 1)
			resultBCh := make(chan error, 1)
			var fileAID, fileBID int64
			go func() {
				var err error
				fileAID, err = storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, hybridA)
				resultACh <- err
			}()
			go func() {
				var err error
				fileBID, err = storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, hybridB)
				resultBCh <- err
			}()
			if errA := <-resultACh; errA != nil {
				t.Fatalf("concurrent store hybrid_a failed: %v", errA)
			}
			if errB := <-resultBCh; errB != nil {
				t.Fatalf("concurrent store hybrid_b failed: %v", errB)
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
