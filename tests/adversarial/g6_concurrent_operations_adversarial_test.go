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
	close(g.releaseCh)
}

func assertDeterministicG6ChunkState(t *testing.T, dbconn *sql.DB, chunkHash string, size int, wantLogicalRefs int) {
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
	if chunkStatus != "COMPLETED" {
		t.Fatalf("expected deterministic G6 chunk to be COMPLETED, got %s", chunkStatus)
	}

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
	cmd := exec.Command(binPath, "store", "--codec", codec, path, "--output", "json")
	cmd.Dir = repoRoot
	cmd.Env = testutils.BuildCommandEnv(env)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("store command: %w; output=%s", err, out)
	}
	payload, ok := testutils.TryParseLastJSONLine(string(out))
	if !ok {
		return 0, fmt.Errorf("no JSON in store output: %s", out)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("store payload missing data: %v", payload)
	}
	idF, ok := data["file_id"].(float64)
	if !ok {
		return 0, fmt.Errorf("store payload missing file_id: %v", data)
	}
	return int64(idF), nil
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

var g6ChunkIDPattern = regexp.MustCompile(`chunk (\d+)`)

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
	Worker int    `json:"worker"`
	FileID int64  `json:"file_id,omitempty"`
	Error  string `json:"error,omitempty"`
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
	if v, err := g6SchemaVersion(dbconn); err == nil {
		manifest.SchemaVersion = v
	} else {
		t.Logf("G6 verify diagnostics: schema version query failed: %v", err)
	}

	matches := g6ChunkIDPattern.FindStringSubmatch(verifyErr.Error())
	if len(matches) != 2 {
		logMixedChunkShapesG6(t, dbconn)
		writeConcurrentInvariantManifestG6(t, manifest)
		return
	}

	var chunkID int64
	if _, err := fmt.Sscanf(matches[1], "%d", &chunkID); err != nil {
		t.Logf("G6 verify diagnostics: parse chunk id from %q: %v", matches[1], err)
		logMixedChunkShapesG6(t, dbconn)
		writeConcurrentInvariantManifestG6(t, manifest)
		return
	}

	manifest.OffendingChunkID = &chunkID
	chunkMeta, chunkHash, err := logChunkMetadataG6(t, dbconn, chunkID)
	if err != nil {
		t.Logf("G6 verify diagnostics: collect chunk metadata chunk_id=%d: %v", chunkID, err)
		writeConcurrentInvariantManifestG6(t, manifest)
		return
	}
	manifest.OffendingChunkHash = chunkHash
	manifest.MigrationCompanionState = chunkMeta
	writeConcurrentInvariantManifestG6(t, manifest)
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

func logChunkMetadataG6(t *testing.T, dbconn *sql.DB, chunkID int64) (g6ChunkMetadataRecord, string, error) {
	t.Helper()

	type chunkMeta struct {
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

	var meta chunkMeta
	err := dbconn.QueryRow(`
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
	`, chunkID).Scan(
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
	if err != nil {
		return g6ChunkMetadataRecord{}, "", err
	}

	var payloadPrefixBytes *int64
	if meta.packedPlaintextSize.Valid && meta.totalReferencedBytes.Valid {
		v := meta.packedPlaintextSize.Int64 - meta.totalReferencedBytes.Int64
		payloadPrefixBytes = &v
	}

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

	legacyContainerFilename := logContainerMetadataG6(t, dbconn, "legacy", meta.legacyContainerID)
	packedContainerFilename := logContainerMetadataG6(t, dbconn, "packed", meta.packedContainerID)

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
	}, meta.chunkHash, nil
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
				idx int
				id  int64
				err error
			}
			resultCh := make(chan g6storeResult, workers)
			for i := 0; i < workers; i++ {
				i := i
				go func() {
					id, err := storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, inPath)
					resultCh <- g6storeResult{idx: i, id: id, err: err}
				}()
			}
			ids := make([]int64, workers)
			storeResults := make([]g6StoreOperationResult, workers)
			for i := 0; i < workers; i++ {
				res := <-resultCh
				storeResults[res.idx] = g6StoreOperationResult{
					Worker: res.idx,
					FileID: res.id,
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

	var retryCaseExecuted bool
	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, _, _, _, tmp, _ := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input-deterministic")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir deterministic input dir: %v", err)
			}

			inPath := filepath.Join(inputDir, fmt.Sprintf("g6-deterministic-%s.bin", codec))
			payload := []byte("g6-deterministic-controlled-interleaving-payload")
			if err := os.WriteFile(inPath, payload, 0o600); err != nil {
				t.Fatalf("write deterministic input: %v", err)
			}
			sum := sha256.Sum256(payload)
			chunkHash := hex.EncodeToString(sum[:])

			runCase := func(name string, target storage.TestStoreInterleavingEvent) {
				t.Run(name, func(t *testing.T) {
					gate := newG6DeterministicInterleavingGate()
					var fired bool
					sgctx, err := storage.LoadDefaultStorageContext()
					if err != nil {
						t.Fatalf("load default storage context: %v", err)
					}
					defer func() { _ = sgctx.Close() }()
					resetHooks := storage.InstallTestStoreInterleavingHooks(&sgctx, func(_ context.Context, event storage.TestStoreInterleavingHookEvent) error {
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

					storeCodec := blocks.CodecPlain
					if codec == "aes-gcm" {
						storeCodec = blocks.CodecAESGCM
					}

					done := make(chan error, 1)
					go func() {
						_, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, storeCodec)
						done <- err
					}()

					event := gate.await(t)
					if event.StoreOpID == "" {
						t.Fatal("expected deterministic G6 event to include store op id")
					}

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

					gate.release()
					select {
					case err := <-done:
						if err != nil {
							t.Fatalf("deterministic postgres store failed: %v", err)
						}
					case <-time.After(20 * time.Second):
						t.Fatal("timeout waiting for deterministic postgres store")
					}

					assertDeterministicG6ChunkState(t, dbconn, chunkHash, len(payload), 1)
					if err := verify.VerifyRepository(dbconn, container.ContainersDir); err != nil {
						t.Fatalf("full verify repository after deterministic postgres interleaving: %v", err)
					}
				})
			}

			runRetryCase := func() {
				t.Run("retry_path_remains_packed", func(t *testing.T) {
					retryCaseExecuted = true
					if _, err := dbconn.Exec(`DELETE FROM file_chunk`); err != nil {
						t.Fatalf("delete file_chunk before retry case: %v", err)
					}
					if _, err := dbconn.Exec(`DELETE FROM logical_file`); err != nil {
						t.Fatalf("delete logical_file before retry case: %v", err)
					}
					if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
						t.Fatalf("delete chunk_block_refs before retry case: %v", err)
					}
					if _, err := dbconn.Exec(`DELETE FROM blocks`); err != nil {
						t.Fatalf("delete blocks before retry case: %v", err)
					}
					if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
						t.Fatalf("delete storage_blocks before retry case: %v", err)
					}
					if _, err := dbconn.Exec(`DELETE FROM chunk`); err != nil {
						t.Fatalf("delete chunk before retry case: %v", err)
					}

					var chunkID int64
					if err := dbconn.QueryRow(
						`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count, chunker_version)
						 VALUES ($1, $2, $3, 0, 0, $4)
						 RETURNING id`,
						chunkHash,
						len(payload),
						"ABORTED",
						"v2-fastcdc",
					).Scan(&chunkID); err != nil {
						t.Fatalf("insert aborted retry chunk: %v", err)
					}

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

					storeCodec := blocks.CodecPlain
					if codec == "aes-gcm" {
						storeCodec = blocks.CodecAESGCM
					}

					if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, storeCodec); err != nil {
						t.Fatalf("store retry-case file: %v", err)
					}

					var sawRetryCAS bool
					var sawPackedMetadata bool
					var sawLegacyCompanion bool
					for _, event := range events {
						if event.ChunkID != chunkID {
							continue
						}
						switch event.Event {
						case storage.TestStoreInterleavingEventBeforeChunkRetryCAS:
							sawRetryCAS = true
						case storage.TestStoreInterleavingEventAfterPackedMetadata:
							sawPackedMetadata = true
						case storage.TestStoreInterleavingEventAfterLegacyCompanionInsert:
							sawLegacyCompanion = true
						}
					}
					if !sawRetryCAS || !sawPackedMetadata || !sawLegacyCompanion {
						t.Fatalf(
							"expected retry path to remain packed on postgres, got retry=%t packed=%t companion=%t events=%+v",
							sawRetryCAS,
							sawPackedMetadata,
							sawLegacyCompanion,
							events,
						)
					}
					assertDeterministicG6ChunkState(t, dbconn, chunkHash, len(payload), 1)
					if err := verify.VerifyRepository(dbconn, container.ContainersDir); err != nil {
						t.Fatalf("full verify repository after deterministic postgres retry case: %v", err)
					}
				})
			}

			runCase("packed_metadata", storage.TestStoreInterleavingEventAfterPackedMetadata)
			runCase("legacy_companion", storage.TestStoreInterleavingEventAfterLegacyCompanionInsert)
			runRetryCase()
		})
	}
	if requireDeterministicG6Env("COLDKEEP_REQUIRE_DETERMINISTIC_G6_RETRY_CASE") && !retryCaseExecuted {
		t.Fatal("deterministic G6 PostgreSQL regression did not execute retry_path_remains_packed")
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
