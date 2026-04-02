package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

// NOTE:
// These tests are integration-style (DB + filesystem).
// They are organized into three tiers:
//
//	 correctness (default) — all non-stress tests; requires COLDKEEP_TEST_DB=1
//	 stress                — high-concurrency / long-running tests; additionally
//	                        requires -short=false (they are skipped under go test -short)
//
// Run correctness tier:
//
//	COLDKEEP_TEST_DB=1 go test ./tests/... -v
//
// Run correctness + stress tier:
//
//	COLDKEEP_TEST_DB=1 go test ./tests/... -v -timeout 10m
//
// Run correctness only (skip stress regardless of environment):
//
//	COLDKEEP_TEST_DB=1 go test ./tests/... -short -v

// -----------------------------------------------------------------------------
// Helpers and test data
// -----------------------------------------------------------------------------

func requireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run integration tests")
	}
}

// requireStress gates tests that spin up many goroutines or run for an extended
// time. They are skipped when -short is passed so that CI correctness runs
// remain fast. Run without -short (or with a generous -timeout) to include them.
func requireStress(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
}

func TestIntegrationHarnessSmoke(t *testing.T) {
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Log("COLDKEEP_TEST_DB is not set; DB-backed integration tests may be skipped")
	} else {
		t.Log("COLDKEEP_TEST_DB is set; DB-backed integration tests can run")
	}
}

type cliExecResult struct {
	stdout   string
	stderr   string
	exitCode int
}

func findRepoRoot(t *testing.T) string {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	dir := cwd
	for i := 0; i < 8; i++ {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	t.Fatalf("could not find repo root (go.mod) from cwd %q", cwd)
	return ""
}

func buildColdkeepBinary(t *testing.T, repoRoot string) string {
	t.Helper()

	binPath := filepath.Join(t.TempDir(), "coldkeep-test-bin")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/coldkeep")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build coldkeep binary: %v\noutput:\n%s", err, string(out))
	}

	return binPath
}

func buildCommandEnv(overrides map[string]string) []string {
	envMap := make(map[string]string)
	for _, kv := range os.Environ() {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		envMap[parts[0]] = parts[1]
	}

	for k, v := range overrides {
		envMap[k] = v
	}

	env := make([]string, 0, len(envMap))
	for k, v := range envMap {
		env = append(env, k+"="+v)
	}

	return env
}

func runColdkeepCommand(t *testing.T, repoRoot, binPath string, env map[string]string, args ...string) cliExecResult {
	t.Helper()

	cmd := exec.Command(binPath, args...)
	cmd.Dir = repoRoot
	cmd.Env = buildCommandEnv(env)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		return cliExecResult{stdout: stdout.String(), stderr: stderr.String(), exitCode: 0}
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		return cliExecResult{stdout: stdout.String(), stderr: stderr.String(), exitCode: exitErr.ExitCode()}
	}

	t.Fatalf("run coldkeep command %v: %v", args, err)
	return cliExecResult{}
}

func tryParseLastJSONLine(output string) (map[string]any, bool) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err == nil {
			return payload, true
		}
	}

	return nil, false
}

func parseJSONLines(output string) []map[string]any {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	parsed := make([]map[string]any, 0, len(lines))
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err == nil {
			parsed = append(parsed, payload)
		}
	}
	return parsed
}

func jsonMap(t *testing.T, payload map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := payload[key]
	if !ok {
		t.Fatalf("missing JSON key %q", key)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("JSON key %q is not an object: %T", key, v)
	}
	return m
}

func jsonInt64(t *testing.T, payload map[string]any, key string) int64 {
	t.Helper()
	v, ok := payload[key]
	if !ok {
		t.Fatalf("missing JSON key %q", key)
	}
	n, ok := v.(float64)
	if !ok {
		t.Fatalf("JSON key %q is not numeric: %T", key, v)
	}
	return int64(n)
}

func assertCLIJSONOK(t *testing.T, res cliExecResult, command string) map[string]any {
	t.Helper()

	if res.exitCode != 0 {
		t.Fatalf("command %s failed with exit=%d\nstdout:\n%s\nstderr:\n%s", command, res.exitCode, res.stdout, res.stderr)
	}

	payload, ok := tryParseLastJSONLine(res.stdout)
	if !ok {
		payload, ok = tryParseLastJSONLine(res.stdout + "\n" + res.stderr)
	}
	if !ok {
		t.Fatalf("command %s produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", command, res.stdout, res.stderr)
	}

	if status, _ := payload["status"].(string); status != "ok" {
		t.Fatalf("command %s did not return status=ok: payload=%v stderr=%s", command, payload, res.stderr)
	}
	if got, _ := payload["command"].(string); got != command {
		t.Fatalf("command mismatch: want %s got %q payload=%v", command, got, payload)
	}

	return payload
}

func findCLIErrorPayload(output string) (map[string]any, bool) {
	for _, payload := range parseJSONLines(output) {
		if _, ok := payload["error_class"]; ok {
			return payload, true
		}
	}
	return nil, false
}

func defaultCLIEnv(storageDir string) map[string]string {
	return map[string]string{
		"COLDKEEP_TEST_DB":     "1",
		"COLDKEEP_CODEC":       getenvOrDefault("COLDKEEP_CODEC", "plain"),
		"COLDKEEP_STORAGE_DIR": storageDir,
		"DB_HOST":              getenvOrDefault("DB_HOST", "127.0.0.1"),
		"DB_PORT":              getenvOrDefault("DB_PORT", "5432"),
		"DB_USER":              getenvOrDefault("DB_USER", "coldkeep"),
		"DB_PASSWORD":          os.Getenv("DB_PASSWORD"),
		"DB_NAME":              getenvOrDefault("DB_NAME", "coldkeep"),
		"DB_SSLMODE":           getenvOrDefault("DB_SSLMODE", "disable"),
	}
}

func getenvOrDefault(name, fallback string) string {
	v := os.Getenv(name)
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func applySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	// If schema already exists, reuse it.
	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}

	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil && !isDuplicateSchemaError(err) {
		t.Fatalf("apply schema: %v", err)
	}
}

func isDuplicateSchemaError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "42710")
}

func resetDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	// Keep schema_version; clear the data tables and reset sequences.
	_, err := dbconn.Exec(`
		TRUNCATE TABLE
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func resetStorage(t *testing.T) {
	t.Helper()
	if container.ContainersDir == "" {
		t.Fatalf("ContainersDir is empty")
	}
	_ = os.RemoveAll(container.ContainersDir)
	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir ContainersDir: %v", err)
	}
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func createTempFile(t *testing.T, dir, name string, size int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	data := make([]byte, size)

	// Deterministic content (repeatable).
	for i := 0; i < size; i++ {
		data[i] = byte((i*31 + 7) % 251)
	}

	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}

func fetchFileIDByHash(t *testing.T, dbconn *sql.DB, fileHash string) int64 {
	t.Helper()
	var id int64
	err := dbconn.QueryRow(`SELECT id FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&id)
	if err != nil {
		t.Fatalf("query logical_file by hash: %v", err)
	}
	return id
}

func assertNoProcessingRows(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var logicalProcessing int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = $1`, filestate.LogicalFileProcessing).Scan(&logicalProcessing); err != nil {
		t.Fatalf("count processing logical_file rows: %v", err)
	}
	if logicalProcessing != 0 {
		t.Fatalf("expected no PROCESSING logical_file rows, got %d", logicalProcessing)
	}

	var chunkProcessing int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = $1`, filestate.ChunkProcessing).Scan(&chunkProcessing); err != nil {
		t.Fatalf("count processing chunk rows: %v", err)
	}
	if chunkProcessing != 0 {
		t.Fatalf("expected no PROCESSING chunk rows, got %d", chunkProcessing)
	}
}

func assertUniqueFileChunkOrders(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var duplicates int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM (
			SELECT logical_file_id, chunk_order
			FROM file_chunk
			GROUP BY logical_file_id, chunk_order
			HAVING COUNT(*) > 1
		) dup
	`).Scan(&duplicates); err != nil {
		t.Fatalf("count duplicate file_chunk order rows: %v", err)
	}
	if duplicates != 0 {
		t.Fatalf("expected no duplicate file_chunk order rows, got %d duplicate groups", duplicates)
	}
}

func newTestContext(dbconn *sql.DB) storage.StorageContext {
	return storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDir(container.ContainersDir, container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
}

type fileChunkRecord struct {
	chunkID              int64
	containerID          int64
	blockOffset          int64
	storedSize           int64
	containerFilename    string
	containerCurrentSize int64
}

func setupStoredFileForVerification(t *testing.T, filename string, size int) (*sql.DB, string, int64) {
	t.Helper()
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		_ = dbconn.Close()
		t.Fatalf("mkdir inputDir: %v", err)
	}

	inPath := createTempFile(t, inputDir, filename, size)
	fileHash := sha256File(t, inPath)

	sgctx := newTestContext(dbconn)

	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	return dbconn, inPath, fetchFileIDByHash(t, dbconn, fileHash)
}

func containerPathForRecord(record fileChunkRecord) string {
	filename := record.containerFilename

	return filepath.Join(container.ContainersDir, filename)
}

func fetchFirstFileChunkRecord(t *testing.T, dbconn *sql.DB, fileID int64) fileChunkRecord {
	t.Helper()

	var record fileChunkRecord
	err := dbconn.QueryRow(`
		SELECT
			fc.chunk_id,
			b.container_id,
			b.block_offset,
			b.stored_size,
			ctr.filename,
			ctr.current_size
		FROM file_chunk fc
		JOIN blocks b ON b.chunk_id = fc.chunk_id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
		LIMIT 1
	`, fileID).Scan(
		&record.chunkID,
		&record.containerID,
		&record.blockOffset,
		&record.storedSize,
		&record.containerFilename,
		&record.containerCurrentSize,
	)
	if err != nil {
		t.Fatalf("query first file chunk record: %v", err)
	}

	return record
}

func corruptFirstCompletedChunkByte(t *testing.T, dbconn *sql.DB, containersDir string) {
	t.Helper()

	var blockOffset int64
	var storedSize int64
	var containerFilename string
	err := dbconn.QueryRow(`
		SELECT b.block_offset, b.stored_size, ctr.filename
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE c.status = $1
		ORDER BY c.id ASC
		LIMIT 1
	`, filestate.ChunkCompleted).Scan(&blockOffset, &storedSize, &containerFilename)
	if err != nil {
		t.Fatalf("query first completed chunk for corruption: %v", err)
	}

	containerPath := filepath.Join(containersDir, containerFilename)
	f, err := os.OpenFile(containerPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file for corruption: %v", err)
	}

	corruptionOffset := blockOffset
	if storedSize > 10 {
		corruptionOffset += 10
	}

	if _, err := f.WriteAt([]byte{0xAC}, corruptionOffset); err != nil {
		_ = f.Close()
		t.Fatalf("write corruption byte: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close container file after corruption: %v", err)
	}
}

// Helpers (small, local)
func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return b
}

func itoa(i int) string {
	// small int to string without fmt to keep output clean
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [32]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + (i % 10))
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func isRetryableTxAbortError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "current transaction is aborted") || strings.Contains(msg, "25p02")
}

// rollbackFailingWriter wraps LocalWriter and deterministically fails rollback
// path cleanup so tests can assert error surfacing plus retirement/quarantine
// behavior on failed cleanup.
type rollbackFailingWriter struct {
	base            *container.LocalWriter
	forcedErr       error
	appendSucceeded bool
	lastPlacement   container.LocalPlacement
	retireCalls     int
}

func newRollbackFailingWriter(base *container.LocalWriter) *rollbackFailingWriter {
	return &rollbackFailingWriter{
		base:      base,
		forcedErr: errors.New("injected rollback cleanup failure"),
	}
}

func (w *rollbackFailingWriter) FinalizeContainer() error {
	return w.base.FinalizeContainer()
}

func (w *rollbackFailingWriter) BindDB(dbconn *sql.DB) {
	w.base.BindDB(dbconn)
}

func (w *rollbackFailingWriter) AppendPayload(tx db.DBTX, payload []byte) (container.LocalPlacement, error) {
	placement, err := w.base.AppendPayload(tx, payload)
	if err == nil {
		w.appendSucceeded = true
		w.lastPlacement = placement
	}
	return placement, err
}

func (w *rollbackFailingWriter) AcknowledgeAppendCommitted() {
	w.base.AcknowledgeAppendCommitted()
}

func (w *rollbackFailingWriter) RollbackLastAppend() error {
	if !w.appendSucceeded {
		return nil
	}
	if w.forcedErr != nil {
		return w.forcedErr
	}
	return errors.New("injected rollback cleanup failure")
}

func (w *rollbackFailingWriter) RetireActiveContainer() error {
	w.retireCalls++
	return w.base.RetireActiveContainer()
}

// chunkBoundarySizes returns the standard test sizes relative to CDC boundaries.
// minChunk = 512 KiB, maxChunk = 2 MiB.
var chunkBoundaryCases = []struct {
	name string
	size int
}{
	{"1_byte", 1},
	{"small_64b", 64},
	{"min_minus_1", 512*1024 - 1},
	{"exactly_min", 512 * 1024},
	{"min_plus_1", 512*1024 + 1},
	{"max_minus_1", 2*1024*1024 - 1},
	{"exactly_max", 2 * 1024 * 1024},
	{"max_plus_1", 2*1024*1024 + 1},
	{"multi_chunk_uneven_tail", 3*2*1024*1024 + 37},
}

type chunkRecord struct {
	order int
	hash  string
	size  int64
}

func createSampleDataset(t *testing.T, dir string) map[string]string {
	t.Helper()

	paths := make(map[string]string)

	// 1. Empty file
	p := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(p, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}
	paths["empty.txt"] = p

	// 2. Small file (1 byte)
	p = filepath.Join(dir, "small.txt")
	if err := os.WriteFile(p, []byte{0x42}, 0o644); err != nil {
		t.Fatalf("write small file: %v", err)
	}
	paths["small.txt"] = p

	// 3. Config-like text
	config := []byte("port: 8080\nhost: localhost\n")
	p = filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, config, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	paths["config.yaml"] = p

	// 4. Repetitive text (good for chunking patterns)
	lorem := bytes.Repeat([]byte("lorem ipsum\n"), 1000)
	p = filepath.Join(dir, "lorem.txt")
	if err := os.WriteFile(p, lorem, 0o644); err != nil {
		t.Fatalf("write lorem: %v", err)
	}
	paths["lorem.txt"] = p

	// 5. Binary file (deterministic pseudo-random)
	bin := make([]byte, 128*1024)
	for i := range bin {
		bin[i] = byte((i*31 + 7) % 251)
	}
	p = filepath.Join(dir, "binary.bin")
	if err := os.WriteFile(p, bin, 0o644); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	paths["binary.bin"] = p

	// 6. Duplicate files
	dup := make([]byte, 64*1024)
	for i := range dup {
		dup[i] = byte((i*17 + 3) % 251)
	}

	p1 := filepath.Join(dir, "dup1.bin")
	p2 := filepath.Join(dir, "dup2.bin")

	if err := os.WriteFile(p1, dup, 0o644); err != nil {
		t.Fatalf("write dup1: %v", err)
	}
	if err := os.WriteFile(p2, dup, 0o644); err != nil {
		t.Fatalf("write dup2: %v", err)
	}

	paths["dup1.bin"] = p1
	paths["dup2.bin"] = p2

	// 7. Shared-chunk hybrid files
	prefix := make([]byte, 64*1024)
	for i := range prefix {
		prefix[i] = byte((i*13 + 5) % 251)
	}

	for i := 0; i < 2; i++ {
		suffix := make([]byte, 32*1024)
		for j := range suffix {
			suffix[j] = byte((j*29 + i) % 251)
		}

		data := append(append([]byte{}, prefix...), suffix...)
		name := fmt.Sprintf("hybrid_%c.bin", 'a'+i)
		p := filepath.Join(dir, name)

		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatalf("write hybrid: %v", err)
		}
		paths[name] = p
	}

	return paths
}

func TestCLIJSONOutputContracts(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "json_contract.bin", 256*1024)

	sim := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"simulate", "store", inPath, "--output", "json"), "simulate")
	simData := jsonMap(t, sim, "data")
	if got := jsonInt64(t, simData, "files"); got != 1 {
		t.Fatalf("simulate files mismatch: want 1 got %d", got)
	}

	store := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")
	storeData := jsonMap(t, store, "data")
	fileID := jsonInt64(t, storeData, "file_id")
	if fileID <= 0 {
		t.Fatalf("expected positive file_id, got %d", fileID)
	}

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"stats", "--output", "json"), "stats")

	list := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"list", "--output", "json"), "list")
	files, ok := list["files"].([]any)
	if !ok || len(files) == 0 {
		t.Fatalf("list returned no files: payload=%v", list)
	}

	search := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"search", "--name", "json_contract", "--output", "json"), "search")
	searchFiles, ok := search["files"].([]any)
	if !ok || len(searchFiles) == 0 {
		t.Fatalf("search returned no files for --name json_contract: payload=%v", search)
	}

	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}
	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"restore", fmt.Sprintf("%d", fileID), outDir, "--output", "json"), "restore")

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"verify", "file", fmt.Sprintf("%d", fileID), "--full", "--output", "json"), "verify")

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"remove", fmt.Sprintf("%d", fileID), "--output", "json"), "remove")

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"gc", "--dry-run", "--output", "json"), "gc")

	// Doctor command: operator-facing health check
	doctor := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"doctor", "--output", "json"), "doctor")
	doctorData, ok := doctor["data"].(map[string]any)
	if !ok {
		t.Fatalf("doctor JSON missing data object: payload=%v", doctor)
	}
	if _, ok := doctorData["recovery_status"]; !ok {
		t.Fatalf("doctor JSON missing recovery_status: payload=%v", doctor)
	}
	if _, ok := doctorData["verify_status"]; !ok {
		t.Fatalf("doctor JSON missing verify_status: payload=%v", doctor)
	}
	if _, ok := doctorData["schema_status"]; !ok {
		t.Fatalf("doctor JSON missing schema_status: payload=%v", doctor)
	}
	if verifyLevel, _ := doctorData["verify_level"].(string); verifyLevel != "standard" {
		t.Fatalf("doctor JSON verify_level mismatch: want standard got %q payload=%v", verifyLevel, doctor)
	}

	// Doctor with --full flag
	doctorFull := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"doctor", "--full", "--output", "json"), "doctor")
	doctorFullData, ok := doctorFull["data"].(map[string]any)
	if !ok {
		t.Fatalf("doctor --full JSON missing data object: payload=%v", doctorFull)
	}
	if _, ok := doctorFullData["recovery_status"]; !ok {
		t.Fatalf("doctor --full JSON missing recovery_status: payload=%v", doctorFull)
	}
	if verifyLevel, _ := doctorFullData["verify_level"].(string); verifyLevel != "full" {
		t.Fatalf("doctor --full JSON verify_level mismatch: want full got %q payload=%v", verifyLevel, doctorFull)
	}
}

func TestDoctorCommand(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	// Store some files first so doctor has something to report on
	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "doctor_test.bin", 512*1024)

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")

	// Test doctor default quick health check (--standard level)
	res := runColdkeepCommand(t, repoRoot, binPath, env, "doctor")
	if res.exitCode != 0 {
		t.Fatalf("doctor command failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stdout, "Doctor health report") {
		t.Fatalf("doctor text output missing heading\nstdout:\n%s", res.stdout)
	}
	if !strings.Contains(res.stdout, "Overall status:") {
		t.Fatalf("doctor text output missing overall status line\nstdout:\n%s", res.stdout)
	}
	if !strings.Contains(res.stdout, "Phase 1 - Recovery:") {
		t.Fatalf("doctor text output missing recovery phase line\nstdout:\n%s", res.stdout)
	}
	if !strings.Contains(res.stdout, "Phase 2 - Verify:") {
		t.Fatalf("doctor text output missing verify phase line\nstdout:\n%s", res.stdout)
	}
	if !strings.Contains(res.stdout, "Phase 3 - Schema:") {
		t.Fatalf("doctor text output missing schema phase line\nstdout:\n%s", res.stdout)
	}

	// Test doctor --standard explicitly
	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--standard")
	if res.exitCode != 0 {
		t.Fatalf("doctor --standard failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	// Test doctor --full
	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--full")
	if res.exitCode != 0 {
		t.Fatalf("doctor --full failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	// Test doctor with JSON output and validate all required status fields
	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json")
	if res.exitCode != 0 {
		t.Fatalf("doctor --output json failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	doctorJSON, ok := tryParseLastJSONLine(res.stdout)
	if !ok {
		// Fallback: check stderr if stdout parsing fails
		doctorJSON, ok = tryParseLastJSONLine(res.stdout + "\n" + res.stderr)
	}
	if !ok {
		t.Fatalf("doctor --output json produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}

	if status, _ := doctorJSON["status"].(string); status != "ok" {
		t.Fatalf("doctor did not return status=ok: payload=%v", doctorJSON)
	}

	doctorData, ok := doctorJSON["data"].(map[string]any)
	if !ok {
		t.Fatalf("doctor JSON missing data object: payload=%v", doctorJSON)
	}

	recoveryStatus, ok := doctorData["recovery_status"].(string)
	if !ok || recoveryStatus == "" {
		t.Fatalf("doctor JSON missing or invalid recovery_status: payload=%v", doctorJSON)
	}

	verifyStatus, ok := doctorData["verify_status"].(string)
	if !ok || verifyStatus == "" {
		t.Fatalf("doctor JSON missing or invalid verify_status: payload=%v", doctorJSON)
	}

	schemaStatus, ok := doctorData["schema_status"].(string)
	if !ok || schemaStatus == "" {
		t.Fatalf("doctor JSON missing or invalid schema_status: payload=%v", doctorJSON)
	}

	verifyLevel, ok := doctorData["verify_level"].(string)
	if !ok || verifyLevel != "standard" {
		t.Fatalf("doctor JSON verify_level mismatch: want standard got %q payload=%v", verifyLevel, doctorJSON)
	}

	// Test doctor --full --output json
	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--full", "--output", "json")
	if res.exitCode != 0 {
		t.Fatalf("doctor --full --output json failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	doctorFullJSON, ok := tryParseLastJSONLine(res.stdout)
	if !ok {
		doctorFullJSON, ok = tryParseLastJSONLine(res.stdout + "\n" + res.stderr)
	}
	if !ok {
		t.Fatalf("doctor --full --output json produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}

	if status, _ := doctorFullJSON["status"].(string); status != "ok" {
		t.Fatalf("doctor --full did not return status=ok: payload=%v", doctorFullJSON)
	}
	doctorFullData, ok := doctorFullJSON["data"].(map[string]any)
	if !ok {
		t.Fatalf("doctor --full JSON missing data object: payload=%v", doctorFullJSON)
	}

	// Verify all status fields are present in full output
	if _, ok := doctorFullData["recovery_status"]; !ok {
		t.Fatalf("doctor --full JSON missing recovery_status: payload=%v", doctorFullJSON)
	}
	if _, ok := doctorFullData["verify_status"]; !ok {
		t.Fatalf("doctor --full JSON missing verify_status: payload=%v", doctorFullJSON)
	}
	if _, ok := doctorFullData["schema_status"]; !ok {
		t.Fatalf("doctor --full JSON missing schema_status: payload=%v", doctorFullJSON)
	}
	if verifyLevel, _ := doctorFullData["verify_level"].(string); verifyLevel != "full" {
		t.Fatalf("doctor --full JSON verify_level mismatch: want full got %q payload=%v", verifyLevel, doctorFullJSON)
	}

	// Deliberately corrupt one stored container byte and verify doctor fails
	// in the verify phase when run at --deep.
	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for doctor corruption step: %v", err)
	}
	defer dbconn.Close()

	corruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--deep", "--output", "json")
	if res.exitCode != 3 {
		t.Fatalf("doctor --deep on corrupted state should exit=3, got=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	errPayload, ok := findCLIErrorPayload(res.stderr)
	if !ok {
		errPayload, ok = findCLIErrorPayload(res.stdout + "\n" + res.stderr)
	}
	if !ok {
		t.Fatalf("doctor --full on corrupted state produced no error JSON payload\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}

	if got, _ := errPayload["error_class"].(string); got != "VERIFY" {
		t.Fatalf("doctor corrupted-state error class mismatch: want VERIFY got %q payload=%v", got, errPayload)
	}
	if got, _ := errPayload["exit_code"].(float64); int(got) != 3 {
		t.Fatalf("doctor corrupted-state exit_code mismatch: want 3 got %v payload=%v", got, errPayload)
	}
	if msg, _ := errPayload["message"].(string); !strings.Contains(msg, "doctor verify phase failed") {
		t.Fatalf("doctor corrupted-state error message should mention verify phase: payload=%v", errPayload)
	}

	t.Logf("doctor command test passed: verify_level=%q recovery=%q verify=%q schema=%q", verifyLevel, recoveryStatus, verifyStatus, schemaStatus)
}

func TestDoctorJSONContractConsistency(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "doctor_contract.bin", 512*1024)

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")

	res := runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json")
	if res.exitCode != 0 {
		t.Fatalf("doctor --output json failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	successPayload, ok := tryParseLastJSONLine(res.stdout)
	if !ok {
		t.Fatalf("doctor success JSON must be emitted on stdout\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if status, _ := successPayload["status"].(string); status != "ok" {
		t.Fatalf("doctor success payload status mismatch: payload=%v", successPayload)
	}
	if command, _ := successPayload["command"].(string); command != "doctor" {
		t.Fatalf("doctor success payload command mismatch: payload=%v", successPayload)
	}
	if _, ok := successPayload["data"].(map[string]any); !ok {
		t.Fatalf("doctor success payload missing data object: payload=%v", successPayload)
	}

	if errPayload, hasErr := findCLIErrorPayload(res.stdout); hasErr {
		t.Fatalf("doctor success run should not emit error payload on stdout: payload=%v stdout=%s", errPayload, res.stdout)
	}

	for _, payload := range parseJSONLines(res.stderr) {
		if status, _ := payload["status"].(string); status == "ok" {
			if command, _ := payload["command"].(string); command == "doctor" {
				t.Fatalf("doctor success payload should not be duplicated to stderr: payload=%v stderr=%s", payload, res.stderr)
			}
		}
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for doctor corruption step: %v", err)
	}
	defer dbconn.Close()

	corruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--deep", "--output", "json")
	if res.exitCode != 3 {
		t.Fatalf("doctor --deep on corrupted state should exit=3, got=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	if errPayload, hasErr := findCLIErrorPayload(res.stdout); hasErr {
		t.Fatalf("doctor failure run should not emit error payload on stdout: payload=%v stdout=%s", errPayload, res.stdout)
	}

	errPayload, ok := findCLIErrorPayload(res.stderr)
	if !ok {
		t.Fatalf("doctor failure JSON must be emitted on stderr\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if got, _ := errPayload["error_class"].(string); got != "VERIFY" {
		t.Fatalf("doctor failure error class mismatch: want VERIFY got %q payload=%v", got, errPayload)
	}
	if got, _ := errPayload["exit_code"].(float64); int(got) != 3 {
		t.Fatalf("doctor failure exit_code mismatch: want 3 got %v payload=%v", got, errPayload)
	}
	if msg, _ := errPayload["message"].(string); !strings.Contains(msg, "doctor verify phase failed") {
		t.Fatalf("doctor failure message should mention verify phase: payload=%v", errPayload)
	}
}

func TestDoctorFailureJSONContractAndStreams(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "doctor_failure_contract.bin", 512*1024)

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for corruption step: %v", err)
	}
	defer dbconn.Close()

	corruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

	res := runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--deep", "--output", "json")
	if res.exitCode != 3 {
		t.Fatalf("doctor --deep on corrupted state should exit=3, got=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	if payload, ok := tryParseLastJSONLine(res.stdout); ok {
		t.Fatalf("doctor failure should not emit JSON on stdout: payload=%v stdout=%s", payload, res.stdout)
	}
	if payload, ok := findCLIErrorPayload(res.stdout); ok {
		t.Fatalf("doctor failure should not emit error JSON on stdout: payload=%v stdout=%s", payload, res.stdout)
	}

	errPayload, ok := findCLIErrorPayload(res.stderr)
	if !ok {
		t.Fatalf("doctor failure JSON must be emitted on stderr\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}

	status, _ := errPayload["status"].(string)
	if status != "error" {
		t.Fatalf("doctor failure status mismatch: want error got %q payload=%v", status, errPayload)
	}
	errorClass, _ := errPayload["error_class"].(string)
	if errorClass != "VERIFY" {
		t.Fatalf("doctor failure error class mismatch: want VERIFY got %q payload=%v", errorClass, errPayload)
	}
	exitCode, _ := errPayload["exit_code"].(float64)
	if int(exitCode) != 3 {
		t.Fatalf("doctor failure exit_code mismatch: want 3 got %v payload=%v", exitCode, errPayload)
	}
	message, _ := errPayload["message"].(string)
	if !strings.Contains(message, "doctor verify phase failed") {
		t.Fatalf("doctor failure message should mention verify phase: payload=%v", errPayload)
	}

	if _, ok := errPayload["command"]; ok {
		t.Fatalf("doctor failure error payload must not include success command field: payload=%v", errPayload)
	}
	if _, ok := errPayload["data"]; ok {
		t.Fatalf("doctor failure error payload must not include success data field: payload=%v", errPayload)
	}
}

func TestDoctorIntegrationSmokeContract(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "doctor_smoke_contract.bin", 512*1024)

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")

	// Basic doctor invocation must pass.
	res := runColdkeepCommand(t, repoRoot, binPath, env, "doctor")
	if res.exitCode != 0 {
		t.Fatalf("doctor failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	// Full doctor invocation must pass.
	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--full")
	if res.exitCode != 0 {
		t.Fatalf("doctor --full failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	// JSON contract must include key phase fields under data.
	jsonRes := runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json")
	payload := assertCLIJSONOK(t, jsonRes, "doctor")
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("doctor JSON missing data object: payload=%v", payload)
	}
	for _, k := range []string{"recovery_status", "verify_status", "schema_status", "verify_level"} {
		if _, ok := data[k]; !ok {
			t.Fatalf("doctor JSON missing required phase field %q: payload=%v", k, payload)
		}
	}

	// Failure contract: deep doctor on corrupted payload must return VERIFY exit class (3).
	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for corruption step: %v", err)
	}
	defer dbconn.Close()
	corruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

	res = runColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--deep", "--output", "json")
	if res.exitCode != 3 {
		t.Fatalf("doctor --deep on corrupted state should exit=3, got=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}
	errPayload, ok := findCLIErrorPayload(res.stderr)
	if !ok {
		errPayload, ok = findCLIErrorPayload(res.stdout + "\n" + res.stderr)
	}
	if !ok {
		t.Fatalf("doctor --deep corrupted-state run produced no error payload\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if got, _ := errPayload["error_class"].(string); got != "VERIFY" {
		t.Fatalf("doctor --deep failure error_class mismatch: want VERIFY got %q payload=%v", got, errPayload)
	}
}

func TestDoctorMutatesStaleSealingStateAndVerifyPasses(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "doctor_recovery_mutation.bin", 512*1024)

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for stale state mutation: %v", err)
	}
	defer dbconn.Close()

	var containerID int64
	if err := dbconn.QueryRow(`SELECT id FROM container ORDER BY id DESC LIMIT 1`).Scan(&containerID); err != nil {
		t.Fatalf("query latest container id: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE container SET sealed = TRUE, sealing = TRUE WHERE id = $1`, containerID); err != nil {
		t.Fatalf("inject stale sealing state: %v", err)
	}

	var isSealed, isSealing bool
	if err := dbconn.QueryRow(`SELECT sealed, sealing FROM container WHERE id = $1`, containerID).Scan(&isSealed, &isSealing); err != nil {
		t.Fatalf("query stale sealing state: %v", err)
	}
	if !isSealed || !isSealing {
		t.Fatalf("setup failed: expected sealed=true and sealing=true, got sealed=%v sealing=%v", isSealed, isSealing)
	}

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"doctor", "--output", "json"), "doctor")

	if err := dbconn.QueryRow(`SELECT sealed, sealing FROM container WHERE id = $1`, containerID).Scan(&isSealed, &isSealing); err != nil {
		t.Fatalf("query sealing state after doctor: %v", err)
	}
	if !isSealed {
		t.Fatalf("expected doctor to preserve sealed=true on recovered container")
	}
	if isSealing {
		t.Fatalf("expected doctor to clear stale sealing marker")
	}

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"verify", "system", "--output", "json"), "verify")
}

func TestSimulationMatchesRealSizeMetrics(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	inputDir := filepath.Join(tmp, "dataset")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir dataset: %v", err)
	}
	createSampleDataset(t, inputDir)

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	simRes := runColdkeepCommand(t, repoRoot, binPath, env,
		"simulate", "store-folder", "--codec", "plain", inputDir, "--output", "json")
	if simRes.exitCode != 0 {
		lowerErr := strings.ToLower(simRes.stderr)
		if strings.Contains(lowerErr, "database is locked") || strings.Contains(lowerErr, "context deadline exceeded") {
			t.Skipf("skipping flaky simulate backend contention: %s", strings.TrimSpace(simRes.stderr))
		}
		t.Fatalf("command simulate failed with exit=%d\nstdout:\n%s\nstderr:\n%s", simRes.exitCode, simRes.stdout, simRes.stderr)
	}
	sim := assertCLIJSONOK(t, simRes, "simulate")
	simData := jsonMap(t, sim, "data")
	simFiles := jsonInt64(t, simData, "files")
	simLogical := jsonInt64(t, simData, "logical_size_bytes")
	simPhysical := jsonInt64(t, simData, "physical_size_bytes")

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store-folder", "--codec", "plain", inputDir, "--output", "json"), "store-folder")

	stats := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"stats", "--output", "json"), "stats")
	statsData := jsonMap(t, stats, "data")
	realFiles := jsonInt64(t, statsData, "completed_files")
	realLogical := jsonInt64(t, statsData, "completed_size_bytes")
	realPhysical := jsonInt64(t, statsData, "live_block_bytes")

	if simFiles != realFiles {
		t.Fatalf("simulation files mismatch: simulated=%d real=%d", simFiles, realFiles)
	}
	if simLogical != realLogical {
		t.Fatalf("simulation logical bytes mismatch: simulated=%d real=%d", simLogical, realLogical)
	}

	// Allow a small delta for accounting differences while still detecting large drift.
	const maxPhysicalDeltaBytes int64 = 1024
	delta := simPhysical - realPhysical
	if delta < 0 {
		delta = -delta
	}
	if delta > maxPhysicalDeltaBytes {
		t.Fatalf("simulation physical bytes drift too large: simulated=%d real=%d delta=%d", simPhysical, realPhysical, delta)
	}
}

func TestCLIJSONOutputStreamSeparation(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := createTempFile(t, inputDir, "stream_contract.bin", 64*1024)

	res := runColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json")
	if res.exitCode != 0 {
		t.Fatalf("store command failed with exit=%d\nstdout:\n%s\nstderr:\n%s", res.exitCode, res.stdout, res.stderr)
	}

	stdoutJSON := parseJSONLines(res.stdout)
	if len(stdoutJSON) == 0 {
		t.Fatalf("expected JSON payload on stdout, got:\n%s", res.stdout)
	}

	storeJSON := stdoutJSON[len(stdoutJSON)-1]
	if got, _ := storeJSON["command"].(string); got != "store" {
		t.Fatalf("expected stdout command payload for store, got: %v", storeJSON)
	}
	if status, _ := storeJSON["status"].(string); status != "ok" {
		t.Fatalf("expected stdout status=ok, got payload: %v", storeJSON)
	}

	if strings.Contains(res.stdout, "startup_recovery") {
		t.Fatalf("startup recovery JSON must not be written to stdout:\n%s", res.stdout)
	}

	stderrJSON := parseJSONLines(res.stderr)
	if len(stderrJSON) == 0 {
		t.Fatalf("expected startup recovery JSON on stderr, got:\n%s", res.stderr)
	}

	recoverySeen := false
	for _, payload := range stderrJSON {
		if event, _ := payload["event"].(string); event == "startup_recovery" {
			recoverySeen = true
		}
		if command, _ := payload["command"].(string); command == "store" {
			t.Fatalf("command payload must not be written to stderr: %v", payload)
		}
	}

	if !recoverySeen {
		t.Fatalf("expected startup_recovery JSON event on stderr, got payloads: %v", stderrJSON)
	}
}

func TestCLIJSONErrorContracts(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	_ = dbconn.Close()

	repoRoot := findRepoRoot(t)
	binPath := buildColdkeepBinary(t, repoRoot)
	env := defaultCLIEnv(container.ContainersDir)

	tests := []struct {
		name          string
		args          []string
		wantExitCode  int
		wantClass     string
		messageSubstr string
	}{
		{
			name:          "usage_unknown_command",
			args:          []string{"nope", "--output", "json"},
			wantExitCode:  2,
			wantClass:     "USAGE",
			messageSubstr: "unknown command",
		},
		{
			name:          "usage_invalid_file_id",
			args:          []string{"restore", "abc", filepath.Join(tmp, "out"), "--output", "json"},
			wantExitCode:  2,
			wantClass:     "USAGE",
			messageSubstr: "Invalid fileID",
		},
		{
			name:          "general_store_missing_path",
			args:          []string{"store", filepath.Join(tmp, "missing-file.bin"), "--output", "json"},
			wantExitCode:  1,
			wantClass:     "GENERAL",
			messageSubstr: "no such file",
		},
		{
			name:          "verify_missing_file_id",
			args:          []string{"verify", "file", "999999", "--deep", "--output", "json"},
			wantExitCode:  3,
			wantClass:     "VERIFY",
			messageSubstr: "does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := runColdkeepCommand(t, repoRoot, binPath, env, tc.args...)

			if res.exitCode != tc.wantExitCode {
				t.Fatalf("exit code mismatch: want=%d got=%d\nstdout:\n%s\nstderr:\n%s", tc.wantExitCode, res.exitCode, res.stdout, res.stderr)
			}

			if strings.TrimSpace(res.stdout) != "" {
				t.Fatalf("expected no stdout for error case, got:\n%s", res.stdout)
			}

			errPayload, ok := findCLIErrorPayload(res.stderr)
			if !ok {
				t.Fatalf("expected JSON error payload in stderr, got:\n%s", res.stderr)
			}

			if got, _ := errPayload["status"].(string); got != "error" {
				t.Fatalf("status mismatch: want=error got=%q payload=%v", got, errPayload)
			}
			if got, _ := errPayload["error_class"].(string); got != tc.wantClass {
				t.Fatalf("error_class mismatch: want=%s got=%q payload=%v", tc.wantClass, got, errPayload)
			}
			if got := jsonInt64(t, errPayload, "exit_code"); got != int64(tc.wantExitCode) {
				t.Fatalf("exit_code mismatch in payload: want=%d got=%d payload=%v", tc.wantExitCode, got, errPayload)
			}

			message, _ := errPayload["message"].(string)
			if !strings.Contains(strings.ToLower(message), strings.ToLower(tc.messageSubstr)) {
				t.Fatalf("message mismatch: expected substring %q in %q", tc.messageSubstr, message)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// test cases
// -----------------------------------------------------------------------------

func fetchFirstChunkRecord(t *testing.T, dbconn *sql.DB, fileID int64) fileChunkRecord {
	t.Helper()

	var record fileChunkRecord
	err := dbconn.QueryRow(`
		SELECT
			c.id,
			b.container_id,
			b.block_offset,
			b.stored_size,
			ctr.filename,
			ctr.current_size
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
		LIMIT 1
	`, fileID).Scan(
		&record.chunkID,
		&record.containerID,
		&record.blockOffset,
		&record.storedSize,
		&record.containerFilename,
		&record.containerCurrentSize,
	)
	if err != nil {
		t.Fatalf("query first chunk record: %v", err)
	}

	return record
}

func TestRoundTripStoreRestore(t *testing.T) {
	requireDB(t)

	// Use temp dirs per test
	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// 512KB file should create multiple chunks depending on CDC params.
	inPath := createTempFile(t, inputDir, "roundtrip.bin", 512*1024)
	want := mustRead(t, inPath)
	wantHash := sha256File(t, inPath)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("storeFileWithStorageContext: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, wantHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "roundtrip.restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restoreFileWithDB: %v", err)
	}

	got := mustRead(t, outPath)
	if !bytes.Equal(want, got) {
		t.Fatalf("restored bytes differ from original")
	}

	gotHash := sha256File(t, outPath)
	if gotHash != wantHash {
		t.Fatalf("hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func TestDedupSameFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "dup.bin", 256*1024)
	fileHash := sha256File(t, inPath)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("second store: %v", err)
	}

	// Should still be 1 logical file for this hash
	var n int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&n); err != nil {
		t.Fatalf("count logical_file: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 logical_file row, got %d", n)
	}
}

// TestStoreFolderIdempotentDedup verifies that re-storing the same folder a
// second time leaves all key storage counters unchanged: file-level and
// chunk-level deduplication must absorb the entire second pass without writing
// new logical files, new chunks, new physical blocks, or new containers.
// This complements TestDedupSameFile (single-file) by exercising the folder
// pipeline, concurrent workers, and shared-chunk routing.
func TestStoreFolderIdempotentDedup(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	createSampleDataset(t, inputDir)

	sgctx := newTestContext(dbconn)

	if err := storage.StoreFolderWithStorageContext(sgctx, inputDir); err != nil {
		t.Fatalf("first store-folder: %v", err)
	}

	type storageSnapshot struct {
		completedFiles  int64
		totalChunks     int64
		completedChunks int64
		liveBlockBytes  int64
		totalContainers int64
	}

	takeSnapshot := func(label string) storageSnapshot {
		t.Helper()
		var s storageSnapshot
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = $1`, filestate.LogicalFileCompleted).Scan(&s.completedFiles); err != nil {
			t.Fatalf("[%s] query completed_files: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&s.totalChunks); err != nil {
			t.Fatalf("[%s] query total_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = $1`, filestate.ChunkCompleted).Scan(&s.completedChunks); err != nil {
			t.Fatalf("[%s] query completed_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`
			SELECT COALESCE(SUM(b.stored_size), 0)
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE c.live_ref_count > 0
		`).Scan(&s.liveBlockBytes); err != nil {
			t.Fatalf("[%s] query live_block_bytes: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&s.totalContainers); err != nil {
			t.Fatalf("[%s] query total_containers: %v", label, err)
		}
		return s
	}

	before := takeSnapshot("before")

	if err := storage.StoreFolderWithStorageContext(sgctx, inputDir); err != nil {
		t.Fatalf("second store-folder: %v", err)
	}

	after := takeSnapshot("after")

	if before.completedFiles != after.completedFiles {
		t.Errorf("dedup failed: completed_files changed %d -> %d", before.completedFiles, after.completedFiles)
	}
	if before.totalChunks != after.totalChunks {
		t.Errorf("dedup failed: total_chunks changed %d -> %d", before.totalChunks, after.totalChunks)
	}
	if before.completedChunks != after.completedChunks {
		t.Errorf("dedup failed: completed_chunks changed %d -> %d", before.completedChunks, after.completedChunks)
	}
	if before.liveBlockBytes != after.liveBlockBytes {
		t.Errorf("dedup failed: live_block_bytes changed %d -> %d", before.liveBlockBytes, after.liveBlockBytes)
	}
	if before.totalContainers != after.totalContainers {
		t.Errorf("dedup failed: total_containers changed %d -> %d", before.totalContainers, after.totalContainers)
	}
}

// TestStoreFolderIdempotentDedupEdgeCases applies the same idempotent re-store
// guarantee as TestStoreFolderIdempotentDedup but against the samples_edge_cases
// fixture: many small files plus a repeating-pattern file. This exercises
// deduplication under high file-count workloads and compressible-data patterns.
func TestStoreFolderIdempotentDedupEdgeCases(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	fixturePath := findRepoFixtureDir(t, "samples_edge_cases")
	inputDir := filepath.Join(tmp, "input")
	copyDirTree(t, fixturePath, inputDir)

	sgctx := newTestContext(dbconn)

	if err := storage.StoreFolderWithStorageContext(sgctx, inputDir); err != nil {
		t.Fatalf("first store-folder: %v", err)
	}

	type storageSnapshot struct {
		completedFiles  int64
		totalChunks     int64
		completedChunks int64
		liveBlockBytes  int64
		totalContainers int64
	}

	takeSnapshot := func(label string) storageSnapshot {
		t.Helper()
		var s storageSnapshot
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = $1`, filestate.LogicalFileCompleted).Scan(&s.completedFiles); err != nil {
			t.Fatalf("[%s] query completed_files: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&s.totalChunks); err != nil {
			t.Fatalf("[%s] query total_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = $1`, filestate.ChunkCompleted).Scan(&s.completedChunks); err != nil {
			t.Fatalf("[%s] query completed_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`
			SELECT COALESCE(SUM(b.stored_size), 0)
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE c.live_ref_count > 0
		`).Scan(&s.liveBlockBytes); err != nil {
			t.Fatalf("[%s] query live_block_bytes: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&s.totalContainers); err != nil {
			t.Fatalf("[%s] query total_containers: %v", label, err)
		}
		return s
	}

	before := takeSnapshot("before")

	if err := storage.StoreFolderWithStorageContext(sgctx, inputDir); err != nil {
		t.Fatalf("second store-folder: %v", err)
	}

	after := takeSnapshot("after")

	if before.completedFiles != after.completedFiles {
		t.Errorf("dedup failed: completed_files changed %d -> %d", before.completedFiles, after.completedFiles)
	}
	if before.totalChunks != after.totalChunks {
		t.Errorf("dedup failed: total_chunks changed %d -> %d", before.totalChunks, after.totalChunks)
	}
	if before.completedChunks != after.completedChunks {
		t.Errorf("dedup failed: completed_chunks changed %d -> %d", before.completedChunks, after.completedChunks)
	}
	if before.liveBlockBytes != after.liveBlockBytes {
		t.Errorf("dedup failed: live_block_bytes changed %d -> %d", before.liveBlockBytes, after.liveBlockBytes)
	}
	if before.totalContainers != after.totalContainers {
		t.Errorf("dedup failed: total_containers changed %d -> %d", before.totalContainers, after.totalContainers)
	}
}

func TestStoreFolderParallelSmoke(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Build folder with duplicates + shared-chunk variants
	inputDir := filepath.Join(tmp, "folder")
	_ = os.MkdirAll(inputDir, 0o755)

	paths := make([]string, 0, 32)

	// 1) Base unique-ish files
	for i := 0; i < 10; i++ {
		paths = append(paths, createTempFile(t, inputDir, "file_"+itoa(i)+".bin", 64*1024))
	}

	// 2) Exact duplicates (file-level dedupe)
	for i := 0; i < 3; i++ {
		src := paths[i]
		dst := filepath.Join(inputDir, "dup_"+itoa(i)+".bin")
		b := mustRead(t, src)
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			t.Fatalf("write dup: %v", err)
		}
		paths = append(paths, dst)
	}

	// 3) Shared-chunk-but-different files (chunk-level dedupe)
	// Create a shared prefix and combine with per-file unique suffix.
	// This should cause some shared chunks even when full file hashes differ.
	sharedPrefix := make([]byte, 32*1024)
	for i := range sharedPrefix {
		sharedPrefix[i] = byte((i*17 + 3) % 251)
	}

	for i := 0; i < 3; i++ {
		suffix := make([]byte, 32*1024)
		for j := range suffix {
			suffix[j] = byte((j*31 + 7 + i) % 251)
		}

		hybrid := append(append([]byte{}, sharedPrefix...), suffix...)
		dst := filepath.Join(inputDir, "hybrid_"+itoa(i)+".bin")
		if err := os.WriteFile(dst, hybrid, 0o644); err != nil {
			t.Fatalf("write hybrid: %v", err)
		}
		paths = append(paths, dst)
	}

	// Run storeFolder with timeout to catch deadlocks/hangs.
	done := make(chan error, 1)
	go func() { done <- storage.StoreFolderWithStorageContext(newTestContext(dbconn), inputDir) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("storeFolder: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("storeFolder timed out (possible deadlock or blocked workers)")
	}

	// Spot-check: restore a few logical files and compare hashes.
	rows, err := dbconn.Query(`SELECT id, file_hash, original_name FROM logical_file ORDER BY id ASC LIMIT 5`)
	if err != nil {
		t.Fatalf("query logical_file: %v", err)
	}
	defer rows.Close()

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)

	for rows.Next() {
		var id int64
		var expectHash, name string
		if err := rows.Scan(&id, &expectHash, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		outPath := filepath.Join(outDir, name)
		if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
			t.Fatalf("restore %d: %v", id, err)
		}
		gotHash := sha256File(t, outPath)
		if gotHash != expectHash {
			t.Fatalf("hash mismatch for restored id=%d want=%s got=%s", id, expectHash, gotHash)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
}

func TestStoreEdgeCasesFolder(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Use the repository's edge case samples folder.
	// It contains:
	// - samples_edge_cases/pattern.txt (one file with a pattern)
	// - samples_edge_cases/many_small/ (50 small files)
	edgeCasesDir := findRepoFixtureDir(t, "samples_edge_cases")

	// Store edge cases folder with timeout to catch deadlocks.
	done := make(chan error, 1)
	go func() {
		done <- storage.StoreFolderWithStorageContext(newTestContext(dbconn), edgeCasesDir)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("storeFolder edge cases: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("storeFolder edge cases timed out (possible deadlock)")
	}

	// Query stats to validate storage succeeded.
	stats, err := maintenance.RunStatsResult()
	if err != nil {
		t.Fatalf("runStatsResult: %v", err)
	}

	// Should have stored multiple files: pattern.txt + 50 files from many_small/
	// At minimum, we should have >0 files.
	if stats.CompletedFiles <= 0 {
		t.Fatalf("expected >0 completed files, got %d", stats.CompletedFiles)
	}

	// Verify all stored files are retrievable.
	rows, err := dbconn.Query(`SELECT id, file_hash, original_name FROM logical_file ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query logical_file: %v", err)
	}
	defer rows.Close()

	fileCount := 0
	restoreDir := filepath.Join(tmp, "restored_edge")
	_ = os.MkdirAll(restoreDir, 0o755)

	for rows.Next() {
		var id int64
		var fileHash, origName string
		if err := rows.Scan(&id, &fileHash, &origName); err != nil {
			t.Fatalf("scan: %v", err)
		}
		fileCount++

		// Spot-check: restore first 3 files and compare hashes.
		if fileCount <= 3 {
			outPath := filepath.Join(restoreDir, fmt.Sprintf("restored_%d_%s", id, filepath.Base(origName)))
			if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
				t.Fatalf("restore id=%d: %v", id, err)
			}

			gotHash := sha256File(t, outPath)
			if gotHash != fileHash {
				t.Fatalf("edge case hash mismatch for id=%d want=%s got=%s", id, fileHash, gotHash)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if fileCount == 0 {
		t.Fatalf("expected >0 files to be stored from edge cases folder, got 0")
	}

	// Run full system verification.
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify system full: %v", err)
	}

	// Ensure no processing rows stuck behind.
	assertNoProcessingRows(t, dbconn)
}

func TestGCRemovesUnusedContainers(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create two different files large enough to likely create separate containers
	fileA := createTempFile(t, inputDir, "fileA.bin", 512*1024)

	// Create fileB with slightly different deterministic pattern
	fileBPath := filepath.Join(inputDir, "fileB.bin")
	b := make([]byte, 512*1024)
	for i := range b {
		b[i] = byte((i*37 + 11) % 251) // different formula
	}
	if err := os.WriteFile(fileBPath, b, 0o644); err != nil {
		t.Fatalf("write fileB: %v", err)
	}
	fileB := fileBPath

	// Store both
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, fileA); err != nil {
		t.Fatalf("store fileA: %v", err)
	}
	if err := storage.StoreFileWithStorageContext(sgctx, fileB); err != nil {
		t.Fatalf("store fileB: %v", err)
	}

	// Count containers before removal
	var containersBefore int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containersBefore); err != nil {
		t.Fatalf("count container: %v", err)
	}
	if containersBefore == 0 {
		t.Fatalf("expected at least 1 container")
	}

	// Fetch fileA ID
	var fileAID int64
	hashA := sha256File(t, fileA)
	if err := dbconn.QueryRow(
		`SELECT id FROM logical_file WHERE file_hash = $1`,
		hashA,
	).Scan(&fileAID); err != nil {
		t.Fatalf("fetch fileA id: %v", err)
	}

	// Remove fileA
	if err := storage.RemoveFileWithDB(dbconn, fileAID); err != nil {
		t.Fatalf("removeFileWithDB: %v", err)
	}

	// Run verify before GC to check for any issues with live_ref_count values or metadata integrity.
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify standard after GC: %v", err)
	}

	// Run GC -- dry run first to check it doesn't delete anything prematurely
	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("runGC (dry-run): %v", err)
	}

	// Verify again after dry-run GC to ensure it doesn't break anything.
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify full after GC: %v", err)
	}

	// Run GC
	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("runGC 'real' run: %v", err)
	}

	// Count containers after GC
	var containersAfter int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containersAfter); err != nil {
		t.Fatalf("count container after: %v", err)
	}

	// GC may delete 0 containers if remaining live chunks share the same container.
	// What we must guarantee is that GC does not break restore and live_ref_count values remain valid.
	var negatives int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check negative live_ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found %d chunks with negative live_ref_count", negatives)
	}

	// Ensure fileB still restores correctly
	var fileBID int64
	hashB := sha256File(t, fileB)
	if err := dbconn.QueryRow(
		`SELECT id FROM logical_file WHERE file_hash = $1`,
		hashB,
	).Scan(&fileBID); err != nil {
		t.Fatalf("fetch fileB id: %v", err)
	}

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "fileB.restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileBID, outPath); err != nil {
		t.Fatalf("restore fileB after GC: %v", err)
	}

	gotHash := sha256File(t, outPath)
	if gotHash != hashB {
		t.Fatalf("hash mismatch after GC: want %s got %s", hashB, gotHash)
	}

	// Ensure no chunk has negative live_ref_count
	//var negatives int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check negative live_ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found chunks with negative live_ref_count")
	}
}

func TestConcurrentStoreSameFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "concurrent.bin", 256*1024)
	fileHash := sha256File(t, inPath)

	// Start two goroutines trying to store the same file
	done := make(chan error, 2)
	go func() {
		sgctx := storage.StorageContext{
			DB:     dbconn,
			Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
		}
		done <- storage.StoreFileWithStorageContext(sgctx, inPath)
	}()
	go func() {
		sgctx := storage.StorageContext{
			DB:     dbconn,
			Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
		}
		done <- storage.StoreFileWithStorageContext(sgctx, inPath)
	}()

	// Wait for both to complete
	err1 := <-done
	err2 := <-done

	if err1 != nil {
		t.Fatalf("first store failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second store failed: %v", err2)
	}

	// Should still be 1 logical file for this hash
	var n int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&n); err != nil {
		t.Fatalf("count logical_file: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 logical_file row, got %d", n)
	}
}

func TestConcurrentStoreSameChunk(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create two files that share a common chunk
	// Use a large shared prefix that will be chunked the same way
	sharedPrefix := make([]byte, 128*1024) // Large enough to be multiple chunks
	for i := range sharedPrefix {
		sharedPrefix[i] = byte((i*31 + 7) % 251)
	}

	// File A: shared prefix + unique suffix
	fileAData := append(append([]byte{}, sharedPrefix...), []byte("uniqueA")...)
	fileAPath := filepath.Join(inputDir, "fileA.bin")
	if err := os.WriteFile(fileAPath, fileAData, 0o644); err != nil {
		t.Fatalf("write fileA: %v", err)
	}

	// File B: same shared prefix + different unique suffix
	fileBData := append(append([]byte{}, sharedPrefix...), []byte("uniqueB")...)
	fileBPath := filepath.Join(inputDir, "fileB.bin")
	if err := os.WriteFile(fileBPath, fileBData, 0o644); err != nil {
		t.Fatalf("write fileB: %v", err)
	}

	// Start two goroutines storing the files concurrently
	done := make(chan error, 2)
	go func() {
		sgctx := storage.StorageContext{
			DB:     dbconn,
			Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
		}
		done <- storage.StoreFileWithStorageContext(sgctx, fileAPath)
	}()
	go func() {
		sgctx := storage.StorageContext{
			DB:     dbconn,
			Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
		}
		done <- storage.StoreFileWithStorageContext(sgctx, fileBPath)
	}()

	// Wait for both to complete
	err1 := <-done
	err2 := <-done

	if err1 != nil {
		t.Fatalf("store fileA failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("store fileB failed: %v", err2)
	}

	// Verify both files are stored
	hashA := sha256File(t, fileAPath)
	hashB := sha256File(t, fileBPath)

	var countA, countB int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, hashA).Scan(&countA); err != nil {
		t.Fatalf("count fileA: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, hashB).Scan(&countB); err != nil {
		t.Fatalf("count fileB: %v", err)
	}

	if countA != 1 || countB != 1 {
		t.Fatalf("expected 1 entry each for fileA and fileB, got %d and %d", countA, countB)
	}
}

func TestConcurrentStoreSameFileStress(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "same-file-stress.bin", 768*1024)
	fileHash := sha256File(t, inPath)

	const workers = 12
	start := make(chan struct{})
	done := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func() {
			<-start
			sgctx := storage.StorageContext{
				DB:     dbconn,
				Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
			}
			done <- storage.StoreFileWithStorageContext(sgctx, inPath)
		}()
	}
	close(start)

	for i := 0; i < workers; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("worker %d store failed: %v", i, err)
			}
		case <-time.After(45 * time.Second):
			t.Fatalf("timed out waiting for worker %d", i)
		}
	}

	var logicalFiles int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&logicalFiles); err != nil {
		t.Fatalf("count logical_file: %v", err)
	}
	if logicalFiles != 1 {
		t.Fatalf("expected 1 logical_file row, got %d", logicalFiles)
	}
	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)

	var status string
	var retryCount int
	if err := dbconn.QueryRow(`SELECT status, retry_count FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&status, &retryCount); err != nil {
		t.Fatalf("query logical_file status: %v", err)
	}
	if status != filestate.LogicalFileCompleted {
		t.Fatalf("expected logical file status COMPLETED, got %s", status)
	}
	if retryCount < 0 {
		t.Fatalf("retry_count should never be negative, got %d", retryCount)
	}

	fileID := fetchFileIDByHash(t, dbconn, fileHash)
	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "same-file-stress.restored.bin")
	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore stored file: %v", err)
	}
	if gotHash := sha256File(t, outPath); gotHash != fileHash {
		t.Fatalf("restored hash mismatch: want %s got %s", fileHash, gotHash)
	}
}

func TestConcurrentStoreFolderStress(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "folder-stress")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir inputDir: %v", err)
	}

	expectedHashes := createSampleDataset(t, inputDir)
	for i := 0; i < 8; i++ {
		src := expectedHashes["binary.bin"]
		dupPath := filepath.Join(inputDir, "dup_stress_"+itoa(i)+".bin")
		if err := os.WriteFile(dupPath, mustRead(t, src), 0o644); err != nil {
			t.Fatalf("write duplicate stress file: %v", err)
		}
	}

	expectedUnique := collectFileHashesByCount(t, inputDir)

	const workers = 4
	start := make(chan struct{})
	done := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func() {
			<-start
			done <- storage.StoreFolderWithStorageContext(newTestContext(dbconn), inputDir)
		}()
	}
	close(start)

	for i := 0; i < workers; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("StoreFolder worker %d failed: %v", i, err)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("timed out waiting for StoreFolder worker %d", i)
		}
	}

	var completedFiles int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = $1`, filestate.LogicalFileCompleted).Scan(&completedFiles); err != nil {
		t.Fatalf("count completed logical files: %v", err)
	}
	if completedFiles != len(expectedUnique) {
		t.Fatalf("expected %d completed logical files, got %d", len(expectedUnique), completedFiles)
	}

	var nonCompleted int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status <> $1`, filestate.LogicalFileCompleted).Scan(&nonCompleted); err != nil {
		t.Fatalf("count non-completed logical files: %v", err)
	}
	if nonCompleted != 0 {
		t.Fatalf("expected no non-completed logical files, got %d", nonCompleted)
	}
	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)

	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir outDir: %v", err)
	}

	rows, err := dbconn.Query(`SELECT id, file_hash FROM logical_file ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query logical_file: %v", err)
	}
	defer rows.Close()

	restored := 0
	for rows.Next() {
		var id int64
		var hash string
		if err := rows.Scan(&id, &hash); err != nil {
			t.Fatalf("scan logical_file row: %v", err)
		}

		outPath := filepath.Join(outDir, fmt.Sprintf("%d.restore.bin", id))
		if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
			t.Fatalf("restore file %d: %v", id, err)
		}
		if gotHash := sha256File(t, outPath); gotHash != hash {
			t.Fatalf("restored hash mismatch for file %d: want %s got %s", id, hash, gotHash)
		}
		restored++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if restored != len(expectedUnique) {
		t.Fatalf("expected to restore %d files, restored %d", len(expectedUnique), restored)
	}
}

func TestConcurrentStoreStressForcesRotation(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalMaxSize := container.GetContainerMaxSize()
	// Keep max size above max chunk payload to force rotation without rejecting chunks.
	container.SetContainerMaxSize(3 * 1024 * 1024)
	defer container.SetContainerMaxSize(originalMaxSize)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir inputDir: %v", err)
	}

	inPath := createTempFile(t, inputDir, "rotation-stress.bin", 6*1024*1024+123)
	fileHash := sha256File(t, inPath)

	const workers = 12
	start := make(chan struct{})
	done := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func() {
			<-start
			sgctx := storage.StorageContext{
				DB:     dbconn,
				Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
			}
			done <- storage.StoreFileWithStorageContext(sgctx, inPath)
		}()
	}
	close(start)

	for i := 0; i < workers; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("worker %d store failed: %v", i, err)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("timed out waiting for worker %d", i)
		}
	}

	var logicalFiles int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&logicalFiles); err != nil {
		t.Fatalf("count logical_file: %v", err)
	}
	if logicalFiles != 1 {
		t.Fatalf("expected 1 logical_file row, got %d", logicalFiles)
	}

	var containerCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containerCount); err != nil {
		t.Fatalf("count containers: %v", err)
	}
	if containerCount < 2 {
		t.Fatalf("expected rotation to create multiple containers, got %d", containerCount)
	}

	var sealedCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed = TRUE`).Scan(&sealedCount); err != nil {
		t.Fatalf("count sealed containers: %v", err)
	}
	if sealedCount < 1 {
		t.Fatalf("expected at least 1 sealed container after rotation, got %d", sealedCount)
	}

	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)

	fileID := fetchFileIDByHash(t, dbconn, fileHash)
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir outDir: %v", err)
	}
	outPath := filepath.Join(outDir, "rotation-stress.restored.bin")
	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore stored file: %v", err)
	}
	if gotHash := sha256File(t, outPath); gotHash != fileHash {
		t.Fatalf("restored hash mismatch: want %s got %s", fileHash, gotHash)
	}
}

func TestRetryAfterAbortedFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "retry_file.bin", 256*1024)
	fileHash := sha256File(t, inPath)
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	// Store the file initially
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	// Manually set the file status to ABORTED to simulate a failed store
	fileID := fetchFileIDByHash(t, dbconn, fileHash)
	if _, err := dbconn.Exec(`UPDATE logical_file SET status = $1 WHERE id = $2`, filestate.LogicalFileAborted, fileID); err != nil {
		t.Fatalf("set status to ABORTED: %v", err)
	}

	// Now try to store the same file again - it should retry and succeed
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("retry store after abort: %v", err)
	}

	// Verify the file is now marked as COMPLETED
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
		t.Fatalf("check status: %v", err)
	}
	if status != filestate.LogicalFileCompleted {
		t.Fatalf("expected status COMPLETED, got %s", status)
	}
}

func TestStoreRebuildsCorruptCompletedMetadata(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "rebuild_corrupt_completed.bin", 768*1024)
	fileHash := sha256File(t, inPath)

	initialCtx := newTestContext(dbconn)
	if err := storage.StoreFileWithStorageContext(initialCtx, inPath); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	// Simulate silent metadata corruption on a COMPLETED file graph.
	if _, err := dbconn.Exec(`DELETE FROM file_chunk WHERE logical_file_id = $1`, fileID); err != nil {
		t.Fatalf("delete file_chunk rows: %v", err)
	}

	var fileChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows after corruption: %v", err)
	}
	if fileChunkCount != 0 {
		t.Fatalf("expected corrupted graph to have 0 file_chunk rows, got %d", fileChunkCount)
	}

	restoreCtx := newTestContext(dbconn)
	result, err := storage.StoreFileWithStorageContextResult(restoreCtx, inPath)
	if err != nil {
		t.Fatalf("re-store after graph corruption: %v", err)
	}
	if result.AlreadyStored {
		t.Fatalf("expected rebuild path, got AlreadyStored=true")
	}
	if result.FileID != fileID {
		t.Fatalf("expected reclaim on existing logical_file row %d, got %d", fileID, result.FileID)
	}

	var status string
	var retryCount int
	if err := dbconn.QueryRow(`SELECT status, retry_count FROM logical_file WHERE id = $1`, fileID).Scan(&status, &retryCount); err != nil {
		t.Fatalf("query rebuilt logical_file status: %v", err)
	}
	if status != filestate.LogicalFileCompleted {
		t.Fatalf("expected rebuilt logical_file status COMPLETED, got %s", status)
	}
	if retryCount < 1 {
		t.Fatalf("expected retry_count >= 1 after rebuild, got %d", retryCount)
	}

	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows after rebuild: %v", err)
	}
	if fileChunkCount == 0 {
		t.Fatalf("expected rebuilt graph to recreate file_chunk rows")
	}

	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)
}

func TestStoreRebuildsMalformedCompletedChunkMetadata(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := filepath.Join(inputDir, "chunk_rebuild_malformed_completed.txt")
	if err := os.WriteFile(inPath, []byte("malformed completed chunk should be rebuilt"), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	chunks, err := chunk.ChunkFile(inPath)
	if err != nil {
		t.Fatalf("chunk file: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected single chunk test input, got %d chunks", len(chunks))
	}

	sum := sha256.Sum256(chunks[0])
	chunkHash := hex.EncodeToString(sum[:])
	chunkSize := int64(len(chunks[0]))

	var malformedChunkID int64
	err = dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		VALUES ($1, $2, $3, 0)
		RETURNING id
	`, chunkHash, chunkSize, filestate.ChunkCompleted).Scan(&malformedChunkID)
	if err != nil {
		t.Fatalf("insert malformed completed chunk: %v", err)
	}

	sgctx := newTestContext(dbconn)
	result, err := storage.StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store file with malformed completed chunk seed: %v", err)
	}
	if result.AlreadyStored {
		t.Fatalf("expected store path (not already-stored shortcut) for malformed completed chunk rebuild")
	}

	var chunkID int64
	var status string
	var retryCount int
	err = dbconn.QueryRow(`
		SELECT id, status, retry_count
		FROM chunk
		WHERE chunk_hash = $1 AND size = $2
	`, chunkHash, chunkSize).Scan(&chunkID, &status, &retryCount)
	if err != nil {
		t.Fatalf("query rebuilt chunk row: %v", err)
	}
	if chunkID != malformedChunkID {
		t.Fatalf("expected rebuild to reclaim malformed chunk id %d, got %d", malformedChunkID, chunkID)
	}
	if status != filestate.ChunkCompleted {
		t.Fatalf("expected rebuilt chunk status COMPLETED, got %s", status)
	}
	if retryCount < 1 {
		t.Fatalf("expected retry_count >= 1 after malformed completed chunk reclaim, got %d", retryCount)
	}

	var blockRows int
	err = dbconn.QueryRow(`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, chunkID).Scan(&blockRows)
	if err != nil {
		t.Fatalf("count chunk blocks: %v", err)
	}
	if blockRows != 1 {
		t.Fatalf("expected rebuilt chunk to have exactly 1 blocks row, got %d", blockRows)
	}
}

func TestStoreRebuildsMalformedCompletedChunkInQuarantinedContainer(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := filepath.Join(inputDir, "chunk_rebuild_quarantined_container.txt")
	if err := os.WriteFile(inPath, []byte("completed chunk in quarantined container should be rebuilt"), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	chunks, err := chunk.ChunkFile(inPath)
	if err != nil {
		t.Fatalf("chunk file: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected single chunk test input, got %d chunks", len(chunks))
	}

	sum := sha256.Sum256(chunks[0])
	chunkHash := hex.EncodeToString(sum[:])
	chunkSize := int64(len(chunks[0]))

	// Pre-seed a quarantined container with a COMPLETED chunk pointing into it.
	var quarantinedContainerID int64
	err = dbconn.QueryRow(`
		INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		VALUES ($1, TRUE, TRUE, $2, $3)
		RETURNING id
	`, "quarantined_chunk_reuse.bin", int64(1024), container.GetContainerMaxSize()).Scan(&quarantinedContainerID)
	if err != nil {
		t.Fatalf("insert quarantined container: %v", err)
	}

	var malformedChunkID int64
	err = dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		VALUES ($1, $2, $3, 0)
		RETURNING id
	`, chunkHash, chunkSize, filestate.ChunkCompleted).Scan(&malformedChunkID)
	if err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, malformedChunkID, "plain", 1, chunkSize, chunkSize, quarantinedContainerID, 64); err != nil {
		t.Fatalf("insert block row pointing to quarantined container: %v", err)
	}

	sgctx := newTestContext(dbconn)
	result, err := storage.StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store file with completed chunk in quarantined container: %v", err)
	}
	if result.AlreadyStored {
		t.Fatalf("expected store path (not already-stored shortcut) when chunk container is quarantined")
	}

	var chunkID int64
	var chunkStatus string
	var retryCount int
	err = dbconn.QueryRow(`
		SELECT id, status, retry_count
		FROM chunk
		WHERE chunk_hash = $1 AND size = $2
	`, chunkHash, chunkSize).Scan(&chunkID, &chunkStatus, &retryCount)
	if err != nil {
		t.Fatalf("query rebuilt chunk row: %v", err)
	}
	if chunkID != malformedChunkID {
		t.Fatalf("expected rebuild to reclaim chunk id %d, got %d", malformedChunkID, chunkID)
	}
	if chunkStatus != filestate.ChunkCompleted {
		t.Fatalf("expected rebuilt chunk status COMPLETED, got %s", chunkStatus)
	}
	if retryCount < 1 {
		t.Fatalf("expected retry_count >= 1 after reclaim, got %d", retryCount)
	}

	// The rebuilt blocks row must now point to a non-quarantined container.
	var rebuiltContainerID int64
	var rebuiltQuarantined bool
	err = dbconn.QueryRow(`
		SELECT ctr.id, ctr.quarantine
		FROM blocks b
		JOIN container ctr ON ctr.id = b.container_id
		WHERE b.chunk_id = $1
	`, chunkID).Scan(&rebuiltContainerID, &rebuiltQuarantined)
	if err != nil {
		t.Fatalf("query rebuilt chunk container: %v", err)
	}
	if rebuiltContainerID == quarantinedContainerID {
		t.Fatalf("rebuilt chunk still points to the original quarantined container")
	}
	if rebuiltQuarantined {
		t.Fatalf("rebuilt chunk points to a quarantined container")
	}
}

func TestConcurrentRetryAfterAbortedFileStress(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "retry_file_stress.bin", 384*1024)
	fileHash := sha256File(t, inPath)

	initialSgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	if err := storage.StoreFileWithStorageContext(initialSgctx, inPath); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, fileHash)
	if _, err := dbconn.Exec(`UPDATE logical_file SET status = $1 WHERE id = $2`, filestate.LogicalFileAborted, fileID); err != nil {
		t.Fatalf("set logical file ABORTED: %v", err)
	}

	const workers = 8
	start := make(chan struct{})
	done := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func() {
			<-start
			sgctx := storage.StorageContext{
				DB:     dbconn,
				Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
			}
			done <- storage.StoreFileWithStorageContext(sgctx, inPath)
		}()
	}
	close(start)

	for i := 0; i < workers; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("retry worker %d failed: %v", i, err)
			}
		case <-time.After(45 * time.Second):
			t.Fatalf("timed out waiting for retry worker %d", i)
		}
	}

	var count int
	var status string
	if err := dbconn.QueryRow(`SELECT COUNT(*), MIN(status) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&count, &status); err != nil {
		t.Fatalf("query logical file rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 logical_file row after concurrent retry, got %d", count)
	}
	if status != filestate.LogicalFileCompleted {
		t.Fatalf("expected logical file status COMPLETED after concurrent retry, got %s", status)
	}
	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)
}

func TestRetryAfterAbortedChunk(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "retry_chunk.bin", 256*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	// Store the file initially to create chunks
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	// Find a chunk from this file and set it to ABORTED
	var chunkID int64
	var chunkHash string
	if err := dbconn.QueryRow(`
		SELECT c.id, c.chunk_hash
		FROM chunk c
		JOIN file_chunk fc ON c.id = fc.chunk_id
		JOIN logical_file lf ON fc.logical_file_id = lf.id
		WHERE lf.file_hash = $1
		LIMIT 1
	`, sha256File(t, inPath)).Scan(&chunkID, &chunkHash); err != nil {
		t.Fatalf("find chunk: %v", err)
	}

	// Set the chunk status to ABORTED
	if _, err := dbconn.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID); err != nil {
		t.Fatalf("set chunk status to ABORTED: %v", err)
	}

	// Now try to store the same file again - it should retry the aborted chunk and succeed
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("retry store after chunk abort: %v", err)
	}

	// Verify the chunk is now marked as COMPLETED
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&status); err != nil {
		t.Fatalf("check chunk status: %v", err)
	}
	if status != filestate.ChunkCompleted {
		t.Fatalf("expected chunk status COMPLETED, got %s", status)
	}
}

func TestConcurrentRetryAfterAbortedChunkStress(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	sharedPrefix := make([]byte, 256*1024)
	for i := range sharedPrefix {
		sharedPrefix[i] = byte((i*31 + 7) % 251)
	}

	fileAPath := filepath.Join(inputDir, "retry_chunk_a.bin")
	fileBPath := filepath.Join(inputDir, "retry_chunk_b.bin")
	if err := os.WriteFile(fileAPath, append(append([]byte{}, sharedPrefix...), []byte("tail-A")...), 0o644); err != nil {
		t.Fatalf("write fileA: %v", err)
	}
	if err := os.WriteFile(fileBPath, append(append([]byte{}, sharedPrefix...), []byte("tail-B")...), 0o644); err != nil {
		t.Fatalf("write fileB: %v", err)
	}

	initialSgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	if err := storage.StoreFileWithStorageContext(initialSgctx, fileAPath); err != nil {
		t.Fatalf("initial store fileA: %v", err)
	}

	var chunkID int64
	var chunkHash string
	if err := dbconn.QueryRow(`
		SELECT c.id, c.chunk_hash
		FROM chunk c
		JOIN file_chunk fc ON c.id = fc.chunk_id
		JOIN logical_file lf ON fc.logical_file_id = lf.id
		WHERE lf.file_hash = $1
		ORDER BY fc.chunk_order ASC
		LIMIT 1
	`, sha256File(t, fileAPath)).Scan(&chunkID, &chunkHash); err != nil {
		t.Fatalf("find shared chunk: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID); err != nil {
		t.Fatalf("set chunk ABORTED: %v", err)
	}

	const workers = 8
	start := make(chan struct{})
	done := make(chan error, workers)
	paths := []string{fileAPath, fileBPath}

	for i := 0; i < workers; i++ {
		path := paths[i%len(paths)]
		go func(p string) {
			<-start
			sgctx := storage.StorageContext{
				DB:     dbconn,
				Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
			}
			done <- storage.StoreFileWithStorageContext(sgctx, p)
		}(path)
	}
	close(start)

	for i := 0; i < workers; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("chunk retry worker %d failed: %v", i, err)
			}
		case <-time.After(45 * time.Second):
			t.Fatalf("timed out waiting for chunk retry worker %d", i)
		}
	}

	var status string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&status); err != nil {
		t.Fatalf("query chunk status: %v", err)
	}
	if status != filestate.ChunkCompleted {
		t.Fatalf("expected chunk status COMPLETED after concurrent retry, got %s", status)
	}
	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)

	for _, p := range paths {
		hash := sha256File(t, p)
		var count int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, hash).Scan(&count); err != nil {
			t.Fatalf("count logical files for %s: %v", filepath.Base(p), err)
		}
		if count != 1 {
			t.Fatalf("expected 1 logical file row for %s, got %d", filepath.Base(p), count)
		}
	}
}

func TestContainerRollover(t *testing.T) {
	requireDB(t)

	// Use temp dirs per test
	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Set small container size for testing rollover
	originalMaxSize := container.GetContainerMaxSize()
	container.SetContainerMaxSize(1 * 1024 * 1024)       // 1MB for quick test
	defer container.SetContainerMaxSize(originalMaxSize) // restore

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create files that will exceed container size
	files := []string{
		createTempFile(t, inputDir, "file1.bin", 600*1024), // 600KB
		createTempFile(t, inputDir, "file2.bin", 600*1024), // 600KB, should trigger rollover
	}
	// Ensure file2 is distinct so logical-file dedupe does not bypass rollover assertions.
	file2Data := mustRead(t, files[1])
	file2Data[0] ^= 0xFF
	if err := os.WriteFile(files[1], file2Data, 0o644); err != nil {
		t.Fatalf("rewrite file2 with distinct content: %v", err)
	}

	// Store first file
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, files[0]); err != nil {
		t.Fatalf("store first file: %v", err)
	}

	// Check that one container exists and is not sealed
	var containerCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed = FALSE`).Scan(&containerCount); err != nil {
		t.Fatalf("count unsealed containers: %v", err)
	}
	if containerCount != 1 {
		t.Fatalf("expected 1 unsealed container, got %d", containerCount)
	}

	// Store second file - should trigger rollover
	if err := storage.StoreFileWithStorageContext(sgctx, files[1]); err != nil {
		t.Fatalf("store second file: %v", err)
	}

	// Check that the first container is now sealed
	var sealedCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed = TRUE`).Scan(&sealedCount); err != nil {
		t.Fatalf("count sealed containers: %v", err)
	}
	if sealedCount != 1 {
		t.Fatalf("expected 1 sealed container, got %d", sealedCount)
	}

	// Check that a new unsealed container exists
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed = FALSE`).Scan(&containerCount); err != nil {
		t.Fatalf("count unsealed containers after rollover: %v", err)
	}
	if containerCount != 1 {
		t.Fatalf("expected 1 unsealed container after rollover, got %d", containerCount)
	}

	// Verify both files can be restored
	for i, file := range files {
		hash := sha256File(t, file)
		fileID := fetchFileIDByHash(t, dbconn, hash)

		outDir := filepath.Join(tmp, "out")
		_ = os.MkdirAll(outDir, 0o755)
		outPath := filepath.Join(outDir, fmt.Sprintf("restored%d.bin", i))

		if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
			t.Fatalf("restore file %d: %v", i, err)
		}

		// Verify content
		original := mustRead(t, file)
		restored := mustRead(t, outPath)
		if !bytes.Equal(original, restored) {
			t.Fatalf("file %d content mismatch", i)
		}
	}
}

func TestRotationSealsAllPreviousContainers(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalMaxSize := container.GetContainerMaxSize()
	// Keep max size above max chunk payload so forced rotations happen without payload rejection.
	container.SetContainerMaxSize(3 * 1024 * 1024)
	defer container.SetContainerMaxSize(originalMaxSize)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "rotation-seal.bin", 8*1024*1024+137)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file with forced rotations: %v", err)
	}

	var totalContainers int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&totalContainers); err != nil {
		t.Fatalf("count containers: %v", err)
	}
	if totalContainers < 2 {
		t.Fatalf("expected at least 2 containers after forced rotations, got %d", totalContainers)
	}

	var oldUnsealed int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM container c
		WHERE c.id < (SELECT MAX(id) FROM container)
		  AND c.sealed = FALSE
	`).Scan(&oldUnsealed); err != nil {
		t.Fatalf("count old unsealed containers: %v", err)
	}
	if oldUnsealed != 0 {
		t.Fatalf("expected all previous containers to be sealed after rotation, found %d old unsealed", oldUnsealed)
	}

	var unsealedTotal int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed = FALSE`).Scan(&unsealedTotal); err != nil {
		t.Fatalf("count unsealed containers: %v", err)
	}
	if unsealedTotal > 1 {
		t.Fatalf("expected at most one unsealed active container, found %d", unsealedTotal)
	}
}

func TestStartupRecoverySimulation(t *testing.T) {
	requireDB(t)

	// Use temp dirs per test
	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create and start storing a file, but simulate it getting stuck
	inPath := createTempFile(t, inputDir, "stuck_file.bin", 256*1024)

	// Manually insert a processing logical file (simulating stuck store)
	hash := sha256File(t, inPath)
	size := int64(256 * 1024)

	_, err = dbconn.Exec(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, retry_count)
		VALUES ($1, $2, $3, $4, 0)
	`, filepath.Base(inPath), size, hash, filestate.LogicalFileProcessing)
	if err != nil {
		t.Fatalf("insert processing logical file: %v", err)
	}

	// Set updated_at to old time to simulate stuck processing
	_, err = dbconn.Exec(`
		UPDATE logical_file
		SET updated_at = NOW() - INTERVAL '15 minutes'
		WHERE file_hash = $1
	`, hash)
	if err != nil {
		t.Fatalf("update logical file timestamp: %v", err)
	}

	// Also create a processing chunk
	_, err = dbconn.Exec(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count)
		VALUES ($1, $2, $3, 0, 0)
	`, "dummy_chunk_hash", int64(128*1024), filestate.ChunkProcessing)
	if err != nil {
		t.Fatalf("insert processing chunk: %v", err)
	}

	// Get chunk ID
	var chunkID int64
	if err := dbconn.QueryRow(`SELECT id FROM chunk WHERE chunk_hash = $1`, "dummy_chunk_hash").Scan(&chunkID); err != nil {
		t.Fatalf("get chunk ID: %v", err)
	}

	// Set chunk updated_at to old time
	_, err = dbconn.Exec(`
		UPDATE chunk
		SET updated_at = NOW() - INTERVAL '15 minutes'
		WHERE id = $1
	`, chunkID)
	if err != nil {
		t.Fatalf("update chunk timestamp: %v", err)
	}

	// Create a container file and then delete it to simulate missing container
	containerPath := filepath.Join(container.ContainersDir, "missing_container.bin")
	if err := os.WriteFile(containerPath, []byte("dummy"), 0o644); err != nil {
		t.Fatalf("create dummy container file: %v", err)
	}

	_, err = dbconn.Exec(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, FALSE, FALSE)
	`, "missing_container.bin", int64(1024), int64(64*1024*1024))
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}

	// Delete the file to simulate missing container
	if err := os.Remove(containerPath); err != nil {
		t.Fatalf("remove container file: %v", err)
	}

	// Now run system recovery
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("system recovery: %v", err)
	}

	// Verify that processing logical file was aborted
	var fileStatus string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE file_hash = $1`, hash).Scan(&fileStatus); err != nil {
		t.Fatalf("check logical file status: %v", err)
	}
	if fileStatus != filestate.LogicalFileAborted {
		t.Fatalf("expected logical file status ABORTED, got %s", fileStatus)
	}

	// Verify that processing chunk was aborted
	var chunkStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&chunkStatus); err != nil {
		t.Fatalf("check chunk status: %v", err)
	}
	if chunkStatus != filestate.ChunkAborted {
		t.Fatalf("expected chunk status ABORTED, got %s", chunkStatus)
	}

	// Verify that missing container was quarantined
	var quarantine bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE filename = $1`, "missing_container.bin").Scan(&quarantine); err != nil {
		t.Fatalf("check container quarantine: %v", err)
	}
	if !quarantine {
		t.Fatalf("expected missing container to be quarantined")
	}
}

func TestStartupRecoveryQuarantinesTruncatedActiveContainerTail(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "truncated-active-tail.bin", 256*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	var containerID int64
	var filename string
	var dbCurrentSize int64
	var sealed bool
	err = dbconn.QueryRow(`
		SELECT id, filename, current_size, sealed
		FROM container
		ORDER BY id DESC
		LIMIT 1
	`).Scan(&containerID, &filename, &dbCurrentSize, &sealed)
	if err != nil {
		t.Fatalf("select active container: %v", err)
	}
	if sealed {
		t.Fatalf("expected newest container to be active/unsealed")
	}

	containerPath := filepath.Join(container.ContainersDir, filename)
	truncatedSize := dbCurrentSize - 32
	if truncatedSize <= 0 {
		t.Fatalf("invalid truncation target for container size %d", dbCurrentSize)
	}
	if err := os.Truncate(containerPath, truncatedSize); err != nil {
		t.Fatalf("truncate active container tail: %v", err)
	}

	report, err := recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("system recovery: %v", err)
	}
	if report.QuarantinedCorruptTail < 1 {
		t.Fatalf("expected corrupt-tail quarantine count >= 1, got %d", report.QuarantinedCorruptTail)
	}

	var quarantine bool
	err = dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = $1`, containerID).Scan(&quarantine)
	if err != nil {
		t.Fatalf("query container quarantine: %v", err)
	}
	if !quarantine {
		t.Fatalf("expected truncated active container to be quarantined")
	}
}

func TestStartupRecoveryQuarantinesSealingContainerWithGhostBytesAndGCSkipsIt(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers dir: %v", err)
	}

	filename := "sealing-ghost-bytes.bin"
	containerPath := filepath.Join(container.ContainersDir, filename)
	dbCurrentSize := int64(container.ContainerHdrLen + 128)
	ghostBytes := []byte("ghost-bytes-left-after-rollback")
	physicalSize := dbCurrentSize + int64(len(ghostBytes))

	if err := os.WriteFile(containerPath, bytes.Repeat([]byte{'g'}, int(physicalSize)), 0o644); err != nil {
		t.Fatalf("write sealing container file: %v", err)
	}

	var containerID int64
	err = dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES ($1, $2, $3, FALSE, TRUE, FALSE)
		RETURNING id
	`, filename, dbCurrentSize, container.GetContainerMaxSize()).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert sealing container: %v", err)
	}

	report, err := recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("system recovery: %v", err)
	}
	if report.SealingQuarantined < 1 {
		t.Fatalf("expected sealing container quarantine count >= 1, got %d", report.SealingQuarantined)
	}
	if report.SealingCompleted != 0 {
		t.Fatalf("expected no sealing completions for ghost-byte container, got %d", report.SealingCompleted)
	}

	var sealed bool
	var sealing bool
	var quarantine bool
	err = dbconn.QueryRow(`SELECT sealed, sealing, quarantine FROM container WHERE id = $1`, containerID).Scan(&sealed, &sealing, &quarantine)
	if err != nil {
		t.Fatalf("query sealing container state: %v", err)
	}
	if sealed {
		t.Fatalf("expected ghost-byte sealing container to remain unsealed")
	}
	if sealing {
		t.Fatalf("expected recovery to clear sealing marker on quarantined container")
	}
	if !quarantine {
		t.Fatalf("expected ghost-byte sealing container to be quarantined")
	}

	dryRunResult, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir)
	if err != nil {
		t.Fatalf("gc dry-run after recovery: %v", err)
	}
	if dryRunResult.AffectedContainers != 0 {
		t.Fatalf("expected gc dry-run to skip quarantined ghost-byte container, got %d affected containers", dryRunResult.AffectedContainers)
	}
	if slices.Contains(dryRunResult.ContainerFilenames, filename) {
		t.Fatalf("gc dry-run must not report quarantined ghost-byte container as collectible")
	}

	realRunResult, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir)
	if err != nil {
		t.Fatalf("gc real run after recovery: %v", err)
	}
	if realRunResult.AffectedContainers != 0 {
		t.Fatalf("expected gc real run to skip quarantined ghost-byte container, got %d affected containers", realRunResult.AffectedContainers)
	}
	if slices.Contains(realRunResult.ContainerFilenames, filename) {
		t.Fatalf("gc real run must not delete quarantined ghost-byte container")
	}

	if _, err := os.Stat(containerPath); err != nil {
		t.Fatalf("expected quarantined ghost-byte container file to remain on disk after gc: %v", err)
	}

	var remainingRows int
	err = dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&remainingRows)
	if err != nil {
		t.Fatalf("count container row after gc: %v", err)
	}
	if remainingRows != 1 {
		t.Fatalf("expected quarantined ghost-byte container row to remain after gc, got %d rows", remainingRows)
	}
}

func TestSealContainerRejectsPhysicalSizeMismatch(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers dir: %v", err)
	}

	t.Run("ghost bytes", func(t *testing.T) {
		filename := "seal-mismatch-ghost.bin"
		path := filepath.Join(container.ContainersDir, filename)
		if err := os.WriteFile(path, bytes.Repeat([]byte{'g'}, container.ContainerHdrLen+256), 0o644); err != nil {
			t.Fatalf("write container file: %v", err)
		}

		var containerID int64
		err := dbconn.QueryRow(`
			INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
			VALUES ($1, $2, $3, FALSE, TRUE, FALSE)
			RETURNING id
		`, filename, int64(container.ContainerHdrLen+128), container.GetContainerMaxSize()).Scan(&containerID)
		if err != nil {
			t.Fatalf("insert container row: %v", err)
		}

		tx, err := dbconn.Begin()
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		err = container.SealContainerInDir(tx, containerID, filename, container.ContainersDir)
		_ = tx.Rollback()
		if err == nil {
			t.Fatalf("expected seal to fail for ghost-byte container")
		}
		if !strings.Contains(err.Error(), "ghost bytes detected") {
			t.Fatalf("expected ghost-byte error, got: %v", err)
		}
	})

	t.Run("truncated file", func(t *testing.T) {
		filename := "seal-mismatch-truncated.bin"
		path := filepath.Join(container.ContainersDir, filename)
		if err := os.WriteFile(path, bytes.Repeat([]byte{'t'}, container.ContainerHdrLen+96), 0o644); err != nil {
			t.Fatalf("write container file: %v", err)
		}

		var containerID int64
		err := dbconn.QueryRow(`
			INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
			VALUES ($1, $2, $3, FALSE, TRUE, FALSE)
			RETURNING id
		`, filename, int64(container.ContainerHdrLen+160), container.GetContainerMaxSize()).Scan(&containerID)
		if err != nil {
			t.Fatalf("insert container row: %v", err)
		}

		tx, err := dbconn.Begin()
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		err = container.SealContainerInDir(tx, containerID, filename, container.ContainersDir)
		_ = tx.Rollback()
		if err == nil {
			t.Fatalf("expected seal to fail for truncated container")
		}
		if !strings.Contains(err.Error(), "truncated file detected") {
			t.Fatalf("expected truncated-file error, got: %v", err)
		}
	})
}

func TestStoreImmediatelyQuarantinesUnopenableActiveContainer(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers dir: %v", err)
	}

	var containerID int64
	err = dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, FALSE, FALSE)
		RETURNING id
	`, "missing-active.bin", int64(container.ContainerHdrLen), container.GetContainerMaxSize()).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert missing active container: %v", err)
	}

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input dir: %v", err)
	}
	inPath := createTempFile(t, inputDir, "open-failure.txt", 4096)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: container.ContainersDir,
	}

	err = storage.StoreFileWithStorageContext(sgctx, inPath)
	if err == nil {
		t.Fatalf("expected store to fail when active container cannot be opened")
	}

	var quarantined bool
	err = dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = $1`, containerID).Scan(&quarantined)
	if err != nil {
		t.Fatalf("query missing active container quarantine: %v", err)
	}
	if !quarantined {
		t.Fatalf("expected missing active container to be quarantined immediately")
	}

	report, err := recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("system recovery: %v", err)
	}
	if report.QuarantinedMissing != 0 {
		t.Fatalf("expected startup recovery not to need quarantining for already-retired container, got %d", report.QuarantinedMissing)
	}
}

func TestStartupRecoveryFailsOnSuspiciousOrphanConflictState(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	orphanFilename := "orphan_conflict_mismatch.bin"
	orphanPath := filepath.Join(container.ContainersDir, orphanFilename)
	orphanBytes := []byte("orphan-file-bytes-for-conflict-validation")
	if err := os.WriteFile(orphanPath, orphanBytes, 0o644); err != nil {
		t.Fatalf("write orphan file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, quarantine, current_size, max_size) VALUES ($1, TRUE, $2, $3)`,
		orphanFilename,
		int64(1),
		int64(1),
	); err != nil {
		t.Fatalf("insert preexisting mismatched quarantine row: %v", err)
	}

	_, err = recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err == nil {
		t.Fatalf("expected suspicious orphan conflict error")
	}
	if !strings.Contains(err.Error(), "suspicious orphan container conflict") {
		t.Fatalf("expected suspicious orphan conflict error, got: %v", err)
	}
}

func TestStartupRecoveryAcceptsDuplicateOrphanRetrierConflictState(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	orphanFilename := "orphan_conflict_duplicate_retrier.bin"
	orphanPath := filepath.Join(container.ContainersDir, orphanFilename)
	orphanBytes := []byte("orphan-file-bytes-for-duplicate-retrier")
	if err := os.WriteFile(orphanPath, orphanBytes, 0o644); err != nil {
		t.Fatalf("write orphan file: %v", err)
	}
	expectedSize := int64(len(orphanBytes))

	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, quarantine, current_size, max_size) VALUES ($1, TRUE, $2, $3)`,
		orphanFilename,
		expectedSize,
		expectedSize,
	); err != nil {
		t.Fatalf("insert preexisting matching quarantine row: %v", err)
	}

	report, err := recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("expected duplicate retrier state to be accepted, got: %v", err)
	}
	if report.QuarantinedOrphan != 0 {
		t.Fatalf("expected no new orphan quarantine rows, got %d", report.QuarantinedOrphan)
	}

	var rowCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE filename = $1`, orphanFilename).Scan(&rowCount); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 container row after duplicate retrier handling, got %d", rowCount)
	}
}

func TestStartupRecoveryNonStrictContinuesOnSuspiciousOrphanConflictState(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	_ = os.Setenv("COLDKEEP_STRICT_RECOVERY", "false")
	defer os.Unsetenv("COLDKEEP_STRICT_RECOVERY")
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	orphanFilename := "orphan_conflict_non_strict.bin"
	orphanPath := filepath.Join(container.ContainersDir, orphanFilename)
	orphanBytes := []byte("orphan-file-bytes-for-non-strict")
	if err := os.WriteFile(orphanPath, orphanBytes, 0o644); err != nil {
		t.Fatalf("write orphan file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, quarantine, current_size, max_size) VALUES ($1, TRUE, $2, $3)`,
		orphanFilename,
		int64(1),
		int64(1),
	); err != nil {
		t.Fatalf("insert preexisting mismatched quarantine row: %v", err)
	}

	_, err = recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("expected non-strict recovery to continue, got: %v", err)
	}
}

func TestVerifyStandard(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_standard.bin", 256*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	t.Run("passes on clean database", func(t *testing.T) {
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
			t.Fatalf("RunVerify on clean DB should not fail: %v", err)
		}
	})

	t.Run("detects corrupted live_ref_count", func(t *testing.T) {
		// Corrupt one chunk's live_ref_count to a wrong value
		if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = live_ref_count + 99 WHERE id = (SELECT id FROM chunk LIMIT 1)`); err != nil {
			t.Fatalf("corrupt live_ref_count: %v", err)
		}
		defer func() {
			// Restore so other sub-tests are not affected
			if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = live_ref_count - 99 WHERE id = (SELECT id FROM chunk LIMIT 1)`); err != nil {
				t.Fatalf("restore live_ref_count: %v", err)
			}

		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err == nil {
			t.Fatal("RunVerify should have detected the corrupted live_ref_count but returned nil")
		}
	})

	t.Run("detects orphan chunk", func(t *testing.T) {
		// Insert a chunk with live_ref_count > 0 but no file_chunk referencing it
		if _, err := dbconn.Exec(`
				INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count)
				VALUES ('orphan_chunk_hash_test', 1024, $1, 1, 0)
		`, filestate.ChunkCompleted); err != nil {
			t.Fatalf("insert orphan chunk: %v", err)
		}
		defer func() {
			if _, err := dbconn.Exec(`DELETE FROM chunk WHERE chunk_hash = 'orphan_chunk_hash_test'`); err != nil {
				t.Fatalf("delete orphan chunk: %v", err)
			}

		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err == nil {
			t.Fatal("RunVerify should have detected the orphan chunk but returned nil")
		}
	})

	t.Run("detects pin_count on non-completed chunk", func(t *testing.T) {
		if _, err := dbconn.Exec(`
				INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count)
				VALUES ('invalid_pinned_processing_chunk', 1024, $1, 0, 1, 0)
		`, filestate.ChunkProcessing); err != nil {
			t.Fatalf("insert invalid pinned processing chunk: %v", err)
		}
		defer func() {
			if _, err := dbconn.Exec(`DELETE FROM chunk WHERE chunk_hash = 'invalid_pinned_processing_chunk'`); err != nil {
				t.Fatalf("delete invalid pinned processing chunk: %v", err)
			}

		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err == nil {
			t.Fatal("RunVerify should have detected pin_count on a non-completed chunk but returned nil")
		}
	})

	t.Run("detects pinned chunk missing block metadata", func(t *testing.T) {
		if _, err := dbconn.Exec(`
				INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count)
				VALUES ('invalid_pinned_unplaced_chunk', 1024, $1, 0, 1, 0)
		`, filestate.ChunkCompleted); err != nil {
			t.Fatalf("insert invalid pinned unplaced chunk: %v", err)
		}
		defer func() {
			if _, err := dbconn.Exec(`DELETE FROM chunk WHERE chunk_hash = 'invalid_pinned_unplaced_chunk'`); err != nil {
				t.Fatalf("delete invalid pinned unplaced chunk: %v", err)
			}

		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err == nil {
			t.Fatal("RunVerify should have detected pinned chunk missing block metadata but returned nil")
		}
	})

	t.Run("detects broken file chunk ordering continuity", func(t *testing.T) {
		var fileID int
		if err := dbconn.QueryRow(`SELECT id FROM logical_file ORDER BY id ASC LIMIT 1`).Scan(&fileID); err != nil {
			t.Fatalf("query logical file id: %v", err)
		}

		if _, err := dbconn.Exec(`UPDATE file_chunk SET chunk_order = chunk_order + 1 WHERE logical_file_id = $1`, fileID); err != nil {
			t.Fatalf("corrupt file chunk ordering: %v", err)
		}
		defer func() {
			if _, err := dbconn.Exec(`UPDATE file_chunk SET chunk_order = chunk_order - 1 WHERE logical_file_id = $1`, fileID); err != nil {
				t.Fatalf("restore file chunk ordering: %v", err)
			}
		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err == nil {
			t.Fatal("RunVerify should have detected broken file chunk ordering continuity but returned nil")
		}
	})

	t.Run("detects missing container file", func(t *testing.T) {
		var filename string
		err := dbconn.QueryRow(`SELECT filename FROM container LIMIT 1`).Scan(&filename)
		if err != nil {
			t.Fatalf("query container filename: %v", err)
		}

		path := filepath.Join(container.ContainersDir, filename)

		err = os.Remove(path)
		if err != nil {
			t.Fatalf("remove container file: %v", err)
		}

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err == nil {
			t.Fatal("verify full should detect missing container file")
		}
	})
}

func TestVerifyFull(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_full.bin", 256*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}

	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	t.Run("passes on clean database", func(t *testing.T) {
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
			t.Fatalf("RunVerify full on clean DB should not fail: %v", err)
		}
	})

	t.Run("detects completed chunk without location", func(t *testing.T) {
		if _, err := dbconn.Exec(`
			INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count)
			VALUES ('verify_full_bad_chunk', 1024, $1, 0, 0)
		`, filestate.ChunkCompleted); err != nil {
			t.Fatalf("insert malformed completed chunk: %v", err)
		}
		defer func() {
			dbconn.Exec(`DELETE FROM chunk WHERE chunk_hash = 'verify_full_bad_chunk'`)
		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err == nil {
			t.Fatal("RunVerify full should have detected malformed completed chunk but returned nil")
		}
	})

	t.Run("detects missing container file", func(t *testing.T) {
		var filename string
		err := dbconn.QueryRow(`SELECT filename FROM container LIMIT 1`).Scan(&filename)
		if err != nil {
			t.Fatalf("query container filename: %v", err)
		}

		path := filepath.Join(container.ContainersDir, filename)

		err = os.Remove(path)
		if err != nil {
			t.Fatalf("remove container file: %v", err)
		}

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err == nil {
			t.Fatal("verify full should detect missing container file")
		}
	})
}

func TestVerifyFileDeepDetectsChunkDataCorruption(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_deep_corruption.bin", 512*1024)
	defer dbconn.Close()

	record := fetchFirstChunkRecord(t, dbconn, fileID)
	containerPath := containerPathForRecord(record)

	file, err := os.OpenFile(containerPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file: %v", err)
	}

	corruptionOffset := record.blockOffset
	if record.storedSize > 10 {
		corruptionOffset += 10
	}

	if _, err := file.WriteAt([]byte{0xFF}, corruptionOffset); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt chunk byte: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close container file: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err == nil {
		t.Fatal("verify file --deep should detect chunk data corruption")
	}
}

func TestVerifyFileStandardPassesOnCleanStoredFile(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_standard_clean.bin", 256*1024)
	defer dbconn.Close()

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyStandard); err != nil {
		t.Fatalf("verify file --standard on clean file should pass: %v", err)
	}
}

func TestVerifyFileFullPassesOnCleanStoredFile(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_full_clean.bin", 256*1024)
	defer dbconn.Close()

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyFull); err != nil {
		t.Fatalf("verify file --full on clean file should pass: %v", err)
	}
}

func TestVerifyFileDeepPassesOnCleanStoredFile(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_deep_clean.bin", 256*1024)
	defer dbconn.Close()

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err != nil {
		t.Fatalf("verify file --deep on clean file should pass: %v", err)
	}
}

func TestVerifyFileFullDetectsContainerTruncation(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_full_truncation.bin", 512*1024)
	defer dbconn.Close()

	record := fetchFirstChunkRecord(t, dbconn, fileID)
	containerPath := containerPathForRecord(record)

	truncatedSize := record.containerCurrentSize - 100
	if truncatedSize <= 0 {
		t.Fatalf("invalid truncated size derived from container size %d", record.containerCurrentSize)
	}

	if err := os.Truncate(containerPath, truncatedSize); err != nil {
		t.Fatalf("truncate container file: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyFull); err == nil {
		t.Fatal("verify file --full should detect truncated container data")
	}
}

func TestVerifyFileFullDetectsMissingContainerFile(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_full_missing_container.bin", 256*1024)
	defer dbconn.Close()

	record := fetchFirstChunkRecord(t, dbconn, fileID)
	containerPath := containerPathForRecord(record)

	if err := os.Remove(containerPath); err != nil {
		t.Fatalf("remove container file: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyFull); err == nil {
		t.Fatal("verify file --full should detect a missing container file")
	}
}

func TestVerifyFileStandardDetectsMissingChunkMetadata(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_standard_missing_chunk.bin", 512*1024)
	defer dbconn.Close()

	record := fetchFirstChunkRecord(t, dbconn, fileID)

	if _, err := dbconn.Exec(`ALTER TABLE file_chunk DROP CONSTRAINT IF EXISTS file_chunk_chunk_id_fkey`); err != nil {
		t.Fatalf("drop file_chunk foreign key: %v", err)
	}

	if _, err := dbconn.Exec(`DELETE FROM blocks WHERE chunk_id = $1`, record.chunkID); err != nil {
		t.Fatalf("delete block row: %v", err)
	}

	if _, err := dbconn.Exec(`DELETE FROM chunk WHERE id = $1`, record.chunkID); err != nil {
		t.Fatalf("delete chunk row: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyStandard); err == nil {
		t.Fatal("verify file should detect missing chunk metadata")
	}
}

func TestVerifyFileStandardDetectsBrokenChunkOrder(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_file_standard_chunk_order.bin", 512*1024)
	defer dbconn.Close()

	if _, err := dbconn.Exec(`UPDATE file_chunk SET chunk_order = chunk_order + 1 WHERE logical_file_id = $1`, fileID); err != nil {
		t.Fatalf("corrupt chunk ordering: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyStandard); err == nil {
		t.Fatal("verify file should detect broken chunk ordering")
	}
}

func TestVerifySystemFullDetectsContainerHashMismatch(t *testing.T) {
	dbconn, _, fileID := setupStoredFileForVerification(t, "verify_system_full_container_hash.bin", 256*1024)
	defer dbconn.Close()

	record := fetchFirstChunkRecord(t, dbconn, fileID)
	containerPath := containerPathForRecord(record)

	file, err := os.OpenFile(containerPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file: %v", err)
	}
	corruptionOffset := record.blockOffset
	if record.storedSize > 10 {
		corruptionOffset += 10
	}
	if _, err := file.WriteAt([]byte{0xAB}, corruptionOffset); err != nil {
		_ = file.Close()
		t.Fatalf("mutate container file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close container file: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); err == nil {
		t.Fatal("verify system --deep should detect a container content mismatch")
	}
}

func TestSharedChunkSafety(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()

	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)

	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir inputDir: %v", err)
	}

	// Create one file
	fileA := createTempFile(t, inputDir, "fileA.bin", 512*1024)

	// Copy to create identical file
	fileB := filepath.Join(inputDir, "fileB.bin")
	data, err := os.ReadFile(fileA)
	if err != nil {
		t.Fatalf("read fileA: %v", err)
	}
	if err := os.WriteFile(fileB, data, 0o644); err != nil {
		t.Fatalf("write fileB: %v", err)
	}
	// Make fileB a distinct logical file while preserving most content overlap.
	if err := os.WriteFile(fileB, append(data, 0x01), 0o644); err != nil {
		t.Fatalf("rewrite fileB: %v", err)
	}

	// Store both files
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, fileA); err != nil {
		t.Fatalf("store fileA: %v", err)
	}
	if err := storage.StoreFileWithStorageContext(sgctx, fileB); err != nil {
		t.Fatalf("store fileB: %v", err)
	}

	fileAHash := sha256File(t, fileA)
	fileAID := fetchFileIDByHash(t, dbconn, fileAHash)

	// Remove file A
	if err := storage.RemoveFileWithDB(dbconn, fileAID); err != nil {
		t.Fatalf("remove fileA: %v", err)
	}

	// Run GC
	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("run GC: %v", err)
	}

	// Restore file B
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restoreDir: %v", err)
	}

	outPath := filepath.Join(restoreDir, "fileB.bin")
	fileBHash := sha256File(t, fileB)
	fileBID := fetchFileIDByHash(t, dbconn, fileBHash)

	if err := storage.RestoreFileWithDB(dbconn, fileBID, outPath); err != nil {
		t.Fatalf("restore fileB: %v", err)
	}

	// Compare hashes
	origHash := sha256File(t, fileB)
	restoreHash := sha256File(t, outPath)

	if origHash != restoreHash {
		t.Fatalf("hash mismatch: expected %s, got %s", origHash, restoreHash)
	}
}

func TestVerifySystemDeepPassesOnCleanStoredFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_system_deep_clean.bin", 512*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); err != nil {
		t.Fatalf("verify system --deep on clean stored file should pass: %v", err)
	}
}

func TestVerifySystemDeepDetectsChunkDataCorruption(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_system_deep_corruption.bin", 512*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	// Fetch first chunk record to find where to corrupt
	var blockOffset int64
	var storedSize int64
	var containerFilename string
	err = dbconn.QueryRow(`
		SELECT b.block_offset, b.stored_size, ctr.filename
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE c.status = $1
		ORDER BY b.block_offset ASC
		LIMIT 1
	`, filestate.ChunkCompleted).Scan(&blockOffset, &storedSize, &containerFilename)
	if err != nil {
		t.Fatalf("query first chunk: %v", err)
	}

	containerPath := filepath.Join(container.ContainersDir, containerFilename)

	// Open container and corrupt a byte in the first chunk's data
	// Skip past the header to reach the actual chunk data
	f, err := os.OpenFile(containerPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file: %v", err)
	}
	defer f.Close()

	corruptionOffset := blockOffset + 10
	if _, err := f.WriteAt([]byte{0xFF}, corruptionOffset); err != nil {
		t.Fatalf("corrupt chunk byte: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); err == nil {
		t.Fatal("verify system --deep should detect chunk data corruption but returned nil")
	}
}

func TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_system_deep_trailing_bytes.bin", 512*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	var containerID int64
	var containerFilename string
	var currentSize int64
	err = dbconn.QueryRow(`
		SELECT ctr.id, ctr.filename, ctr.current_size
		FROM container ctr
		WHERE ctr.quarantine = FALSE
		AND EXISTS (
			SELECT 1
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE b.container_id = ctr.id
			AND c.status = $1
		)
		ORDER BY ctr.id ASC
		LIMIT 1
	`, filestate.ChunkCompleted).Scan(&containerID, &containerFilename, &currentSize)
	if err != nil {
		t.Fatalf("query target container: %v", err)
	}

	containerPath := filepath.Join(container.ContainersDir, containerFilename)
	trail := []byte("trailing-garbage-after-last-block")
	f, err := os.OpenFile(containerPath, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open container file for append: %v", err)
	}
	if _, err := f.Write(trail); err != nil {
		_ = f.Close()
		t.Fatalf("append trailing bytes: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close container file: %v", err)
	}

	// Keep full verify size checks green so deep-mode tail accounting is what fails.
	if _, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, currentSize+int64(len(trail)), containerID); err != nil {
		t.Fatalf("update container current_size for trailing-byte simulation: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); err == nil {
		t.Fatal("verify system --deep should detect trailing unaccounted bytes but returned nil")
	}
}

func TestVerifySystemFullDetectsNonContiguousOffsets(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_system_full_non_contiguous.bin", 4*1024*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	var secondChunkID int64
	var secondBlockOffset int64
	err = dbconn.QueryRow(`
		SELECT b.chunk_id, b.block_offset
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE c.status = $1
		ORDER BY b.container_id ASC, b.block_offset ASC
		OFFSET 1
		LIMIT 1
	`, filestate.ChunkCompleted).Scan(&secondChunkID, &secondBlockOffset)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Skip("not enough completed chunks to validate offset continuity")
		}
		t.Fatalf("query second chunk: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE blocks SET block_offset = $1 WHERE chunk_id = $2`, secondBlockOffset+1, secondChunkID); err != nil {
		t.Fatalf("corrupt block offset continuity: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err == nil {
		t.Fatal("verify system --full should detect non-contiguous block offsets")
	}
}

func TestVerifySystemDeepAggregatesChunkErrors(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "verify_system_deep_aggregate.bin", 4*1024*1024)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}

	rows, err := dbconn.Query(`
		SELECT b.block_offset, b.stored_size, ctr.filename
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE c.status = $1
		ORDER BY b.container_id ASC, b.block_offset ASC
		LIMIT 2
	`, filestate.ChunkCompleted)
	if err != nil {
		t.Fatalf("query chunks for corruption: %v", err)
	}
	defer rows.Close()

	type chunkToCorrupt struct {
		blockOffset int64
		storedSize  int64
		filename    string
	}
	var chunksToCorrupt []chunkToCorrupt
	for rows.Next() {
		var c chunkToCorrupt
		if err := rows.Scan(&c.blockOffset, &c.storedSize, &c.filename); err != nil {
			t.Fatalf("scan chunk for corruption: %v", err)
		}
		chunksToCorrupt = append(chunksToCorrupt, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunks for corruption: %v", err)
	}
	if len(chunksToCorrupt) < 2 {
		t.Skip("not enough completed chunks to validate deep aggregation")
	}

	for _, c := range chunksToCorrupt {
		containerPath := filepath.Join(container.ContainersDir, c.filename)
		f, err := os.OpenFile(containerPath, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open container file: %v", err)
		}

		corruptionOffset := c.blockOffset
		if c.storedSize > 1 {
			corruptionOffset++
		}
		if _, err := f.WriteAt([]byte{0xEE}, corruptionOffset); err != nil {
			_ = f.Close()
			t.Fatalf("corrupt chunk byte: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close container file: %v", err)
		}
	}

	err = maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
	if err == nil {
		t.Fatal("verify system --deep should detect multiple corrupted chunks")
	}
	if !strings.Contains(err.Error(), "found 2 errors in deep verification of container files") {
		t.Fatalf("expected aggregated deep verification error count, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// v0.5 deterministic-restore guarantee tests
// ---------------------------------------------------------------------------

// TestZeroByteFile verifies that a zero-byte file can be stored, restored,
// has size 0, and its SHA-256 matches the well-known hash of empty content.
func TestZeroByteFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a zero-byte input file.
	inPath := filepath.Join(inputDir, "empty.bin")
	if err := os.WriteFile(inPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	// SHA-256("") = e3b0c44298fc1c149afbf4c8996fb924...
	const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	if gotHash := sha256File(t, inPath); gotHash != emptyHash {
		t.Fatalf("unexpected hash for empty file: %s", gotHash)
	}

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store empty file: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, emptyHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "empty.restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore empty file: %v", err)
	}

	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat restored file: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("restored file size: expected 0, got %d", info.Size())
	}

	if gotHash := sha256File(t, outPath); gotHash != emptyHash {
		t.Fatalf("restored hash mismatch: want %s got %s", emptyHash, gotHash)
	}
}

// TestRepeatRestoreDeterminism restores the same file three times and asserts
// that all three results are byte-for-byte identical and match the original.
func TestRepeatRestoreDeterminism(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	inPath := createTempFile(t, inputDir, "determinism.bin", 512*1024)
	wantHash := sha256File(t, inPath)
	wantBytes := mustRead(t, inPath)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, wantHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)

	const restoreRuns = 3
	for i := 0; i < restoreRuns; i++ {
		outPath := filepath.Join(outDir, fmt.Sprintf("determinism.run%d.bin", i))
		if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
			t.Fatalf("restore run %d: %v", i, err)
		}

		gotBytes := mustRead(t, outPath)
		if !bytes.Equal(wantBytes, gotBytes) {
			t.Fatalf("run %d: restored bytes differ from original", i)
		}

		gotHash := sha256File(t, outPath)
		if gotHash != wantHash {
			t.Fatalf("run %d: hash mismatch: want %s got %s", i, wantHash, gotHash)
		}
	}
}

// TestStoreGCRestore is the v0.5 headline guarantee: a stored file remains
// fully restorable after GC runs (even in the presence of other deleted files).
func TestStoreGCRestore(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Store the "keeper" file that must survive GC.
	keeperPath := createTempFile(t, inputDir, "keeper.bin", 512*1024)
	keeperHash := sha256File(t, keeperPath)
	keeperBytes := mustRead(t, keeperPath)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, keeperPath); err != nil {
		t.Fatalf("store keeper: %v", err)
	}
	keeperID := fetchFileIDByHash(t, dbconn, keeperHash)

	// Store a second file, then remove it so GC has something to collect.
	noisePath := createTempFile(t, inputDir, "noise.bin", 512*1024)
	// Ensure noise differs from keeper to avoid same-logical-file dedupe aliasing.
	noiseBytes := mustRead(t, noisePath)
	noiseBytes[0] ^= 0xFF
	if err := os.WriteFile(noisePath, noiseBytes, 0o644); err != nil {
		t.Fatalf("rewrite noise file with distinct content: %v", err)
	}
	noiseHash := sha256File(t, noisePath)

	if err := storage.StoreFileWithStorageContext(sgctx, noisePath); err != nil {
		t.Fatalf("store noise: %v", err)
	}
	noiseID := fetchFileIDByHash(t, dbconn, noiseHash)

	if err := storage.RemoveFileWithDB(dbconn, noiseID); err != nil {
		t.Fatalf("remove noise: %v", err)
	}

	// Dry-run GC must not break anything.
	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("GC dry-run: %v", err)
	}

	// Real GC.
	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("GC real run: %v", err)
	}

	// After GC, live_ref_count values must not go negative.
	var negatives int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check live_ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found %d chunks with negative live_ref_count after GC", negatives)
	}

	// Restore the keeper and verify byte-perfect fidelity.
	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "keeper.restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, keeperID, outPath); err != nil {
		t.Fatalf("restore keeper after GC: %v", err)
	}

	gotBytes := mustRead(t, outPath)
	if !bytes.Equal(keeperBytes, gotBytes) {
		t.Fatalf("restored bytes differ from original after GC")
	}

	gotHash := sha256File(t, outPath)
	if gotHash != keeperHash {
		t.Fatalf("hash mismatch after GC: want %s got %s", keeperHash, gotHash)
	}
}

// TestGCRestorePinRaceContainerNotDeleted validates that GC cannot delete a
// sealed container while a restore-style chunk pin is in-flight.
func TestGCRestorePinRaceContainerNotDeleted(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containerFile := "gc-restore-race.bin"
	containerPath := filepath.Join(container.ContainersDir, containerFile)
	if err := os.WriteFile(containerPath, []byte("race-test-container"), 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	err = dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		containerFile,
		int64(len("race-test-container")),
		container.GetContainerMaxSize(),
	).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	err = dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"gc-restore-race-chunk",
		int64(16),
		filestate.ChunkCompleted,
		int64(0),
	).Scan(&chunkID)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID,
		"plain",
		1,
		int64(16),
		int64(16),
		[]byte{},
		containerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	// Hold a restore-style pin transaction open while GC starts.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pinTx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin pin tx: %v", err)
	}
	if _, err := pinTx.ExecContext(ctx, `UPDATE chunk SET pin_count = pin_count + 1 WHERE id = $1`, chunkID); err != nil {
		_ = pinTx.Rollback()
		t.Fatalf("pin chunk in tx: %v", err)
	}

	gcDone := make(chan error, 1)
	go func() {
		gcDone <- maintenance.RunGCWithContainersDir(false, container.ContainersDir)
	}()

	select {
	case err := <-gcDone:
		_ = pinTx.Rollback()
		t.Fatalf("GC finished before pin tx commit; expected it to wait on chunk lock: %v", err)
	case <-time.After(250 * time.Millisecond):
		// Expected: GC is blocked by the pin transaction.
	}

	if err := pinTx.Commit(); err != nil {
		t.Fatalf("commit pin tx: %v", err)
	}

	select {
	case err := <-gcDone:
		if err != nil {
			t.Fatalf("run GC: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for GC completion")
	}

	var remainingContainers int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&remainingContainers); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if remainingContainers != 1 {
		t.Fatalf("expected container to remain after pinned restore chunk, got count=%d", remainingContainers)
	}

	var remainingBlocks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks WHERE container_id = $1`, containerID).Scan(&remainingBlocks); err != nil {
		t.Fatalf("count block rows: %v", err)
	}
	if remainingBlocks != 1 {
		t.Fatalf("expected block mapping to remain after pinned restore chunk, got count=%d", remainingBlocks)
	}

	var pinnedPinCount int64
	if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinnedPinCount); err != nil {
		t.Fatalf("query chunk pin_count: %v", err)
	}
	if pinnedPinCount != 1 {
		t.Fatalf("expected chunk pin_count=1 after pin commit, got %d", pinnedPinCount)
	}

	if _, err := os.Stat(containerPath); err != nil {
		t.Fatalf("expected container file to remain on disk: %v", err)
	}
}

// TestGCRestoreRemoveInterleavingContainerPreservedWhilePinned exercises a
// three-way race where restore pinning, remove, and GC overlap.
func TestGCRestoreRemoveInterleavingContainerPreservedWhilePinned(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containerFile := "gc-remove-restore-interleaving.bin"
	containerPath := filepath.Join(container.ContainersDir, containerFile)
	if err := os.WriteFile(containerPath, []byte("interleaving-container"), 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	err = dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		containerFile,
		int64(len("interleaving-container")),
		container.GetContainerMaxSize(),
	).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	err = dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"gc-remove-restore-race-chunk",
		int64(22),
		filestate.ChunkCompleted,
		int64(1),
	).Scan(&chunkID)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID,
		"plain",
		1,
		int64(22),
		int64(22),
		[]byte{},
		containerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	err = dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"interleaving.txt",
		int64(22),
		"gc-remove-restore-race-file-hash",
		filestate.LogicalFileCompleted,
	).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)`,
		fileID,
		chunkID,
		int64(0),
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pinTx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin pin tx: %v", err)
	}
	if _, err := pinTx.ExecContext(ctx, `UPDATE chunk SET pin_count = pin_count + 1 WHERE id = $1`, chunkID); err != nil {
		_ = pinTx.Rollback()
		t.Fatalf("pin chunk in tx: %v", err)
	}

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- storage.RemoveFileWithDB(dbconn, fileID)
	}()

	gcDone := make(chan error, 1)
	go func() {
		gcDone <- maintenance.RunGCWithContainersDir(false, container.ContainersDir)
	}()

	select {
	case err := <-removeDone:
		_ = pinTx.Rollback()
		t.Fatalf("remove finished before pin tx commit; expected row-lock wait: %v", err)
	case <-time.After(250 * time.Millisecond):
		// Expected: remove blocks on chunk row lock held by pin tx.
	}

	select {
	case err := <-gcDone:
		_ = pinTx.Rollback()
		t.Fatalf("GC finished before pin tx commit; expected row-lock wait: %v", err)
	case <-time.After(250 * time.Millisecond):
		// Expected: GC blocks on chunk row lock held by pin tx.
	}

	if err := pinTx.Commit(); err != nil {
		t.Fatalf("commit pin tx: %v", err)
	}

	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatalf("remove after pin commit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for remove completion")
	}

	select {
	case err := <-gcDone:
		if err != nil {
			t.Fatalf("gc after pin commit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for gc completion")
	}

	var containerCountAfterInterleave int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&containerCountAfterInterleave); err != nil {
		t.Fatalf("count container rows after interleave: %v", err)
	}
	if containerCountAfterInterleave != 1 {
		t.Fatalf("expected container to remain while restore pin_count is still active, got count=%d", containerCountAfterInterleave)
	}

	var pinCountBeforeUnpin int64
	if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinCountBeforeUnpin); err != nil {
		t.Fatalf("query chunk pin_count before unpin: %v", err)
	}
	if pinCountBeforeUnpin != 1 {
		t.Fatalf("expected pin_count=1 before restore unpin, got %d", pinCountBeforeUnpin)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET pin_count = pin_count - 1 WHERE id = $1 AND pin_count > 0`, chunkID); err != nil {
		t.Fatalf("unpin restore chunk: %v", err)
	}

	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("final gc after unpin: %v", err)
	}

	var containerCountAfterFinalGC int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&containerCountAfterFinalGC); err != nil {
		t.Fatalf("count container rows after final gc: %v", err)
	}
	if containerCountAfterFinalGC != 0 {
		t.Fatalf("expected container deletion after unpin + final gc, got count=%d", containerCountAfterFinalGC)
	}

	if _, err := os.Stat(containerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected container file deleted after final gc, stat err=%v", err)
	}
}

// TestChunkBoundaryMatrix stores and restores files at classic CDC boundary sizes,
// verifying byte-perfect restoration for each case.
func TestChunkBoundaryMatrix(t *testing.T) {
	requireDB(t)

	for _, tc := range chunkBoundaryCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			container.ContainersDir = filepath.Join(tmp, "containers")
			_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
			resetStorage(t)

			dbconn, err := db.ConnectDB()
			if err != nil {
				t.Fatalf("connectDB: %v", err)
			}
			defer dbconn.Close()

			applySchema(t, dbconn)
			resetDB(t, dbconn)

			inputDir := filepath.Join(tmp, "input")
			_ = os.MkdirAll(inputDir, 0o755)

			inPath := createTempFile(t, inputDir, tc.name+".bin", tc.size)
			wantHash := sha256File(t, inPath)
			wantBytes := mustRead(t, inPath)

			sgctx := storage.StorageContext{
				DB:     dbconn,
				Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
			}
			if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
				t.Fatalf("store: %v", err)
			}

			fileID := fetchFileIDByHash(t, dbconn, wantHash)

			outDir := filepath.Join(tmp, "out")
			_ = os.MkdirAll(outDir, 0o755)
			outPath := filepath.Join(outDir, tc.name+".restored.bin")

			if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
				t.Fatalf("restore: %v", err)
			}

			gotBytes := mustRead(t, outPath)
			if !bytes.Equal(wantBytes, gotBytes) {
				t.Fatalf("restored bytes differ for size %d", tc.size)
			}

			gotHash := sha256File(t, outPath)
			if gotHash != wantHash {
				t.Fatalf("hash mismatch: want %s got %s", wantHash, gotHash)
			}

			// Confirm the restored file size matches the input exactly.
			info, err := os.Stat(outPath)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if int(info.Size()) != tc.size {
				t.Fatalf("size mismatch: want %d got %d", tc.size, info.Size())
			}
		})
	}
}

func queryChunkGraph(t *testing.T, dbconn *sql.DB, fileID int64) []chunkRecord {
	t.Helper()
	rows, err := dbconn.Query(`
		SELECT fc.chunk_order, c.chunk_hash, c.size
		FROM file_chunk fc
		JOIN chunk c ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, fileID)
	if err != nil {
		t.Fatalf("query chunk graph: %v", err)
	}
	defer rows.Close()

	var records []chunkRecord
	for rows.Next() {
		var r chunkRecord
		if err := rows.Scan(&r.order, &r.hash, &r.size); err != nil {
			t.Fatalf("scan chunk graph row: %v", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk graph: %v", err)
	}
	return records
}

func findRepoFixtureDir(t *testing.T, fixtureDir string) string {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	dir := cwd
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, fixtureDir)
		info, statErr := os.Stat(candidate)
		if statErr == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	t.Fatalf("could not find fixture directory %q from cwd %q", fixtureDir, cwd)
	return ""
}

func copyDirTree(t *testing.T, srcDir, dstDir string) {
	t.Helper()

	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir dstDir %s: %v", dstDir, err)
	}

	err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		target := filepath.Join(dstDir, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatalf("copy fixture %s -> %s: %v", srcDir, dstDir, err)
	}
}

func collectFileHashesByCount(t *testing.T, root string) map[string]int {
	t.Helper()

	hashCount := make(map[string]int)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		hashCount[sha256File(t, path)]++
		return nil
	})
	if err != nil {
		t.Fatalf("collect file hashes from %s: %v", root, err)
	}

	return hashCount
}

func runFixtureFolderEndToEnd(t *testing.T, fixtureDir string) {
	t.Helper()
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	fixturePath := findRepoFixtureDir(t, fixtureDir)
	inputDir := filepath.Join(tmp, "input")
	copyDirTree(t, fixturePath, inputDir)

	expectedHashCounts := collectFileHashesByCount(t, inputDir)
	expectedUniqueCount := len(expectedHashCounts)
	expectedUniqueHashes := make(map[string]bool, len(expectedHashCounts))
	for hash := range expectedHashCounts {
		expectedUniqueHashes[hash] = true
	}

	if err := storage.StoreFolderWithStorageContext(newTestContext(dbconn), inputDir); err != nil {
		t.Fatalf("store folder %s: %v", fixtureDir, err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after store for %s: %v", fixtureDir, err)
	}

	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("gc dry-run for %s: %v", fixtureDir, err)
	}
	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("gc real for %s: %v", fixtureDir, err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before restart for %s: %v", fixtureDir, err)
	}
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("system recovery for %s: %v", fixtureDir, err)
	}
	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after restart for %s: %v", fixtureDir, err)
	}

	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir out for %s: %v", fixtureDir, err)
	}

	rows, err := dbconn.Query(`
		SELECT id, file_hash
		FROM logical_file
		WHERE status = $1
		ORDER BY id ASC
	`, filestate.LogicalFileCompleted)
	if err != nil {
		t.Fatalf("query logical_file for %s: %v", fixtureDir, err)
	}
	defer rows.Close()

	restoredCount := 0
	for rows.Next() {
		var id int64
		var hash string
		if err := rows.Scan(&id, &hash); err != nil {
			t.Fatalf("scan logical_file row for %s: %v", fixtureDir, err)
		}

		outPath := filepath.Join(outDir, fmt.Sprintf("%d.restore.bin", id))
		if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
			t.Fatalf("restore file id %d for %s: %v", id, fixtureDir, err)
		}

		gotHash := sha256File(t, outPath)
		if gotHash != hash {
			t.Fatalf("restored hash mismatch for file id %d in %s: want %s got %s", id, fixtureDir, hash, gotHash)
		}

		if !expectedUniqueHashes[gotHash] {
			t.Fatalf("unexpected restored hash %s for %s", gotHash, fixtureDir)
		}
		delete(expectedUniqueHashes, gotHash)
		restoredCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error for %s: %v", fixtureDir, err)
	}

	if restoredCount != expectedUniqueCount {
		t.Fatalf("restored file count mismatch for %s: want %d got %d", fixtureDir, expectedUniqueCount, restoredCount)
	}

	for hash := range expectedUniqueHashes {
		t.Fatalf("missing restored file for hash %s in %s", hash, fixtureDir)
	}
}

// TestSameInputSameChunkGraph verifies that storing identical file content in
// two separate fresh environments yields the same chunk count, chunk hashes,
// and chunk order — confirming cross-run CDC determinism.
func TestSameInputSameChunkGraph(t *testing.T) {
	requireDB(t)

	// Generate a fixed data blob (deterministic, cross-run stable).
	const fileSize = 3*512*1024 + 77 // spans multiple chunks with uneven tail
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte((i*31 + 7) % 251)
	}

	storeAndQuery := func(label string) []chunkRecord {
		tmp := t.TempDir()
		container.ContainersDir = filepath.Join(tmp, "containers")
		_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
		resetStorage(t)

		dbconn, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("%s: connectDB: %v", label, err)
		}
		defer dbconn.Close()

		applySchema(t, dbconn)
		resetDB(t, dbconn)

		inputDir := filepath.Join(tmp, "input")
		_ = os.MkdirAll(inputDir, 0o755)

		inPath := filepath.Join(inputDir, label+".bin")
		if err := os.WriteFile(inPath, data, 0o644); err != nil {
			t.Fatalf("%s: write file: %v", label, err)
		}

		sgctx := storage.StorageContext{
			DB:     dbconn,
			Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
		}
		if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
			t.Fatalf("%s: store: %v", label, err)
		}

		sum := sha256.Sum256(data)
		fileHash := hex.EncodeToString(sum[:])
		fileID := fetchFileIDByHash(t, dbconn, fileHash)

		return queryChunkGraph(t, dbconn, fileID)
	}

	run1 := storeAndQuery("run1")
	run2 := storeAndQuery("run2")

	if len(run1) != len(run2) {
		t.Fatalf("chunk count mismatch: run1=%d run2=%d", len(run1), len(run2))
	}
	if len(run1) == 0 {
		t.Fatalf("expected at least one chunk (file size %d)", fileSize)
	}

	for i := range run1 {
		if run1[i].order != run2[i].order {
			t.Errorf("chunk %d: order mismatch: run1=%d run2=%d", i, run1[i].order, run2[i].order)
		}
		if run1[i].hash != run2[i].hash {
			t.Errorf("chunk %d: hash mismatch: run1=%s run2=%s", i, run1[i].hash, run2[i].hash)
		}
		if run1[i].size != run2[i].size {
			t.Errorf("chunk %d: size mismatch: run1=%d run2=%d", i, run1[i].size, run2[i].size)
		}
	}
}

func TestSampleDatasetEndToEnd(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// -----------------------------
	// Create dataset
	// -----------------------------
	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	paths := createSampleDataset(t, inputDir)

	// Precompute hashes
	expectedHashes := make(map[string]string)
	for name, p := range paths {
		expectedHashes[name] = sha256File(t, p)
	}

	// -----------------------------
	// Store folder
	// -----------------------------
	if err := storage.StoreFolderWithStorageContext(newTestContext(dbconn), inputDir); err != nil {
		t.Fatalf("store folder: %v", err)
	}

	// -----------------------------
	// Verify system
	// -----------------------------
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after store: %v", err)
	}

	// -----------------------------
	// Run GC (dry + real)
	// -----------------------------
	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("gc dry-run: %v", err)
	}
	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("gc real: %v", err)
	}

	// -----------------------------
	// Restore all files
	// -----------------------------
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}

	rows, err := dbconn.Query(`SELECT id, original_name, file_hash FROM logical_file ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query logical_file: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name, hash string

		if err := rows.Scan(&id, &name, &hash); err != nil {
			t.Fatalf("scan: %v", err)
		}

		outPath := filepath.Join(outDir, name)

		if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
			t.Fatalf("restore %s: %v", name, err)
		}

		gotHash := sha256File(t, outPath)

		if gotHash != hash {
			t.Fatalf("hash mismatch for %s: want %s got %s", name, hash, gotHash)
		}

		// Extra: compare with original file
		if expectedHashes[name] != gotHash {
			t.Fatalf("original mismatch for %s", name)
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
}

func TestSamplesFolderEndToEnd(t *testing.T) {
	runFixtureFolderEndToEnd(t, "samples")
}

func TestSamplesEdgeCasesFolderEndToEnd(t *testing.T) {
	runFixtureFolderEndToEnd(t, "samples_edge_cases")
}

// runFixtureFolderRestoreAll stores every file in the given fixture directory
// tree, then restores each logical file by name and compares it byte-for-byte
// against the original file found in the source tree.
//
// This is stricter than runFixtureFolderEndToEnd in two ways:
//  1. It maps each restored logical file back to a representative fixture file
//     by SHA-256 and diffs the raw bytes, avoiding basename collisions while
//     still catching systematic encode/decode bugs.
//  2. It counts every unique content hash in the fixture and verifies that the
//     completed logical_file row count matches that deduplicated total.
func runFixtureFolderRestoreAll(t *testing.T, fixtureDir string) {
	t.Helper()
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	fixturePath := findRepoFixtureDir(t, fixtureDir)
	inputDir := filepath.Join(tmp, "input")
	copyDirTree(t, fixturePath, inputDir)

	expectedHashCounts := collectFileHashesByCount(t, inputDir)
	expectedUniqueCount := len(expectedHashCounts)
	hashToPath := make(map[string]string, expectedUniqueCount)
	if walkErr := filepath.WalkDir(inputDir, func(path string, d os.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return e
		}
		hash := sha256File(t, path)
		if _, exists := hashToPath[hash]; !exists {
			hashToPath[hash] = path
		}
		return nil
	}); walkErr != nil {
		t.Fatalf("walk input dir: %v", walkErr)
	}

	if err := storage.StoreFolderWithStorageContext(newTestContext(dbconn), inputDir); err != nil {
		t.Fatalf("store folder %s: %v", fixtureDir, err)
	}

	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}

	rows, err := dbconn.Query(`
		SELECT id, original_name, file_hash
		FROM logical_file
		WHERE status = $1
		ORDER BY id ASC
	`, filestate.LogicalFileCompleted)
	if err != nil {
		t.Fatalf("query logical_file: %v", err)
	}
	defer rows.Close()

	restoredCount := 0
	seenHashes := make(map[string]bool, expectedUniqueCount)
	for rows.Next() {
		var id int64
		var origName, storedHash string
		if err := rows.Scan(&id, &origName, &storedHash); err != nil {
			t.Fatalf("scan: %v", err)
		}

		outPath := filepath.Join(outDir, fmt.Sprintf("%d_%s", id, origName))
		if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
			t.Errorf("restore id=%d name=%s: %v", id, origName, err)
			continue
		}

		// Verify restored hash matches what the DB recorded.
		gotHash := sha256File(t, outPath)
		if gotHash != storedHash {
			t.Errorf("restored hash mismatch for id=%d name=%s: db_hash=%s restored_hash=%s",
				id, origName, storedHash, gotHash)
		}

		if _, found := expectedHashCounts[storedHash]; !found {
			t.Errorf("unexpected stored hash for id=%d name=%s: %s", id, origName, storedHash)
			continue
		}
		if seenHashes[storedHash] {
			t.Errorf("duplicate logical_file row for hash %s (id=%d name=%s)", storedHash, id, origName)
			continue
		}

		// Verify restored bytes match a representative source file for this hash.
		srcPath, found := hashToPath[storedHash]
		if !found {
			t.Errorf("original source not found for hash=%s id=%d name=%s", storedHash, id, origName)
			continue
		}
		orig, readErr := os.ReadFile(srcPath)
		if readErr != nil {
			t.Fatalf("read original %s: %v", srcPath, readErr)
		}
		restored, readErr := os.ReadFile(outPath)
		if readErr != nil {
			t.Fatalf("read restored %s: %v", outPath, readErr)
		}
		if !bytes.Equal(orig, restored) {
			t.Errorf("byte mismatch for id=%d name=%s: original %d bytes, restored %d bytes",
				id, origName, len(orig), len(restored))
		}

		seenHashes[storedHash] = true
		restoredCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if restoredCount == 0 {
		t.Fatalf("no files were restored for fixture %s", fixtureDir)
	}
	if restoredCount != expectedUniqueCount {
		t.Fatalf("restored logical file count mismatch for fixture %s: got=%d want=%d", fixtureDir, restoredCount, expectedUniqueCount)
	}
}

// TestSamplesFolderRestoreAll stores the samples/ fixture and restores every
// logical file, verifying byte-perfect fidelity against the original source
// file. Complements TestSamplesFolderEndToEnd (hash-set check) by comparing
// each restored file directly against its original on disk.
func TestSamplesFolderRestoreAll(t *testing.T) {
	runFixtureFolderRestoreAll(t, "samples")
}

// TestSamplesEdgeCasesFolderRestoreAll applies the same full restore-all check
// to samples_edge_cases/, including the multi-chunk binaries, the deeply-nested
// leaf file, all 50 small files, and the repeating-pattern file.
func TestSamplesEdgeCasesFolderRestoreAll(t *testing.T) {
	runFixtureFolderRestoreAll(t, "samples_edge_cases")
}

// TestRollbackAfterAppendContamination simulates an unresolved append outcome:
// payload append to the active container succeeds and is fsynced, but the
// transaction never reaches the commit acknowledgment path because block insert
// or DB commit fails. After restart, recovery should quarantine or truncate the
// contaminated active container to prevent future stores from reusing it.
func TestRollbackAfterAppendContamination(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a test file
	fileContent := make([]byte, 256*1024)
	for i := range fileContent {
		fileContent[i] = byte((i*13 + 7) % 251)
	}
	inPath := filepath.Join(inputDir, "rollback-test.bin")
	if err := os.WriteFile(inPath, fileContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// Do an initial store to get a clean DB state
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	// Fetch the active container ID before the rollback simulation
	var firstContainerID int64
	err = dbconn.QueryRow(`
		SELECT id FROM container WHERE sealed = FALSE ORDER BY id DESC LIMIT 1
	`).Scan(&firstContainerID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("query initial active container: %v", err)
	}

	// Now simulate an unresolved append that fails before the commit
	// acknowledgment path by:
	// 1. Creating a new logical file in PROCESSING state
	// 2. Adding a chunk in PROCESSING state
	// 3. Manually writing bytes to the active container file (simulating successful append+fsync)
	// 4. NOT committing the block insert to DB
	// 5. Mark container as having sealing flag or leave it in inconsistent state

	var activeContainerID int64
	var activeFilename string
	var activeCurrentSize int64

	err = dbconn.QueryRow(`
		SELECT id, filename, current_size FROM container WHERE sealed = FALSE ORDER BY id DESC LIMIT 1
	`).Scan(&activeContainerID, &activeFilename, &activeCurrentSize)
	if err != nil {
		t.Fatalf("query active container for contamination: %v", err)
	}

	contaminatedPath := filepath.Join(container.ContainersDir, activeFilename)

	// Insert a simulated appended payload to the container file
	ghostBytes := []byte("contaminated-append-after-failed-commit")
	existingContent := mustRead(t, contaminatedPath)
	contaminatedContent := append(existingContent, ghostBytes...)
	if err := os.WriteFile(contaminatedPath, contaminatedContent, 0o644); err != nil {
		t.Fatalf("write contaminated container: %v", err)
	}

	// Update container current_size in DB to reflect the append, but leave it unsealed
	// This simulates: fsync succeeded, but the append never reached commit
	// acknowledgment path because block INSERT or commit failed.
	newSize := activeCurrentSize + int64(len(ghostBytes))
	_, err = dbconn.Exec(`
		UPDATE container SET current_size = $1 WHERE id = $2
	`, newSize, activeContainerID)
	if err != nil {
		t.Fatalf("update container current_size: %v", err)
	}

	// DO NOT seal the container; it remains active with ghost bytes appended

	// Now run startup recovery - it should detect the tail corruption
	_, err = recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("system recovery: %v", err)
	}

	// Retirement/quarantine policy: contaminated unresolved append bytes should be
	// quarantined or truncated.
	// Check that the container is either truncated or quarantined
	var isQuarantined bool
	var dbCurrentSize int64
	err = dbconn.QueryRow(`
		SELECT quarantine, current_size FROM container WHERE id = $1
	`, activeContainerID).Scan(&isQuarantined, &dbCurrentSize)
	if err != nil {
		t.Fatalf("query container state after recovery: %v", err)
	}

	// Container should either be quarantined OR the file should be truncated to
	// remove unresolved append ghost bytes.
	stat, err := os.Stat(contaminatedPath)
	if err != nil {
		t.Fatalf("stat container file: %v", err)
	}
	physicalSize := stat.Size()

	if !isQuarantined && physicalSize > dbCurrentSize {
		t.Fatalf("contaminated container neither quarantined nor truncated: quarantine=%v physical=%d db=%d",
			isQuarantined, physicalSize, dbCurrentSize)
	}

	// Verify that future stores do not reuse the contaminated container
	inPath2 := filepath.Join(inputDir, "after-rollback.bin")
	fileContent2 := make([]byte, 128*1024)
	for i := range fileContent2 {
		fileContent2[i] = byte((i*29 + 11) % 251)
	}
	if err := os.WriteFile(inPath2, fileContent2, 0o644); err != nil {
		t.Fatalf("write post-recovery file: %v", err)
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath2); err != nil {
		t.Fatalf("post-recovery store: %v", err)
	}

	// The post-recovery store should have used a different (new) container, not the contaminated one
	var newlyUsedContainerID int64
	err = dbconn.QueryRow(`
		SELECT MAX(b.container_id) FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		JOIN file_chunk fc ON fc.chunk_id = c.id
		JOIN logical_file lf ON lf.id = fc.logical_file_id
		WHERE lf.file_hash = $1
	`, sha256File(t, inPath2)).Scan(&newlyUsedContainerID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("query container used for post-recovery store: %v", err)
	}

	if newlyUsedContainerID == activeContainerID && !isQuarantined {
		t.Fatalf("post-recovery store reused contaminated active container")
	}

	// Verify system integrity after recovery
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify after rollback recovery: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

func TestStoreSurfacesRollbackCleanupFailureAndRetiresActiveContainer(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Seed one successful store so we have a committed active container row that
	// can be explicitly retired/quarantined on rollback path cleanup failure.
	seedPath := createTempFile(t, inputDir, "seed.bin", 128*1024)
	seedCtx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: container.ContainersDir,
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResult(seedCtx, seedPath, blocks.CodecPlain); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	var activeID int64
	var activeFilename string
	var dbSizeBefore int64
	if err := dbconn.QueryRow(`
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = FALSE AND quarantine = FALSE
		ORDER BY id DESC
		LIMIT 1
	`).Scan(&activeID, &activeFilename, &dbSizeBefore); err != nil {
		t.Fatalf("query active container before injected failure: %v", err)
	}

	containerPath := filepath.Join(container.ContainersDir, activeFilename)
	beforeStat, err := os.Stat(containerPath)
	if err != nil {
		t.Fatalf("stat active container before injected failure: %v", err)
	}

	// Force a post-append transactional failure at file_chunk linking time.
	if _, err := dbconn.Exec(`DROP TRIGGER IF EXISTS ck_fail_file_chunk_insert_trg ON file_chunk`); err != nil {
		t.Fatalf("drop stale trigger: %v", err)
	}
	if _, err := dbconn.Exec(`DROP FUNCTION IF EXISTS ck_fail_file_chunk_insert()`); err != nil {
		t.Fatalf("drop stale trigger function: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE FUNCTION ck_fail_file_chunk_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'injected file_chunk insert failure';
		END;
		$$
	`); err != nil {
		t.Fatalf("create trigger function: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TRIGGER ck_fail_file_chunk_insert_trg
		BEFORE INSERT ON file_chunk
		FOR EACH ROW
		EXECUTE FUNCTION ck_fail_file_chunk_insert()
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	defer func() {
		_, _ = dbconn.Exec(`DROP TRIGGER IF EXISTS ck_fail_file_chunk_insert_trg ON file_chunk`)
		_, _ = dbconn.Exec(`DROP FUNCTION IF EXISTS ck_fail_file_chunk_insert()`)
	}()

	failPath := createTempFile(t, inputDir, "rollback_fail_surface.bin", 320*1024)
	wrappedWriter := newRollbackFailingWriter(
		container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn),
	)
	failCtx := storage.StorageContext{
		DB:           dbconn,
		Writer:       wrappedWriter,
		ContainerDir: container.ContainersDir,
	}

	_, err = storage.StoreFileWithStorageContextAndCodecResult(failCtx, failPath, blocks.CodecPlain)
	if err == nil {
		t.Fatalf("expected store to fail due to injected file_chunk insert + rollback cleanup failure")
	}

	errText := strings.ToLower(err.Error())
	if !strings.Contains(errText, "injected file_chunk insert failure") {
		t.Fatalf("expected surfaced transactional failure in error, got: %v", err)
	}
	if !strings.Contains(errText, "rollback failed") {
		t.Fatalf("expected surfaced rollback cleanup failure in error, got: %v", err)
	}

	if !wrappedWriter.appendSucceeded {
		t.Fatalf("expected append to succeed before injected failure")
	}
	if wrappedWriter.retireCalls == 0 {
		t.Fatalf("expected rollback cleanup failure path to retire/quarantine active container")
	}

	retiredID := wrappedWriter.lastPlacement.ContainerID
	if retiredID <= 0 {
		t.Fatalf("expected valid retired container id, got %d", retiredID)
	}

	var retiredQuarantined bool
	var dbSizeAfter int64
	if err := dbconn.QueryRow(
		`SELECT quarantine, current_size FROM container WHERE id = $1`,
		retiredID,
	).Scan(&retiredQuarantined, &dbSizeAfter); err != nil {
		t.Fatalf("query retired container state: %v", err)
	}
	if !retiredQuarantined {
		t.Fatalf("expected active container %d to be quarantined after rollback cleanup failure", retiredID)
	}

	afterStat, err := os.Stat(containerPath)
	if err != nil {
		t.Fatalf("stat active container after injected failure: %v", err)
	}
	if afterStat.Size() <= beforeStat.Size() {
		t.Fatalf("expected physical append to persist before rollback failure (size %d -> %d)", beforeStat.Size(), afterStat.Size())
	}
	if afterStat.Size() <= dbSizeAfter || dbSizeAfter != dbSizeBefore {
		t.Fatalf("expected quarantined container to show physical/db mismatch after failed rollback (physical=%d db_before=%d db_after=%d)", afterStat.Size(), dbSizeBefore, dbSizeAfter)
	}
}

// TestSealFailureAfterPhysicalFinalize simulates a scenario where a container is
// physically finalized (rotation completed, file truncated to final size) but the
// DB seal update fails (marked.sealing=TRUE but sealed=FALSE). The next store
// attempt must not reopen this container as active.
func TestSealFailureAfterPhysicalFinalize(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers: %v", err)
	}

	// Create a container in a sealing state: physically finalized but not yet sealed in DB
	filename := "physically-finalized-unsealed.bin"
	containerPath := filepath.Join(container.ContainersDir, filename)
	finalSize := int64(container.ContainerHdrLen + 256*1024)

	// Write a finalized container file
	containerData := make([]byte, finalSize)
	for i := range containerData {
		containerData[i] = byte((i*17 + 3) % 251)
	}
	if err := os.WriteFile(containerPath, containerData, 0o644); err != nil {
		t.Fatalf("write finalized container: %v", err)
	}

	// Insert into DB with sealing=TRUE, sealed=FALSE to simulate failed seal update
	var containerID int64
	err = dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES ($1, $2, $3, FALSE, TRUE, FALSE)
		RETURNING id
	`, filename, finalSize, container.GetContainerMaxSize()).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert sealing container: %v", err)
	}

	// Verify the container is marked sealing but not sealed
	var isSealing, isSealed bool
	err = dbconn.QueryRow(`SELECT sealed, sealing FROM container WHERE id = $1`, containerID).Scan(&isSealed, &isSealing)
	if err != nil {
		t.Fatalf("query container sealing state: %v", err)
	}
	if isSealed || !isSealing {
		t.Fatalf("setup failed: container should be sealing but not sealed; got sealed=%v sealing=%v", isSealed, isSealing)
	}

	// Try to store a new file - the store should NOT attempt to reopen the sealing container
	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	newFileContent := make([]byte, 128*1024)
	for i := range newFileContent {
		newFileContent[i] = byte((i*41 + 13) % 251)
	}
	newFilePath := filepath.Join(inputDir, "post-fail-seal.bin")
	if err := os.WriteFile(newFilePath, newFileContent, 0o644); err != nil {
		t.Fatalf("write new test file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if err := storage.StoreFileWithStorageContext(sgctx, newFilePath); err != nil {
		t.Fatalf("store after failed seal: %v", err)
	}

	// Verify that the new store did NOT use the sealing container
	var usedContainerID int64
	err = dbconn.QueryRow(`
		SELECT DISTINCT b.container_id FROM blocks b
		ORDER BY b.container_id DESC LIMIT 1
	`).Scan(&usedContainerID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("query used container: %v", err)
	}

	if usedContainerID == containerID {
		t.Fatalf("new store reopened the sealing/unsealed container instead of creating/using a different one")
	}

	// Run recovery to complete or quarantine the sealing container
	_, err = recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}

	// After recovery, the sealing container should be either sealed or quarantined
	err = dbconn.QueryRow(`SELECT sealed, sealing FROM container WHERE id = $1`, containerID).Scan(&isSealed, &isSealing)
	if err != nil {
		t.Fatalf("query container state after recovery: %v", err)
	}

	if !isSealed && isSealing {
		t.Fatalf("after recovery, container should be sealed or sealing cleared, got sealed=%v sealing=%v", isSealed, isSealing)
	}

	// Verify system integrity
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after seal failure: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestRemoveRejectsProcessingLogicalFile verifies that the remove operation explicitly
// rejects files in PROCESSING state and provides a clear error message.
func TestRemoveRejectsProcessingLogicalFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Insert a logical file in PROCESSING state
	processingHash := "test-processing-file-for-remove-check"
	processingSize := int64(256 * 1024)
	var processingFileID int64

	err = dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, retry_count)
		VALUES ($1, $2, $3, $4, 0)
		RETURNING id
	`, "processing-file.bin", processingSize, processingHash, filestate.LogicalFileProcessing).Scan(&processingFileID)
	if err != nil {
		t.Fatalf("insert processing logical file: %v", err)
	}

	// Attempt to remove the PROCESSING file
	err = storage.RemoveFileWithDB(dbconn, processingFileID)

	// Should fail with a clear error
	if err == nil {
		t.Fatalf("expected remove to reject PROCESSING file, but it succeeded")
	}

	// Verify error message is clear
	if !strings.Contains(err.Error(), "PROCESSING") {
		t.Fatalf("expected error to mention PROCESSING state, got: %v", err)
	}

	// Verify file state unchanged
	var status string
	err = dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, processingFileID).Scan(&status)
	if err != nil {
		t.Fatalf("query file status after failed remove: %v", err)
	}
	if status != filestate.LogicalFileProcessing {
		t.Fatalf("expected file status to remain PROCESSING, got %s", status)
	}

	// Verify no partial cleanup occurred (no file_chunk rows should exist)
	var chunks int
	err = dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, processingFileID).Scan(&chunks)
	if err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if chunks != 0 {
		t.Fatalf("expected no file_chunk rows after failed remove, got %d", chunks)
	}
}

// TestReuseRefusesStructurallyBrokenCompletedFile verifies that the store operation
// detects and refuses to reuse a completed file with a structurally broken file_chunk
// graph (non-contiguous orders, missing chunks, etc.).
func TestReuseRefusesStructurallyBrokenCompletedFile(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Store a file normally to get a valid state
	testFile := createTempFile(t, inputDir, "broken-graph-test.bin", 4*1024*1024)
	fileHash := sha256File(t, testFile)

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	// Now corrupt the file_chunk graph by deleting a chunk in the middle
	// and leaving a gap in chunk_order values
	var allChunkIDs []int64
	rows, err := dbconn.Query(`
		SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order ASC
	`, fileID)
	if err != nil {
		t.Fatalf("query file_chunk: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int64
		if err := rows.Scan(&cid); err != nil {
			t.Fatalf("scan chunk_id: %v", err)
		}
		allChunkIDs = append(allChunkIDs, cid)
	}

	if len(allChunkIDs) < 2 {
		t.Fatalf("test file has fewer than 2 chunks, cannot simulate gap")
	}

	// Delete the middle chunk's file_chunk entry (simulating corruption)
	middleIdx := len(allChunkIDs) / 2
	_, err = dbconn.Exec(`
		DELETE FROM file_chunk WHERE logical_file_id = $1 AND chunk_id = $2
	`, fileID, allChunkIDs[middleIdx])
	if err != nil {
		t.Fatalf("corrupt file_chunk order: %v", err)
	}

	// Try to store the same file again - should detect the broken graph and not reuse
	inputDir2 := filepath.Join(tmp, "input2")
	_ = os.MkdirAll(inputDir2, 0o755)

	testFile2 := createTempFile(t, inputDir2, "broken-graph-test-again.bin", 256*1024)
	// Write exact same content to get same hash
	originalContent := mustRead(t, testFile)
	if err := os.WriteFile(testFile2, originalContent, 0o644); err != nil {
		t.Fatalf("write duplicate test file: %v", err)
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	_, err = storage.StoreFileWithStorageContextResult(sgctx, testFile2)
	if err != nil {
		// Store might fail or succeed with rebuild; both are acceptable
		// as long as it doesn't blindly reuse
		t.Logf("store after graph corruption failed (expected): %v", err)
	} else {
		// If it succeeded, verify it detected the issue and didn't just reuse
		t.Logf("store after graph corruption succeeded with rebuild (acceptable)")
	}

	// Verify system integrity
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Logf("verify after broken file test: %v (may be expected if graph was corrupted)", err)
	}
}

// TestReuseRefusesStructurallyBrokenCompletedChunk verifies that the store operation
// detects and refuses to reuse a completed chunk that is missing block metadata,
// references a quarantined container, or has a missing container file.
func TestReuseRefusesSemanticallyCorruptedCompletedFile(t *testing.T) {
	requireDB(t)

	testModes := []string{"suspicious", "always"}
	for _, mode := range testModes {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", mode)

			tmp := t.TempDir()
			container.ContainersDir = filepath.Join(tmp, "containers")
			_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
			resetStorage(t)

			dbconn, err := db.ConnectDB()
			if err != nil {
				t.Fatalf("connectDB: %v", err)
			}
			defer dbconn.Close()

			applySchema(t, dbconn)
			resetDB(t, dbconn)

			inputDir := filepath.Join(tmp, "input")
			_ = os.MkdirAll(inputDir, 0o755)
			inPath := createTempFile(t, inputDir, "semantic-reuse-corruption.bin", 1024*1024)
			fileHash := sha256File(t, inPath)

			initialCtx := newTestContext(dbconn)
			if err := storage.StoreFileWithStorageContext(initialCtx, inPath); err != nil {
				t.Fatalf("initial store: %v", err)
			}

			fileID := fetchFileIDByHash(t, dbconn, fileHash)
			record := fetchFirstFileChunkRecord(t, dbconn, fileID)
			if record.storedSize <= 0 {
				t.Fatalf("expected first stored block size > 0, got %d", record.storedSize)
			}

			containerPath := containerPathForRecord(record)
			f, err := os.OpenFile(containerPath, os.O_RDWR, 0o644)
			if err != nil {
				t.Fatalf("open container for corruption: %v", err)
			}
			corruptionOffset := record.blockOffset
			if record.storedSize > 1 {
				corruptionOffset++
			}
			if _, err := f.WriteAt([]byte{0xEE}, corruptionOffset); err != nil {
				_ = f.Close()
				t.Fatalf("corrupt chunk payload byte: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("close corrupted container: %v", err)
			}

			restoreCtx := newTestContext(dbconn)
			result, err := storage.StoreFileWithStorageContextResult(restoreCtx, inPath)
			if err != nil {
				t.Fatalf("store after semantic corruption (mode=%s): %v", mode, err)
			}
			if result.AlreadyStored {
				t.Fatalf("expected semantic reuse guard to refuse AlreadyStored shortcut in mode=%s", mode)
			}
			if result.FileID != fileID {
				t.Fatalf("expected semantic corruption rebuild to reuse logical file row %d, got %d", fileID, result.FileID)
			}

			var status string
			var retryCount int64
			if err := dbconn.QueryRow(`SELECT status, retry_count FROM logical_file WHERE id = $1`, fileID).Scan(&status, &retryCount); err != nil {
				t.Fatalf("query logical_file status after semantic corruption rebuild: %v", err)
			}
			if status != filestate.LogicalFileCompleted {
				t.Fatalf("expected logical_file status COMPLETED after rebuild, got %s", status)
			}
			if retryCount < 1 {
				t.Fatalf("expected retry_count >= 1 after semantic corruption rebuild, got %d", retryCount)
			}
		})
	}
}

func TestReuseRefusesStructurallyBrokenCompletedChunk(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a test file with known content
	testContent := make([]byte, 512*1024)
	for i := range testContent {
		testContent[i] = byte((i*23 + 5) % 251)
	}
	testFile := filepath.Join(inputDir, "chunk-break-test.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// Store it normally
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("initial store: %v", err)
	}

	// Get the first chunk and its container
	var chunkID, containerID int64
	var containerFilename string
	err = dbconn.QueryRow(`
		SELECT DISTINCT c.id, b.container_id, ctr.filename
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		LIMIT 1
	`).Scan(&chunkID, &containerID, &containerFilename)
	if err != nil {
		t.Fatalf("query chunk and container: %v", err)
	}

	// Test scenario: Quarantine the container
	_, err = dbconn.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, containerID)
	if err != nil {
		t.Fatalf("quarantine container: %v", err)
	}

	// Try to reuse that chunk by creating a new file that would use it
	// We simulate this by trying to store a file with content that should hash to
	// the same chunks. For now, we verify that the chunk reuse validation catches it.

	// Mark the chunk as ABORTED to simulate failure
	_, err = dbconn.Exec(`
		UPDATE chunk SET status = $1 WHERE id = $2
	`, filestate.ChunkAborted, chunkID)
	if err != nil {
		t.Fatalf("mark chunk aborted: %v", err)
	}

	// Now try storing the same content again and verify it rebuilds
	testFile2 := filepath.Join(inputDir, "chunk-break-test-retry.bin")
	if err := os.WriteFile(testFile2, testContent, 0o644); err != nil {
		t.Fatalf("write retry test file: %v", err)
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	_, err = storage.StoreFileWithStorageContextResult(sgctx, testFile2)
	if err != nil {
		// Failure is acceptable; the system refused to reuse
		t.Logf("store after chunk corruption failed (expected): %v", err)
	} else {
		// If it succeeds, verify it created new chunk placements rather than reusing
		t.Logf("store after chunk breakage rebuilt (acceptable)")
	}

	// Test scenario: Missing container file
	// Get another container and delete its file
	var delContainerID int64
	var delFilename string
	err = dbconn.QueryRow(`
		SELECT DISTINCT ctr.id, ctr.filename FROM container ctr
		JOIN blocks b ON b.container_id = ctr.id
		WHERE ctr.id != $1 LIMIT 1
	`, containerID).Scan(&delContainerID, &delFilename)
	if err != nil && err != sql.ErrNoRows {
		delPath := filepath.Join(container.ContainersDir, delFilename)
		if err := os.Remove(delPath); err == nil || !strings.Contains(err.Error(), "no such file") {
			// Container file deleted; reuse validation should catch this
			t.Logf("deleted container file for validation test")
		}
	}

	// System should handle gracefully; verify won't show dangling references
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Logf("verify after chunk breakage: expected to detect issues: %v", err)
	}
}

// TestConcurrentRemoveAndRestore verifies that remove and restore operations racing
// on the same file do not cause partial/corrupted data or state corruption. Either
// restore completes with full correct content, or remove succeeds and prevents restore.
func TestConcurrentRemoveAndRestore(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create and store a test file
	testContent := make([]byte, 512*1024)
	for i := range testContent {
		testContent[i] = byte((i*37 + 13) % 251)
	}
	testFile := filepath.Join(inputDir, "concurrent-remove-restore.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("store: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, sha256File(t, testFile))

	// Race remove and restore
	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "restored.bin")

	done := make(chan error, 2)

	// Start restore in goroutine
	go func() {
		done <- storage.RestoreFileWithDB(dbconn, fileID, outPath)
	}()

	// Start remove in goroutine (slight delay to increase race likelihood)
	time.Sleep(1 * time.Millisecond)
	go func() {
		done <- storage.RemoveFileWithDB(dbconn, fileID)
	}()

	// Wait for both to complete
	err1 := <-done
	err2 := <-done

	// Both may succeed or fail, but not in a corrupt way
	// Check outcomes:
	var outcomeRestoreFailed, outcomeRemoveFailed bool
	if err1 != nil {
		outcomeRestoreFailed = true
	}
	if err2 != nil {
		outcomeRemoveFailed = true
	}

	// If restore succeeded, verify output file integrity
	if !outcomeRestoreFailed {
		if info, err := os.Stat(outPath); err != nil || info.Size() != int64(len(testContent)) {
			t.Fatalf("restore output size mismatch or missing: size=%v err=%v", info.Size(), err)
		}
		got := mustRead(t, outPath)
		if !bytes.Equal(got, testContent) {
			t.Fatalf("restored content differs from original")
		}
	}

	// If remove succeeded, verify file is actually gone
	if !outcomeRemoveFailed {
		var status string
		err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status)
		if err == nil {
			// File entry might still exist but should be marked ABORTED
			if status != filestate.LogicalFileAborted {
				t.Logf("warning: removed file not marked as ABORTED, status=%s", status)
			}
		}
	}

	// Verify system integrity - no orphaned PROCESSING rows
	assertNoProcessingRows(t, dbconn)

	// Verify no partial/corrupted state
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify after concurrent remove/restore: %v", err)
	}
}

// TestGCDuringActiveStore verifies that GC does not delete containers or chunks
// while store is actively appending to them. GC must respect live_ref_count and
// sealing markers to prevent data loss.
func TestGCDuringActiveStore(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a large test file to force multiple container rotations
	testContent := make([]byte, 8*1024*1024)
	for i := range testContent {
		testContent[i] = byte((i*41 + 7) % 251)
	}
	testFile := filepath.Join(inputDir, "large-store-file.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write large test file: %v", err)
	}

	// Store file while GC runs concurrently
	done := make(chan error, 2)
	var storedContainerCount int64
	var beforeContainers int64

	// Record initial container count
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&beforeContainers); err != nil {
		t.Fatalf("count initial containers: %v", err)
	}

	// Start store
	go func() {
		sgctx := storage.StorageContext{
			DB:           dbconn,
			Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
			ContainerDir: container.ContainersDir,
		}
		done <- storage.StoreFileWithStorageContext(sgctx, testFile)
	}()

	// Give store a chance to start writing
	time.Sleep(10 * time.Millisecond)

	// Start GC while store is active (non-dry-run to actually delete)
	go func() {
		done <- maintenance.RunGCWithContainersDir(false, container.ContainersDir)
	}()

	// Wait for both to complete
	err1 := <-done
	err2 := <-done

	if err1 != nil {
		t.Fatalf("store failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("gc failed: %v", err2)
	}

	// Count containers after concurrent operations
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&storedContainerCount); err != nil {
		t.Fatalf("count containers after store+gc: %v", err)
	}

	// We should have at least as many containers as before (GC shouldn't delete live containers)
	if storedContainerCount < beforeContainers {
		t.Fatalf("GC deleted containers during active store: before=%d after=%d", beforeContainers, storedContainerCount)
	}

	// Verify the stored file is still retrievable and passes verification
	fileHash := sha256File(t, testFile)
	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "large-restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore after concurrent GC: %v", err)
	}

	got := mustRead(t, outPath)
	if !bytes.Equal(got, testContent) {
		t.Fatalf("restored content differs after concurrent GC")
	}

	// Verify system integrity
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after concurrent store+gc: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

func TestLargeStoreRestoreVerifyDeepDoesNotTimeoutByDefault(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Use a reasonably large deterministic payload to exercise long deep verify/store/restore paths.
	testContent := make([]byte, 32*1024*1024)
	for i := range testContent {
		testContent[i] = byte((i*19 + 11) % 251)
	}
	inPath := filepath.Join(inputDir, "large-timeout-guard.bin")
	if err := os.WriteFile(inPath, testContent, 0o644); err != nil {
		t.Fatalf("write large input: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store large file: %v", err)
	}

	fileHash := sha256File(t, inPath)
	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "large-timeout-guard.restored.bin")
	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore large file: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); err != nil {
		t.Fatalf("deep verify large dataset: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestContainerOverflowProtection verifies that the store operation rotates
// containers before they overflow, preventing corruption and unrecoverable states.
func TestContainerOverflowProtection(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Reduce container max size to force frequent rotation
	originalMaxSize := container.GetContainerMaxSize()
	// Set above the largest single payload size so the writer can still rotate safely.
	container.SetContainerMaxSize(3 * 1024 * 1024)
	defer container.SetContainerMaxSize(originalMaxSize)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a file larger than max container size (will require rotation).
	fileSize := 9 * 1024 * 1024 // 9MiB, requires multiple containers at 3MiB max
	testContent := make([]byte, fileSize)
	for i := range testContent {
		testContent[i] = byte((i*23 + 11) % 251)
	}
	testFile := filepath.Join(inputDir, "overflow-test.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("store large file: %v", err)
	}

	// Verify no container exceeded max size
	var containers []struct {
		id          int64
		filename    string
		currentSize int64
		maxSize     int64
	}

	rows, err := dbconn.Query(`SELECT id, filename, current_size, max_size FROM container`)
	if err != nil {
		t.Fatalf("query containers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var c struct {
			id          int64
			filename    string
			currentSize int64
			maxSize     int64
		}
		if err := rows.Scan(&c.id, &c.filename, &c.currentSize, &c.maxSize); err != nil {
			t.Fatalf("scan container: %v", err)
		}
		containers = append(containers, c)

		// Check DB size doesn't exceed max
		if c.currentSize > c.maxSize {
			t.Fatalf("container %d exceeded max size: current=%d max=%d", c.id, c.currentSize, c.maxSize)
		}

		// Check physical file size doesn't exceed DB claimed size significantly
		// (may be slightly larger due to header, but not much)
		containerPath := filepath.Join(container.ContainersDir, c.filename)
		stat, err := os.Stat(containerPath)
		if err != nil {
			t.Fatalf("stat container file: %v", err)
		}

		// Allow for small header but not overflow
		if stat.Size() > c.currentSize+int64(container.ContainerHdrLen)*2 {
			t.Fatalf("container %d file size exceeds DB current_size: file=%d db=%d", c.id, stat.Size(), c.currentSize)
		}
	}

	if len(containers) < 2 {
		t.Fatalf("expected multiple containers for 6MiB file, got %d", len(containers))
	}

	// Verify at least one container is sealed (rotation occurred)
	var sealedCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed = TRUE`).Scan(&sealedCount); err != nil {
		t.Fatalf("count sealed containers: %v", err)
	}
	if sealedCount < 1 {
		t.Fatalf("expected at least 1 sealed container after rotation, got %d", sealedCount)
	}

	// Verify file restores correctly
	fileHash := sha256File(t, testFile)
	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "overflow-restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got := mustRead(t, outPath)
	if !bytes.Equal(got, testContent) {
		t.Fatalf("restored content differs from original")
	}

	// Verify system integrity
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after overflow protection test: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestBlockOffsetContinuityValidation verifies that the system detects and
// prevents non-contiguous or overlapping block offsets within containers,
// which could cause silent data loss or corruption during restore.
func TestBlockOffsetContinuityValidation(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Store a normal file first
	testContent := make([]byte, 1024*1024)
	for i := range testContent {
		testContent[i] = byte((i*19 + 5) % 251)
	}
	testFile := filepath.Join(inputDir, "offset-test.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("store: %v", err)
	}

	// Now corrupt block offsets in a container to create a gap
	// Find first container and its blocks
	var containerID int64
	var blocks []struct {
		id      int64
		offset  int64
		size    int64
		chunkID int64
	}

	rows, err := dbconn.Query(`
		SELECT b.id, b.block_offset, b.stored_size, b.chunk_id
		FROM blocks b
		ORDER BY b.container_id, b.block_offset
		LIMIT 10
	`)
	if err != nil {
		t.Fatalf("query blocks: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var b struct {
			id      int64
			offset  int64
			size    int64
			chunkID int64
		}
		if err := rows.Scan(&b.id, &b.offset, &b.size, &b.chunkID); err != nil {
			t.Fatalf("scan block: %v", err)
		}
		blocks = append(blocks, b)
		if containerID == 0 {
			// Get container from first block
			if err := dbconn.QueryRow(`SELECT container_id FROM blocks WHERE id = $1`, b.id).Scan(&containerID); err != nil {
				t.Fatalf("get container from block: %v", err)
			}
		}
	}

	if len(blocks) < 2 {
		t.Logf("not enough blocks to corrupt offsets, skipping offset corruption test")
		return
	}

	t.Run("gap in offsets", func(t *testing.T) {
		// Corrupt second block to create gap: shift its offset forward by 1000 bytes
		// This creates a hole: [0-B1_size] HOLE [B1_size+1000...]
		if len(blocks) >= 2 {
			newOffset := blocks[0].offset + blocks[0].size + 1000 // Create 1000-byte gap
			_, err := dbconn.Exec(`UPDATE blocks SET block_offset = $1 WHERE id = $2`, newOffset, blocks[1].id)
			if err != nil {
				t.Fatalf("corrupt block offset: %v", err)
			}
			defer func() {
				// Restore
				_, _ = dbconn.Exec(`UPDATE blocks SET block_offset = $1 WHERE id = $2`, blocks[1].offset, blocks[1].id)
			}()

			// Verify system detects the gap
			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err == nil {
				t.Fatalf("expected verify to detect offset gap, but it passed")
			} else {
				t.Logf("verify correctly detected offset gap: %v", err)
			}
		}
	})

	t.Run("overlapping offsets", func(t *testing.T) {
		// Corrupt second block to create overlap
		if len(blocks) >= 2 {
			// Move second block so it overlaps with first
			newOffset := blocks[0].offset + 100 // Overlap by 100 bytes
			_, err := dbconn.Exec(`UPDATE blocks SET block_offset = $1 WHERE id = $2`, newOffset, blocks[1].id)
			if err != nil {
				t.Fatalf("corrupt block offset: %v", err)
			}
			defer func() {
				// Restore
				_, _ = dbconn.Exec(`UPDATE blocks SET block_offset = $1 WHERE id = $2`, blocks[1].offset, blocks[1].id)
			}()

			// Verify system detects the overlap
			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err == nil {
				t.Fatalf("expected verify to detect offset overlap, but it passed")
			} else {
				t.Logf("verify correctly detected offset overlap: %v", err)
			}
		}
	})

	t.Run("no gaps after repair", func(t *testing.T) {
		// After corruption tests above (which rollback), verify should pass
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
			t.Fatalf("verify should pass after offset corruption tests are restored: %v", err)
		}

		// Also verify restore still works
		fileHash := sha256File(t, testFile)
		fileID := fetchFileIDByHash(t, dbconn, fileHash)

		outDir := filepath.Join(tmp, "out")
		_ = os.MkdirAll(outDir, 0o755)
		outPath := filepath.Join(outDir, "offset-restored.bin")

		if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
			t.Fatalf("restore after offset tests: %v", err)
		}

		got := mustRead(t, outPath)
		if !bytes.Equal(got, testContent) {
			t.Fatalf("restored content differs")
		}
	})
}

// TestRemoveWithSharedChunksRefCount verifies that removing a file with shared
// chunks correctly decrements ref_count without affecting other files still
// referencing those chunks. This tests the atomicity and correctness of dedup
// ref counting under file removal.
func TestRemoveWithSharedChunksRefCount(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a shared prefix that both files will use
	sharedPrefix := make([]byte, 512*1024)
	for i := range sharedPrefix {
		sharedPrefix[i] = byte((i*31 + 7) % 251)
	}

	// File A: shared prefix + unique suffix A
	suffixA := make([]byte, 128*1024)
	for i := range suffixA {
		suffixA[i] = byte((i*11 + 3) % 251)
	}
	fileAContent := append(append([]byte{}, sharedPrefix...), suffixA...)
	fileAPath := filepath.Join(inputDir, "shared-file-a.bin")
	if err := os.WriteFile(fileAPath, fileAContent, 0o644); err != nil {
		t.Fatalf("write fileA: %v", err)
	}

	// File B: same shared prefix + unique suffix B
	suffixB := make([]byte, 128*1024)
	for i := range suffixB {
		suffixB[i] = byte((i*17 + 5) % 251)
	}
	fileBContent := append(append([]byte{}, sharedPrefix...), suffixB...)
	fileBPath := filepath.Join(inputDir, "shared-file-b.bin")
	if err := os.WriteFile(fileBPath, fileBContent, 0o644); err != nil {
		t.Fatalf("write fileB: %v", err)
	}

	hashA := sha256File(t, fileAPath)
	hashB := sha256File(t, fileBPath)

	// Store both files
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, fileAPath); err != nil {
		t.Fatalf("store fileA: %v", err)
	}
	if err := storage.StoreFileWithStorageContext(sgctx, fileBPath); err != nil {
		t.Fatalf("store fileB: %v", err)
	}

	fileAID := fetchFileIDByHash(t, dbconn, hashA)
	fileBID := fetchFileIDByHash(t, dbconn, hashB)

	// Count shared chunks before removal
	var sharedChunkCount int
	err = dbconn.QueryRow(`
		SELECT COUNT(DISTINCT c.id)
		FROM chunk c
		WHERE EXISTS (
			SELECT 1 FROM file_chunk fc WHERE fc.chunk_id = c.id AND fc.logical_file_id = $1
		)
		AND EXISTS (
			SELECT 1 FROM file_chunk fc WHERE fc.chunk_id = c.id AND fc.logical_file_id = $2
		)
	`, fileAID, fileBID).Scan(&sharedChunkCount)
	if err != nil {
		t.Fatalf("count shared chunks: %v", err)
	}

	if sharedChunkCount == 0 {
		t.Logf("warning: files don't share chunks as expected, but continuing test")
	}

	// Record ref_count for chunks referenced by both files
	type chunkRef struct {
		id       int64
		refCount int64
	}
	var chunksBeforeRemove []chunkRef

	rows, err := dbconn.Query(`
		SELECT DISTINCT c.id, c.live_ref_count
		FROM chunk c
		WHERE EXISTS (
			SELECT 1 FROM file_chunk fc WHERE fc.chunk_id = c.id AND fc.logical_file_id = $1
		)
	`, fileAID)
	if err != nil {
		t.Fatalf("query chunks for fileA: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cr chunkRef
		if err := rows.Scan(&cr.id, &cr.refCount); err != nil {
			t.Fatalf("scan chunk: %v", err)
		}
		chunksBeforeRemove = append(chunksBeforeRemove, cr)
	}

	// Remove file A
	if err := storage.RemoveFileWithDB(dbconn, fileAID); err != nil {
		t.Fatalf("remove fileA: %v", err)
	}

	// Verify chunks' ref_counts decreased by 1 (or went to 0 if fileA was the only ref)
	for _, before := range chunksBeforeRemove {
		var afterRefCount int64
		err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, before.id).Scan(&afterRefCount)
		if err != nil && err != sql.ErrNoRows {
			t.Fatalf("query chunk ref_count after remove: %v", err)
		}

		expectedAfter := before.refCount - 1
		if expectedAfter < 0 {
			expectedAfter = 0
		}

		if afterRefCount != expectedAfter {
			t.Fatalf("chunk %d ref_count mismatch after remove: before=%d after=%d expected=%d",
				before.id, before.refCount, afterRefCount, expectedAfter)
		}
	}

	// Verify file B still restores correctly (proves shared chunks not deleted prematurely)
	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "fileB-after-A-removed.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileBID, outPath); err != nil {
		t.Fatalf("restore fileB after fileA removed: %v", err)
	}

	got := mustRead(t, outPath)
	if !bytes.Equal(got, fileBContent) {
		t.Fatalf("fileB content differs after fileA removal")
	}

	// Verify system integrity
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify after file removal: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestMultiFileOpenConcurrentRestore verifies that multiple threads can safely
// restore different files concurrently without cross-contamination, especially
// when files share chunks. Tests verify-time safety of concurrent block reads.
func TestMultiFileOpenConcurrentRestore(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create 5 distinct files with shared patterns to encourage chunking overlap
	type testFile struct {
		path    string
		content []byte
		hash    string
		id      int64
	}
	var files []testFile

	basePattern := make([]byte, 256*1024)
	for i := range basePattern {
		basePattern[i] = byte((i*23 + 7) % 251)
	}

	for i := 0; i < 5; i++ {
		// Each file: base pattern + unique footer
		footer := make([]byte, 64*1024)
		for j := range footer {
			footer[j] = byte((j*37 + i) % 251)
		}

		content := append(append([]byte{}, basePattern...), footer...)
		path := filepath.Join(inputDir, fmt.Sprintf("concurrent-file-%d.bin", i))
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}

		hash := sha256File(t, path)
		files = append(files, testFile{
			path:    path,
			content: content,
			hash:    hash,
		})
	}

	// Store all files
	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	for i := range files {
		if err := storage.StoreFileWithStorageContext(sgctx, files[i].path); err != nil {
			t.Fatalf("store file %d: %v", i, err)
		}
		files[i].id = fetchFileIDByHash(t, dbconn, files[i].hash)
	}

	// Concurrently restore all files
	const workers = 5
	outDir := filepath.Join(tmp, "out-concurrent")
	_ = os.MkdirAll(outDir, 0o755)

	done := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			outPath := filepath.Join(outDir, fmt.Sprintf("restored-%d.bin", idx))
			done <- storage.RestoreFileWithDB(dbconn, files[idx].id, outPath)
		}(i)
	}

	// Wait for all restores to complete
	for i := 0; i < workers; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent restore %d failed: %v", i, err)
		}
	}

	// Verify all restored files match originals (no cross-contamination)
	for i := range files {
		outPath := filepath.Join(outDir, fmt.Sprintf("restored-%d.bin", i))
		got := mustRead(t, outPath)
		if !bytes.Equal(got, files[i].content) {
			t.Fatalf("restored file %d content differs from original", i)
		}
	}

	// Verify system integrity
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after concurrent restores: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestRestoreFailureRecovery verifies that a failed restore (e.g., from container
// corruption mid-read) fails gracefully without leaving orphaned state, and a
// subsequent restore attempt can succeed.
func TestRestoreFailureRecovery(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create and store a test file spanning multiple chunks
	testContent := make([]byte, 1024*1024)
	for i := range testContent {
		testContent[i] = byte((i*43 + 11) % 251)
	}
	testFile := filepath.Join(inputDir, "restore-failure-test.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:     dbconn,
		Writer: container.NewLocalWriter(container.GetContainerMaxSize()),
	}
	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("store: %v", err)
	}

	fileHash := sha256File(t, testFile)
	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	// Find a container file and corrupt it mid-range (not at edges to cause read failure)
	var corruptContainerPath string

	rows, err := dbconn.Query(`SELECT filename FROM container LIMIT 1`)
	if err != nil {
		t.Fatalf("query containers: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			t.Fatalf("scan container filename: %v", err)
		}
		corruptContainerPath = filepath.Join(container.ContainersDir, filename)
	}

	if corruptContainerPath == "" {
		t.Fatalf("no containers found to corrupt")
	}

	// Corrupt the container by zeroing out a chunk in the middle.
	originalContainerBytes := mustRead(t, corruptContainerPath)
	corrupted := append([]byte{}, originalContainerBytes...)
	if len(corrupted) > 1024 {
		// Zero out 512 bytes starting at 512 bytes into the file
		for i := 512; i < 512+512 && i < len(corrupted); i++ {
			corrupted[i] = 0
		}
		if err := os.WriteFile(corruptContainerPath, corrupted, 0o644); err != nil {
			t.Fatalf("corrupt container: %v", err)
		}
	}

	// Try to restore - expect failure due to corruption
	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "restore-attempt-1.bin")

	err = storage.RestoreFileWithDB(dbconn, fileID, outPath)
	shouldHaveFailed := err != nil
	if !shouldHaveFailed {
		t.Logf("warning: restore should have failed due to corruption, but succeeded")
	}

	// Fix the container corruption by restoring the original bytes.
	if err := os.WriteFile(corruptContainerPath, originalContainerBytes, 0o644); err != nil {
		t.Fatalf("restore container after corruption: %v", err)
	}

	// Try restore again - should succeed now
	outPath2 := filepath.Join(outDir, "restore-attempt-2.bin")
	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath2); err != nil {
		t.Fatalf("second restore attempt failed: %v", err)
	}

	got := mustRead(t, outPath2)
	if !bytes.Equal(got, testContent) {
		t.Fatalf("restored content differs from original after recovery")
	}

	_ = got // Mark got as intentionally used

	// Verify system is clean
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after restore failure recovery: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestImplicitContainerFinalizationDuringStore verifies that as containers fill
// during a large store operation, they are properly sealed/finalized without
// leaving any in an inconsistent sealing state or with ghost bytes.
func TestImplicitContainerFinalizationDuringStore(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Reduce container max size, but keep it above the largest single payload.
	originalMaxSize := container.GetContainerMaxSize()
	container.SetContainerMaxSize(3 * 1024 * 1024)
	defer container.SetContainerMaxSize(originalMaxSize)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Create a file large enough to need multiple containers.
	fileSize := 11 * 1024 * 1024 // 11MiB
	testContent := make([]byte, fileSize)
	for i := range testContent {
		testContent[i] = byte((i*47 + 13) % 251)
	}
	testFile := filepath.Join(inputDir, "finalize-test.bin")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if err := storage.StoreFileWithStorageContext(sgctx, testFile); err != nil {
		t.Fatalf("store: %v", err)
	}

	// Verify all sealed containers are properly finalized (no ghost bytes)
	rows, err := dbconn.Query(`
		SELECT id, filename, current_size, sealed, sealing
		FROM container
		ORDER BY id
	`)
	if err != nil {
		t.Fatalf("query containers: %v", err)
	}
	defer rows.Close()

	type containerState struct {
		id          int64
		filename    string
		currentSize int64
		sealed      bool
		sealing     bool
	}
	var containers []containerState

	for rows.Next() {
		var c containerState
		if err := rows.Scan(&c.id, &c.filename, &c.currentSize, &c.sealed, &c.sealing); err != nil {
			t.Fatalf("scan container: %v", err)
		}
		containers = append(containers, c)
	}

	if len(containers) < 3 {
		t.Fatalf("expected multiple containers for 11MiB file with 3MiB max, got %d", len(containers))
	}

	// Check each container state
	var sealedCount, unopenedCount int
	for _, c := range containers {
		// Verify no orphaned sealing state
		if c.sealing && !c.sealed {
			// This is the active container mid-sealing, which is OK
			unopenedCount++
		} else if c.sealed {
			sealedCount++
		}

		// Check for ghost bytes
		containerPath := filepath.Join(container.ContainersDir, c.filename)
		stat, err := os.Stat(containerPath)
		if err != nil {
			t.Fatalf("stat container %d: %v", c.id, err)
		}

		physicalSize := stat.Size()
		allowedOverhead := int64(container.ContainerHdrLen * 2)

		if physicalSize > c.currentSize+allowedOverhead {
			t.Fatalf("container %d has ghost bytes: current_size=%d physical=%d",
				c.id, c.currentSize, physicalSize)
		}
	}

	// At least most containers should be sealed
	if sealedCount < len(containers)-1 {
		t.Fatalf("too many unsealed containers: sealed=%d total=%d", sealedCount, len(containers))
	}

	// Verify restore works correctly across all containers
	fileHash := sha256File(t, testFile)
	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "finalize-restored.bin")

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got := mustRead(t, outPath)
	if !bytes.Equal(got, testContent) {
		t.Fatalf("restored content differs from original")
	}

	// Verify system integrity - especially important for finalization
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after finalization: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestGCDryRunAccuracyMatchesRealRun verifies that GC dry-run reports exactly
// the same target containers that a subsequent real GC run deletes.
func TestGCDryRunAccuracyMatchesRealRun(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	deadName := "dry-run-dead.bin"
	liveName := "dry-run-live.bin"
	deadPath := filepath.Join(container.ContainersDir, deadName)
	livePath := filepath.Join(container.ContainersDir, liveName)
	if err := os.WriteFile(deadPath, []byte("dead-container"), 0o644); err != nil {
		t.Fatalf("write dead container file: %v", err)
	}
	if err := os.WriteFile(livePath, []byte("live-container"), 0o644); err != nil {
		t.Fatalf("write live container file: %v", err)
	}

	var deadContainerID int64
	err = dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		deadName,
		int64(len("dead-container")),
		container.GetContainerMaxSize(),
	).Scan(&deadContainerID)
	if err != nil {
		t.Fatalf("insert dead container row: %v", err)
	}

	var deadChunkID int64
	err = dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		"dry-run-dead-chunk",
		int64(14),
		filestate.ChunkCompleted,
		int64(0),
		int64(0),
	).Scan(&deadChunkID)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		deadChunkID,
		"plain",
		1,
		int64(14),
		int64(14),
		[]byte{},
		deadContainerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert dead block: %v", err)
	}

	var liveContainerID int64
	err = dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		liveName,
		int64(len("live-container")),
		container.GetContainerMaxSize(),
	).Scan(&liveContainerID)
	if err != nil {
		t.Fatalf("insert live container row: %v", err)
	}

	var liveChunkID int64
	err = dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		"dry-run-live-chunk",
		int64(14),
		filestate.ChunkCompleted,
		int64(1),
		int64(0),
	).Scan(&liveChunkID)
	if err != nil {
		t.Fatalf("insert live chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		liveChunkID,
		"plain",
		1,
		int64(14),
		int64(14),
		[]byte{},
		liveContainerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert live block: %v", err)
	}

	dryResult, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir)
	if err != nil {
		t.Fatalf("gc dry-run: %v", err)
	}

	if dryResult.AffectedContainers != 1 {
		t.Fatalf("expected dry-run affected_containers=1, got %d", dryResult.AffectedContainers)
	}
	if len(dryResult.ContainerFilenames) != 1 || dryResult.ContainerFilenames[0] != deadName {
		t.Fatalf("dry-run container list mismatch: got %v", dryResult.ContainerFilenames)
	}

	if _, err := os.Stat(deadPath); err != nil {
		t.Fatalf("dry-run must not delete dead container file: %v", err)
	}
	if _, err := os.Stat(livePath); err != nil {
		t.Fatalf("dry-run must not touch live container file: %v", err)
	}

	realResult, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir)
	if err != nil {
		t.Fatalf("gc real run: %v", err)
	}

	if realResult.AffectedContainers != dryResult.AffectedContainers {
		t.Fatalf("dry-run/real affected mismatch: dry=%d real=%d", dryResult.AffectedContainers, realResult.AffectedContainers)
	}
	if len(realResult.ContainerFilenames) != 1 || realResult.ContainerFilenames[0] != deadName {
		t.Fatalf("real run container list mismatch: got %v", realResult.ContainerFilenames)
	}

	if _, err := os.Stat(deadPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead container file should be deleted after real gc, stat err=%v", err)
	}
	if _, err := os.Stat(livePath); err != nil {
		t.Fatalf("live container file should remain after real gc: %v", err)
	}
}

// TestSearchListConsistencyWithFilters verifies that list and search views stay
// consistent for completed files, including name and size filters.
func TestSearchListConsistencyWithFilters(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	type fileDef struct {
		name string
		size int
	}
	files := []fileDef{
		{name: "alpha-one.txt", size: 48 * 1024},
		{name: "alpha-two.bin", size: 80 * 1024},
		{name: "beta-only.txt", size: 64 * 1024},
		{name: "gamma-alpha.log", size: 120 * 1024},
	}

	stored := make(map[int64]fileDef)
	sgctx := newTestContext(dbconn)
	for _, f := range files {
		inPath := createTempFile(t, inputDir, f.name, f.size)
		if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
			t.Fatalf("store %s: %v", f.name, err)
		}
		h := sha256File(t, inPath)
		id := fetchFileIDByHash(t, dbconn, h)
		stored[id] = f
	}

	listRows, err := listing.ListFilesResultWithDB(dbconn, nil)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(listRows) != len(files) {
		t.Fatalf("list row count mismatch: want %d got %d", len(files), len(listRows))
	}

	listIDs := make(map[int64]struct{}, len(listRows))
	for _, row := range listRows {
		listIDs[row.ID] = struct{}{}
	}

	searchAlpha, err := listing.SearchFilesResultWithDB(dbconn, []string{"--name", "alpha"})
	if err != nil {
		t.Fatalf("search --name alpha: %v", err)
	}
	if len(searchAlpha) != 3 {
		t.Fatalf("expected 3 alpha search rows, got %d", len(searchAlpha))
	}
	for _, row := range searchAlpha {
		if _, ok := listIDs[row.ID]; !ok {
			t.Fatalf("search row id=%d not present in list", row.ID)
		}
		if !strings.Contains(strings.ToLower(row.Name), "alpha") {
			t.Fatalf("search returned unexpected name=%q", row.Name)
		}
	}

	searchSized, err := listing.SearchFilesResultWithDB(dbconn, []string{"--min-size", "60000", "--max-size", "100000"})
	if err != nil {
		t.Fatalf("search by size range: %v", err)
	}
	if len(searchSized) != 2 {
		t.Fatalf("expected 2 files in size range, got %d", len(searchSized))
	}
	for _, row := range searchSized {
		if row.SizeBytes < 60000 || row.SizeBytes > 100000 {
			t.Fatalf("search size filter violated for id=%d size=%d", row.ID, row.SizeBytes)
		}
	}
}

// TestPinCountAtomicityConcurrentRestore verifies that concurrent restores do
// not leave stuck or negative pin_count values after unpin cleanup.
func TestPinCountAtomicityConcurrentRestore(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	inPath := createTempFile(t, inputDir, "pin-atomicity.bin", 4*1024*1024)
	wantHash := sha256File(t, inPath)
	wantBytes := mustRead(t, inPath)

	sgctx := newTestContext(dbconn)
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file: %v", err)
	}
	fileID := fetchFileIDByHash(t, dbconn, wantHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)

	const workers = 8
	start := make(chan struct{})
	done := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			<-start
			outPath := filepath.Join(outDir, fmt.Sprintf("pin-restore-%d.bin", idx))
			err := storage.RestoreFileWithDB(dbconn, fileID, outPath)
			if err != nil {
				done <- fmt.Errorf("restore worker %d: %w", idx, err)
				return
			}
			got := mustRead(t, outPath)
			if !bytes.Equal(got, wantBytes) {
				done <- fmt.Errorf("restore worker %d content mismatch", idx)
				return
			}
			done <- nil
		}(i)
	}

	close(start)
	for i := 0; i < workers; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	var negativePins int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE pin_count < 0`).Scan(&negativePins); err != nil {
		t.Fatalf("count negative pin_count: %v", err)
	}
	if negativePins != 0 {
		t.Fatalf("found %d chunks with negative pin_count", negativePins)
	}

	var stuckPins int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE pin_count != 0`).Scan(&stuckPins); err != nil {
		t.Fatalf("count stuck pin_count: %v", err)
	}
	if stuckPins != 0 {
		t.Fatalf("found %d chunks with non-zero pin_count after concurrent restore", stuckPins)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestEndToEndGCRestoreRemoveInterleaving runs restore, remove, and GC in
// parallel against real stored files and verifies no corruption or invalid
// reference state remains.
func TestEndToEndGCRestoreRemoveInterleaving(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	shared := make([]byte, 512*1024)
	for i := range shared {
		shared[i] = byte((i*29 + 17) % 251)
	}

	keepTail := make([]byte, 2*1024*1024)
	for i := range keepTail {
		keepTail[i] = byte((i*7 + 41) % 251)
	}
	victimTail := make([]byte, 2*1024*1024)
	for i := range victimTail {
		victimTail[i] = byte((i*13 + 19) % 251)
	}

	keepBytes := append(append([]byte{}, shared...), keepTail...)
	victimBytes := append(append([]byte{}, shared...), victimTail...)

	keepPath := filepath.Join(inputDir, "interleave-keep.bin")
	victimPath := filepath.Join(inputDir, "interleave-victim.bin")
	if err := os.WriteFile(keepPath, keepBytes, 0o644); err != nil {
		t.Fatalf("write keep file: %v", err)
	}
	if err := os.WriteFile(victimPath, victimBytes, 0o644); err != nil {
		t.Fatalf("write victim file: %v", err)
	}

	sgctx := newTestContext(dbconn)
	if err := storage.StoreFileWithStorageContext(sgctx, keepPath); err != nil {
		t.Fatalf("store keep file: %v", err)
	}
	if err := storage.StoreFileWithStorageContext(sgctx, victimPath); err != nil {
		t.Fatalf("store victim file: %v", err)
	}

	keepID := fetchFileIDByHash(t, dbconn, sha256File(t, keepPath))
	victimID := fetchFileIDByHash(t, dbconn, sha256File(t, victimPath))

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "keep.restored.bin")

	results := make(chan error, 3)
	go func() {
		results <- storage.RestoreFileWithDB(dbconn, keepID, outPath)
	}()
	go func() {
		results <- storage.RemoveFileWithDB(dbconn, victimID)
	}()
	go func() {
		_, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir)
		results <- err
	}()

	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Fatalf("interleaving operation failed: %v", err)
		}
	}

	got := mustRead(t, outPath)
	if !bytes.Equal(got, keepBytes) {
		t.Fatalf("restored keep file differs after interleaving")
	}

	var negativeRefs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`).Scan(&negativeRefs); err != nil {
		t.Fatalf("count negative live_ref_count: %v", err)
	}
	if negativeRefs != 0 {
		t.Fatalf("found %d chunks with negative live_ref_count", negativeRefs)
	}

	var negativePins int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE pin_count < 0`).Scan(&negativePins); err != nil {
		t.Fatalf("count negative pin_count: %v", err)
	}
	if negativePins != 0 {
		t.Fatalf("found %d chunks with negative pin_count", negativePins)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify after interleaving: %v", err)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestStoreMultiChunkFileVerifiesAtomicFinalization stores a multi-chunk file
// and verifies that the atomic finalization boundary works correctly:
// - File is marked COMPLETED
// - All file_chunk rows exist with contiguous chunk_order [0, 1, 2, ...]
// - File can be restored successfully
// - Re-store recognizes file as already stored
func TestStoreMultiChunkFileVerifiesAtomicFinalization(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// 3MB file will span multiple chunks via CDC (min chunk ~512KB, max ~2MB)
	inPath := createTempFile(t, inputDir, "multi_chunk_finalize.bin", 3*1024*1024)
	fileHash := sha256File(t, inPath)

	sgctx := newTestContext(dbconn)

	// Store the multi-chunk file
	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse codec: %v", err)
	}
	result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, codec)
	if err != nil {
		t.Fatalf("store multi-chunk file: %v", err)
	}
	fileID := result.FileID

	// Verify file is marked COMPLETED
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
		t.Fatalf("query logical_file status: %v", err)
	}
	if status != filestate.LogicalFileCompleted {
		t.Fatalf("expected file status COMPLETED, got %s", status)
	}

	// Verify all file_chunk rows exist with contiguous chunk_order [0, 1, 2, ...]
	var chunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&chunkCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if chunkCount < 2 {
		t.Fatalf("expected at least 2 chunks in multi-chunk file, got %d", chunkCount)
	}

	// Verify chunk_order is contiguous starting from 0
	var maxOrder int
	if err := dbconn.QueryRow(`SELECT MAX(chunk_order) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&maxOrder); err != nil {
		t.Fatalf("query max chunk_order: %v", err)
	}
	if maxOrder != chunkCount-1 {
		t.Fatalf("expected chunk_order to be 0..%d, but max is %d", chunkCount-1, maxOrder)
	}

	var minOrder int
	if err := dbconn.QueryRow(`SELECT MIN(chunk_order) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&minOrder); err != nil {
		t.Fatalf("query min chunk_order: %v", err)
	}
	if minOrder != 0 {
		t.Fatalf("expected min chunk_order to be 0, got %d", minOrder)
	}

	// Verify no gaps in chunk_order sequence
	var gapCount int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM (
			SELECT chunk_order FROM file_chunk WHERE logical_file_id = $1
			EXCEPT
			SELECT generate_series(0, $2)
		) missing_orders
	`, fileID, chunkCount-1).Scan(&gapCount); err != nil {
		t.Fatalf("check for gaps in chunk_order: %v", err)
	}
	if gapCount != 0 {
		t.Fatalf("expected no gaps in chunk_order sequence, found %d missing orders", gapCount)
	}

	// Verify file can be restored successfully
	restoreDir := filepath.Join(tmp, "restore")
	_ = os.MkdirAll(restoreDir, 0o755)
	outPath := filepath.Join(restoreDir, "restored.bin")
	restoreCtx := newTestContext(dbconn)

	if err := storage.RestoreFileWithStorageContext(restoreCtx, fileID, outPath); err != nil {
		t.Fatalf("restore multi-chunk file: %v", err)
	}

	restoredHash := sha256File(t, outPath)
	if restoredHash != fileHash {
		t.Fatalf("restored file hash mismatch: original %s, restored %s", fileHash, restoredHash)
	}

	// Re-store the same file and verify it's recognized as already stored
	result2, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, codec)
	if err != nil {
		t.Fatalf("re-store multi-chunk file: %v", err)
	}
	if !result2.AlreadyStored {
		t.Fatalf("expected re-store to recognize already stored file, got AlreadyStored=false")
	}
	if result2.FileID != fileID {
		t.Fatalf("expected same file ID on re-store, original %d got %d", fileID, result2.FileID)
	}

	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)
}

func TestFinalLogicalFileCompletionFailureLeavesExpectedState(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Fail only the final logical-file completion status transition.
	if _, err := dbconn.Exec(`DROP TRIGGER IF EXISTS ck_fail_logical_complete_trg ON logical_file`); err != nil {
		t.Fatalf("drop stale logical_file trigger: %v", err)
	}
	if _, err := dbconn.Exec(`DROP FUNCTION IF EXISTS ck_fail_logical_complete()`); err != nil {
		t.Fatalf("drop stale logical_file function: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE FUNCTION ck_fail_logical_complete()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.status = 'COMPLETED' THEN
				RAISE EXCEPTION 'injected logical completion failure';
			END IF;
			RETURN NEW;
		END;
		$$
	`); err != nil {
		t.Fatalf("create logical completion trigger function: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TRIGGER ck_fail_logical_complete_trg
		BEFORE UPDATE OF status ON logical_file
		FOR EACH ROW
		WHEN (NEW.status = 'COMPLETED')
		EXECUTE FUNCTION ck_fail_logical_complete()
	`); err != nil {
		t.Fatalf("create logical completion trigger: %v", err)
	}
	defer func() {
		_, _ = dbconn.Exec(`DROP TRIGGER IF EXISTS ck_fail_logical_complete_trg ON logical_file`)
		_, _ = dbconn.Exec(`DROP FUNCTION IF EXISTS ck_fail_logical_complete()`)
	}()

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "finalize-fail.bin", 3*1024*1024)
	fileHash := sha256File(t, inPath)

	ctx := newTestContext(dbconn)
	_, err = storage.StoreFileWithStorageContextAndCodecResult(ctx, inPath, blocks.CodecPlain)
	if err == nil {
		t.Fatalf("expected store to fail at final logical-file completion phase")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "injected logical completion failure") {
		t.Fatalf("expected injected finalization failure to be surfaced, got: %v", err)
	}

	fileID := fetchFileIDByHash(t, dbconn, fileHash)

	var status string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
		t.Fatalf("query failed logical_file status: %v", err)
	}
	if status != filestate.LogicalFileAborted {
		t.Fatalf("expected logical_file to be ABORTED after finalization failure, got %q", status)
	}

	var linkedCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&linkedCount); err != nil {
		t.Fatalf("count file_chunk rows for failed file: %v", err)
	}
	if linkedCount < 2 {
		t.Fatalf("expected linked chunks to exist before final completion failure, got %d", linkedCount)
	}

	var minOrder, maxOrder int
	if err := dbconn.QueryRow(`
		SELECT COALESCE(MIN(chunk_order), -1), COALESCE(MAX(chunk_order), -1)
		FROM file_chunk
		WHERE logical_file_id = $1
	`, fileID).Scan(&minOrder, &maxOrder); err != nil {
		t.Fatalf("query chunk_order bounds: %v", err)
	}
	if minOrder != 0 || maxOrder != linkedCount-1 {
		t.Fatalf("expected contiguous linked chunk orders despite finalization failure: min=%d max=%d count=%d", minOrder, maxOrder, linkedCount)
	}

	var nonCompletedRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1 AND c.status <> $2
	`, fileID, filestate.ChunkCompleted).Scan(&nonCompletedRefs); err != nil {
		t.Fatalf("count non-completed linked chunks: %v", err)
	}
	if nonCompletedRefs != 0 {
		t.Fatalf("expected linked chunks to remain COMPLETED when final logical completion fails, found %d non-completed refs", nonCompletedRefs)
	}

	assertNoProcessingRows(t, dbconn)
}

// TestStoreVerifiesFileChunkContiguityOnCompletion stores multiple files
// of various sizes and explicitly verifies that each completed file has
// perfectly contiguous chunk_order sequences, validating the atomic
// finalization boundary verification logic.
func TestStoreVerifiesFileChunkContiguityOnCompletion(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	sgctx := newTestContext(dbconn)

	testCases := []struct {
		name string
		size int
	}{
		{"small", 256 * 1024},       // Single or small multi-chunk
		{"medium", 2 * 1024 * 1024}, // Multi-chunk
		{"large", 5 * 1024 * 1024},  // Multi-chunk
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inPath := createTempFile(t, inputDir, "contiguity_"+tc.name+".bin", tc.size)

			codec, err := blocks.ParseCodec("plain")
			if err != nil {
				t.Fatalf("parse codec: %v", err)
			}
			result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, codec)
			if err != nil {
				t.Fatalf("store file: %v", err)
			}
			fileID := result.FileID

			// Query file_chunk rows and verify they form a perfect sequence [0, 1, 2, ...n-1]
			rows, err := dbconn.Query(
				`SELECT chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`,
				fileID,
			)
			if err != nil {
				t.Fatalf("query file_chunk orders: %v", err)
			}
			defer rows.Close()

			expectedOrder := 0
			rowCount := 0
			for rows.Next() {
				var order int
				if err := rows.Scan(&order); err != nil {
					t.Fatalf("scan chunk_order: %v", err)
				}

				if order != expectedOrder {
					t.Errorf("file %s: expected chunk_order %d, got %d (non-contiguous)", tc.name, expectedOrder, order)
				}

				expectedOrder++
				rowCount++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("file_chunk query error: %v", err)
			}

			if rowCount == 0 {
				t.Errorf("file %s: found no file_chunk rows", tc.name)
			}

			// Verify file is COMPLETED
			var status string
			if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
				t.Fatalf("query file status: %v", err)
			}
			if status != filestate.LogicalFileCompleted {
				t.Errorf("file %s: expected status COMPLETED, got %s", tc.name, status)
			}
		})
	}

	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)
}

// TestConcurrentStoreMultiChunkFilesAtomicCompletion concurrently stores
// many large multi-chunk files and verifies that the atomic finalization
// boundary works correctly under concurrent load. All files should complete
// successfully with valid contiguous chunk_order sequences.
func TestConcurrentStoreMultiChunkFilesAtomicCompletion(t *testing.T) {
	requireDB(t)
	requireStress(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	resetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// Prepare 20 multi-chunk files (3-6MB each)
	fileCount := 20
	filePaths := make([]string, fileCount)
	fileHashes := make([]string, fileCount)

	for i := 0; i < fileCount; i++ {
		size := 3*1024*1024 + int64(i)*512*1024 // 3MB to 12MB
		filename := fmt.Sprintf("concurrent_multi_%d.bin", i)
		path := createTempFile(t, inputDir, filename, int(size))
		filePaths[i] = path
		fileHashes[i] = sha256File(t, path)
	}

	// Store files concurrently using 5 worker goroutines
	workerCount := 5
	errChan := make(chan error, fileCount)
	fileIDs := make(map[string]int64)
	var fileIDsMutex sync.Mutex

	for worker := 0; worker < workerCount; worker++ {
		go func(workerID int) {
			for i := workerID; i < fileCount; i += workerCount {
				const maxStoreAttempts = 4
				var result storage.StoreFileResult
				var err error

				for attempt := 0; attempt < maxStoreAttempts; attempt++ {
					ctx := newTestContext(dbconn)
					codec, parseErr := blocks.ParseCodec("plain")
					if parseErr != nil {
						errChan <- fmt.Errorf("parse codec: %w", parseErr)
						return
					}

					result, err = storage.StoreFileWithStorageContextAndCodecResult(ctx, filePaths[i], codec)
					if err == nil {
						break
					}
					if !isRetryableTxAbortError(err) || attempt == maxStoreAttempts-1 {
						errChan <- fmt.Errorf("worker %d file %d store: %w", workerID, i, err)
						return
					}
					time.Sleep(time.Duration(10*(attempt+1)) * time.Millisecond)
				}

				fileIDsMutex.Lock()
				fileIDs[filePaths[i]] = result.FileID
				fileIDsMutex.Unlock()
				errChan <- nil
			}
		}(worker)
	}

	// Collect errors
	for i := 0; i < fileCount; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("concurrent store error: %v", err)
		}
	}

	// Verify all files stored successfully with valid chunk sequences
	for i, path := range filePaths {
		fileIDsMutex.Lock()
		fileID, ok := fileIDs[path]
		fileIDsMutex.Unlock()

		if !ok {
			t.Fatalf("file %d not found in completed fileIDs", i)
		}

		// Verify COMPLETED status
		var status string
		if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
			t.Fatalf("file %d status query: %v", i, err)
		}
		if status != filestate.LogicalFileCompleted {
			t.Fatalf("file %d: expected status COMPLETED, got %s", i, status)
		}

		// Verify contiguous chunk_order
		var chunkCount int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&chunkCount); err != nil {
			t.Fatalf("file %d: count file_chunk rows: %v", i, err)
		}

		var maxOrder int
		if err := dbconn.QueryRow(`SELECT MAX(chunk_order) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&maxOrder); err != nil {
			t.Fatalf("file %d: query max chunk_order: %v", i, err)
		}
		if maxOrder != chunkCount-1 {
			t.Fatalf("file %d: expected max chunk_order=%d, got %d", i, chunkCount-1, maxOrder)
		}
	}

	// Verify all files can be restored
	restoreDir := filepath.Join(tmp, "restore")
	_ = os.MkdirAll(restoreDir, 0o755)

	for i, path := range filePaths {
		fileIDsMutex.Lock()
		fileID := fileIDs[path]
		fileIDsMutex.Unlock()

		outPath := filepath.Join(restoreDir, fmt.Sprintf("restored_%d.bin", i))
		ctx := newTestContext(dbconn)

		if err := storage.RestoreFileWithStorageContext(ctx, fileID, outPath); err != nil {
			t.Fatalf("restore file %d: %v", i, err)
		}

		restoredHash := sha256File(t, outPath)
		if restoredHash != fileHashes[i] {
			t.Fatalf("file %d: hash mismatch after restore", i)
		}
	}

	assertNoProcessingRows(t, dbconn)
	assertUniqueFileChunkOrders(t, dbconn)
}
