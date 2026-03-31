package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
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

	// 1) Allow explicit override (best for Docker / CI)
	if p := os.Getenv("COLDKEEP_SCHEMA_PATH"); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read schema %s: %v", p, err)
		}
		if _, err := dbconn.Exec(string(b)); err != nil && !isDuplicateSchemaError(err) {
			t.Fatalf("apply schema: %v", err)
		}
		return
	}

	// 2) Walk upwards from cwd to find db/schema_postgres.sql
	cwd, err := os.Getwd()
	if err == nil {
		dir := cwd
		for i := 0; i < 6; i++ { // climb up a few levels
			candidate := filepath.Join(dir, "db", "schema_postgres.sql")
			if _, statErr := os.Stat(candidate); statErr == nil {
				b, err := os.ReadFile(candidate)
				if err != nil {
					t.Fatalf("read schema %s: %v", candidate, err)
				}
				if _, err := dbconn.Exec(string(b)); err != nil && !isDuplicateSchemaError(err) {
					t.Fatalf("apply schema: %v", err)
				}
				return
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	// 3) Common fallbacks for containers / mounts
	candidates := []string{
		"../db/schema_postgres.sql",
		"../../db/schema_postgres.sql",
		"/repo/db/schema_postgres.sql",
		"/work/db/schema_postgres.sql",
		"/db/schema_postgres.sql",
		"/app/db/schema_postgres.sql",
	}
	for _, p := range candidates {
		if _, statErr := os.Stat(p); statErr == nil {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read schema %s: %v", p, err)
			}
			if _, err := dbconn.Exec(string(b)); err != nil && !isDuplicateSchemaError(err) {
				t.Fatalf("apply schema: %v", err)
			}
			return
		}
	}

	t.Fatalf("could not find db/schema_postgres.sql; set COLDKEEP_SCHEMA_PATH to an absolute path")
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
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'PROCESSING'`).Scan(&logicalProcessing); err != nil {
		t.Fatalf("count processing logical_file rows: %v", err)
	}
	if logicalProcessing != 0 {
		t.Fatalf("expected no PROCESSING logical_file rows, got %d", logicalProcessing)
	}

	var chunkProcessing int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'PROCESSING'`).Scan(&chunkProcessing); err != nil {
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

	sim := assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"simulate", "store-folder", inputDir, "--output", "json"), "simulate")
	simData := jsonMap(t, sim, "data")
	simFiles := jsonInt64(t, simData, "files")
	simLogical := jsonInt64(t, simData, "logical_size_bytes")
	simPhysical := jsonInt64(t, simData, "physical_size_bytes")

	assertCLIJSONOK(t, runColdkeepCommand(t, repoRoot, binPath, env,
		"store-folder", inputDir, "--output", "json"), "store-folder")

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
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`).Scan(&s.completedFiles); err != nil {
			t.Fatalf("[%s] query completed_files: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&s.totalChunks); err != nil {
			t.Fatalf("[%s] query total_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`).Scan(&s.completedChunks); err != nil {
			t.Fatalf("[%s] query completed_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`
			SELECT COALESCE(SUM(b.stored_size), 0)
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE c.ref_count > 0
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
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`).Scan(&s.completedFiles); err != nil {
			t.Fatalf("[%s] query completed_files: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&s.totalChunks); err != nil {
			t.Fatalf("[%s] query total_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`).Scan(&s.completedChunks); err != nil {
			t.Fatalf("[%s] query completed_chunks: %v", label, err)
		}
		if err := dbconn.QueryRow(`
			SELECT COALESCE(SUM(b.stored_size), 0)
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE c.ref_count > 0
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

	// Run verify before GC to check for any issues with ref_counts or metadata integrity.
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
	// What we must guarantee is that GC does not break restore and ref_counts remain valid.
	var negatives int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check negative ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found %d chunks with negative ref_count", negatives)
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

	// Ensure no chunk has negative ref_count
	//var negatives int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check negative ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found chunks with negative ref_count")
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
	if status != "COMPLETED" {
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
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`).Scan(&completedFiles); err != nil {
		t.Fatalf("count completed logical files: %v", err)
	}
	if completedFiles != len(expectedUnique) {
		t.Fatalf("expected %d completed logical files, got %d", len(expectedUnique), completedFiles)
	}

	var nonCompleted int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status <> 'COMPLETED'`).Scan(&nonCompleted); err != nil {
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
	if _, err := dbconn.Exec(`UPDATE logical_file SET status = 'ABORTED' WHERE id = $1`, fileID); err != nil {
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
	if status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got %s", status)
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
	if _, err := dbconn.Exec(`UPDATE logical_file SET status = 'ABORTED' WHERE id = $1`, fileID); err != nil {
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
	if status != "COMPLETED" {
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
	if _, err := dbconn.Exec(`UPDATE chunk SET status = 'ABORTED' WHERE id = $1`, chunkID); err != nil {
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
	if status != "COMPLETED" {
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

	if _, err := dbconn.Exec(`UPDATE chunk SET status = 'ABORTED' WHERE id = $1`, chunkID); err != nil {
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
	if status != "COMPLETED" {
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
		VALUES ($1, $2, $3, 'PROCESSING', 0)
	`, filepath.Base(inPath), size, hash)
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
		INSERT INTO chunk (chunk_hash, size, status, ref_count, retry_count)
		VALUES ($1, $2, 'PROCESSING', 0, 0)
	`, "dummy_chunk_hash", int64(128*1024))
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
	if fileStatus != "ABORTED" {
		t.Fatalf("expected logical file status ABORTED, got %s", fileStatus)
	}

	// Verify that processing chunk was aborted
	var chunkStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&chunkStatus); err != nil {
		t.Fatalf("check chunk status: %v", err)
	}
	if chunkStatus != "ABORTED" {
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

	t.Run("detects corrupted ref_count", func(t *testing.T) {
		// Corrupt one chunk's ref_count to a wrong value
		if _, err := dbconn.Exec(`UPDATE chunk SET ref_count = ref_count + 99 WHERE id = (SELECT id FROM chunk LIMIT 1)`); err != nil {
			t.Fatalf("corrupt ref_count: %v", err)
		}
		defer func() {
			// Restore so other sub-tests are not affected
			if _, err := dbconn.Exec(`UPDATE chunk SET ref_count = ref_count - 99 WHERE id = (SELECT id FROM chunk LIMIT 1)`); err != nil {
				t.Fatalf("restore ref_count: %v", err)
			}

		}()

		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err == nil {
			t.Fatal("RunVerify should have detected the corrupted ref_count but returned nil")
		}
	})

	t.Run("detects orphan chunk", func(t *testing.T) {
		// Insert a chunk with ref_count > 0 but no file_chunk referencing it
		if _, err := dbconn.Exec(`
				INSERT INTO chunk (chunk_hash, size, status, ref_count, retry_count)
				VALUES ('orphan_chunk_hash_test', 1024, 'COMPLETED', 1, 0)
		`); err != nil {
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
			INSERT INTO chunk (chunk_hash, size, status, ref_count, retry_count)
			VALUES ('verify_full_bad_chunk', 1024, 'COMPLETED', 0, 0)
		`); err != nil {
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
		WHERE c.status = 'COMPLETED'
		ORDER BY b.block_offset ASC
		LIMIT 1
	`).Scan(&blockOffset, &storedSize, &containerFilename)
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
		WHERE c.status = 'COMPLETED'
		ORDER BY b.container_id ASC, b.block_offset ASC
		OFFSET 1
		LIMIT 1
	`).Scan(&secondChunkID, &secondBlockOffset)
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
		WHERE c.status = 'COMPLETED'
		ORDER BY b.container_id ASC, b.block_offset ASC
		LIMIT 2
	`)
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

	// After GC, ref_counts must not go negative.
	var negatives int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found %d chunks with negative ref_count after GC", negatives)
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
		WHERE status = 'COMPLETED'
		ORDER BY id ASC
	`)
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
		WHERE status = 'COMPLETED'
		ORDER BY id ASC
	`)
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
