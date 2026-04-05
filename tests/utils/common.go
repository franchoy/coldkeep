package testutils

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

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

func RequireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run integration tests")
	}
}

func RequireStress(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
}

func RequireLongRun(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping long-run test in -short mode")
	}
	if os.Getenv("COLDKEEP_LONG_RUN") == "" {
		t.Skip("Set COLDKEEP_LONG_RUN=1 to run long-run integration tests")
	}
}

type CLIExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func FindRepoRoot(t *testing.T) string {
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

func BuildColdkeepBinary(t *testing.T, repoRoot string) string {
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

func BuildCommandEnv(overrides map[string]string) []string {
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

func RunColdkeepCommand(t *testing.T, repoRoot, binPath string, env map[string]string, args ...string) CLIExecResult {
	t.Helper()

	cmd := exec.Command(binPath, args...)
	cmd.Dir = repoRoot
	cmd.Env = BuildCommandEnv(env)

	var Stdout bytes.Buffer
	var Stderr bytes.Buffer
	cmd.Stdout = &Stdout
	cmd.Stderr = &Stderr

	err := cmd.Run()
	if err == nil {
		return CLIExecResult{Stdout: Stdout.String(), Stderr: Stderr.String(), ExitCode: 0}
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		return CLIExecResult{Stdout: Stdout.String(), Stderr: Stderr.String(), ExitCode: exitErr.ExitCode()}
	}

	t.Fatalf("run coldkeep command %v: %v", args, err)
	return CLIExecResult{}
}

func TryParseLastJSONLine(output string) (map[string]any, bool) {
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

func ParseJSONLines(output string) []map[string]any {
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

func JSONMap(t *testing.T, payload map[string]any, key string) map[string]any {
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

func JSONInt64(t *testing.T, payload map[string]any, key string) int64 {
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

func AssertCLIJSONOK(t *testing.T, res CLIExecResult, command string) map[string]any {
	t.Helper()

	if res.ExitCode != 0 {
		t.Fatalf("command %s failed with exit=%d\nstdout:\n%s\nstderr:\n%s", command, res.ExitCode, res.Stdout, res.Stderr)
	}

	payload, ok := TryParseLastJSONLine(res.Stdout)
	if !ok {
		payload, ok = TryParseLastJSONLine(res.Stdout + "\n" + res.Stderr)
	}
	if !ok {
		t.Fatalf("command %s produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", command, res.Stdout, res.Stderr)
	}

	if status, _ := payload["status"].(string); status != "ok" {
		t.Fatalf("command %s did not return status=ok: payload=%v Stderr=%s", command, payload, res.Stderr)
	}
	if got, _ := payload["command"].(string); got != command {
		t.Fatalf("command mismatch: want %s got %q payload=%v", command, got, payload)
	}

	return payload
}

func FindCLIErrorPayload(output string) (map[string]any, bool) {
	for _, payload := range ParseJSONLines(output) {
		if _, ok := payload["error_class"]; ok {
			return payload, true
		}
	}
	return nil, false
}

func DefaultCLIEnv(storageDir string) map[string]string {
	codec := GetenvOrDefault("COLDKEEP_CODEC", "plain")
	env := map[string]string{
		"COLDKEEP_TEST_DB":     "1",
		"COLDKEEP_CODEC":       codec,
		"COLDKEEP_STORAGE_DIR": storageDir,
		"DB_HOST":              GetenvOrDefault("DB_HOST", "127.0.0.1"),
		"DB_PORT":              GetenvOrDefault("DB_PORT", "5432"),
		"DB_USER":              GetenvOrDefault("DB_USER", "coldkeep"),
		"DB_PASSWORD":          os.Getenv("DB_PASSWORD"),
		"DB_NAME":              GetenvOrDefault("DB_NAME", "coldkeep"),
		"DB_SSLMODE":           GetenvOrDefault("DB_SSLMODE", "disable"),
	}
	if codec == "aes-gcm" {
		env["COLDKEEP_KEY"] = GetenvOrDefault("COLDKEEP_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	}
	return env
}

func GetenvOrDefault(name, fallback string) string {
	v := os.Getenv(name)
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func ApplySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	// If schema already exists, reuse it.
	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}

	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil && !IsDuplicateSchemaError(err) {
		t.Fatalf("apply schema: %v", err)
	}
}

func IsDuplicateSchemaError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "42710")
}

func ResetDB(t *testing.T, dbconn *sql.DB) {
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

func ResetStorage(t *testing.T) {
	t.Helper()
	if container.ContainersDir == "" {
		t.Fatalf("ContainersDir is empty")
	}
	_ = os.RemoveAll(container.ContainersDir)
	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir ContainersDir: %v", err)
	}
}

func SHA256File(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func CreateTempFile(t *testing.T, dir, name string, Size int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	data := make([]byte, Size)

	// Deterministic content (repeatable).
	for i := 0; i < Size; i++ {
		data[i] = byte((i*31 + 7) % 251)
	}

	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}

func FetchFileIDByHash(t *testing.T, dbconn *sql.DB, fileHash string) int64 {
	t.Helper()
	var id int64
	err := dbconn.QueryRow(`SELECT id FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&id)
	if err != nil {
		t.Fatalf("query logical_file by Hash: %v", err)
	}
	return id
}

func AssertNoProcessingRows(t *testing.T, dbconn *sql.DB) {
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

func AssertUniqueFileChunkOrders(t *testing.T, dbconn *sql.DB) {
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
		t.Fatalf("count duplicate file_chunk Order rows: %v", err)
	}
	if duplicates != 0 {
		t.Fatalf("expected no duplicate file_chunk Order rows, got %d duplicate groups", duplicates)
	}
}

func NewTestContext(dbconn *sql.DB) storage.StorageContext {
	return storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDir(container.ContainersDir, container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
}

type FileChunkRecord struct {
	ChunkID              int64
	ContainerID          int64
	BlockOffset          int64
	StoredSize           int64
	ContainerFilename    string
	ContainerCurrentSize int64
}

func SetupStoredFileForVerification(t *testing.T, filename string, Size int) (*sql.DB, string, int64) {
	t.Helper()
	RequireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	ResetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}

	ApplySchema(t, dbconn)
	ResetDB(t, dbconn)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		_ = dbconn.Close()
		t.Fatalf("mkdir inputDir: %v", err)
	}

	inPath := CreateTempFile(t, inputDir, filename, Size)
	fileHash := SHA256File(t, inPath)

	sgctx := NewTestContext(dbconn)

	if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain); err != nil {
		t.Fatalf("store file: %v", err)
	}

	return dbconn, inPath, FetchFileIDByHash(t, dbconn, fileHash)
}

func ContainerPathForRecord(record FileChunkRecord) string {
	filename := record.ContainerFilename

	return filepath.Join(container.ContainersDir, filename)
}

func FetchFirstFileChunkRecord(t *testing.T, dbconn *sql.DB, fileID int64) FileChunkRecord {
	t.Helper()

	var record FileChunkRecord
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
		&record.ChunkID,
		&record.ContainerID,
		&record.BlockOffset,
		&record.StoredSize,
		&record.ContainerFilename,
		&record.ContainerCurrentSize,
	)
	if err != nil {
		t.Fatalf("query first file chunk record: %v", err)
	}

	return record
}

func CorruptFirstCompletedChunkByte(t *testing.T, dbconn *sql.DB, containersDir string) {
	t.Helper()

	var BlockOffset int64
	var StoredSize int64
	var ContainerFilename string
	err := dbconn.QueryRow(`
		SELECT b.block_offset, b.stored_size, ctr.filename
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE c.status = $1
		ORDER BY c.id ASC
		LIMIT 1
	`, filestate.ChunkCompleted).Scan(&BlockOffset, &StoredSize, &ContainerFilename)
	if err != nil {
		t.Fatalf("query first completed chunk for corruption: %v", err)
	}

	containerPath := filepath.Join(containersDir, ContainerFilename)
	f, err := os.OpenFile(containerPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file for corruption: %v", err)
	}

	corruptionOffset := BlockOffset
	if StoredSize > 10 {
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

func MustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return b
}

func SetTestAESGCMKey(t *testing.T) {
	t.Helper()
	t.Setenv("COLDKEEP_KEY", "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
}

func AssertDeepVerifyAggregateError(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s verify error but got nil", context)
	}
	errText := err.Error()
	if !strings.Contains(errText, "system deep verification failed") || !strings.Contains(errText, "found 1 errors in deep verification of container files") {
		t.Fatalf("expected %s verify error to keep deep aggregate contract, got: %v", context, err)
	}
}

func AssertErrorContains(t *testing.T, err error, substring string, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s error but got nil", context)
	}
	actual := strings.ToLower(err.Error())
	expect := strings.ToLower(substring)
	if !strings.Contains(actual, expect) {
		t.Fatalf("expected %s error to contain (case-insensitive) %q, got: %v", context, substring, err)
	}
}

func Itoa(i int) string {
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

func IsRetryableTxAbortError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "current transaction is aborted") || strings.Contains(msg, "25p02")
}

type RollbackFailingWriter struct {
	Base            *container.LocalWriter
	ForcedErr       error
	AppendSucceeded bool
	LastPlacement   container.LocalPlacement
	QuarantineCalls int
}

func NewRollbackFailingWriter(Base *container.LocalWriter) *RollbackFailingWriter {
	return &RollbackFailingWriter{
		Base:      Base,
		ForcedErr: errors.New("injected rollback cleanup failure"),
	}
}

func (w *RollbackFailingWriter) FinalizeContainer() error {
	return w.Base.FinalizeContainer()
}

func (w *RollbackFailingWriter) BindDB(dbconn *sql.DB) {
	w.Base.BindDB(dbconn)
}

func (w *RollbackFailingWriter) AppendPayload(tx db.DBTX, payload []byte) (container.LocalPlacement, error) {
	placement, err := w.Base.AppendPayload(tx, payload)
	if err == nil {
		w.AppendSucceeded = true
		w.LastPlacement = placement
	}
	return placement, err
}

func (w *RollbackFailingWriter) AcknowledgeAppendCommitted() {
	w.Base.AcknowledgeAppendCommitted()
}

func (w *RollbackFailingWriter) RollbackLastAppend() error {
	if !w.AppendSucceeded {
		return nil
	}
	if w.ForcedErr != nil {
		return w.ForcedErr
	}
	return errors.New("injected rollback cleanup failure")
}

func (w *RollbackFailingWriter) QuarantineActiveContainer() error {
	w.QuarantineCalls++
	return w.Base.QuarantineActiveContainer()
}

var ChunkBoundaryCases = []struct {
	Name string
	Size int
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

type ChunkRecord struct {
	Order int
	Hash  string
	Size  int64
}

func CreateSampleDataset(t *testing.T, dir string) map[string]string {
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

func FetchFirstChunkRecord(t *testing.T, dbconn *sql.DB, fileID int64) FileChunkRecord {
	t.Helper()

	var record FileChunkRecord
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
		&record.ChunkID,
		&record.ContainerID,
		&record.BlockOffset,
		&record.StoredSize,
		&record.ContainerFilename,
		&record.ContainerCurrentSize,
	)
	if err != nil {
		t.Fatalf("query first chunk record: %v", err)
	}

	return record
}

func OpenRawPostgresDB(t *testing.T, dbName string) *sql.DB {
	t.Helper()
	if dbName == "" {
		dbName = GetenvOrDefault("DB_NAME", "coldkeep")
	}
	connStr := "host=" + GetenvOrDefault("DB_HOST", "127.0.0.1") +
		" port=" + GetenvOrDefault("DB_PORT", "5432") +
		" user=" + GetenvOrDefault("DB_USER", "coldkeep") +
		" password=" + os.Getenv("DB_PASSWORD") +
		" dbname=" + dbName +
		" sslmode=" + GetenvOrDefault("DB_SSLMODE", "disable") +
		" connect_timeout=5"
	rawDB, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open raw postgres DB (%s): %v", dbName, err)
	}
	if err := rawDB.Ping(); err != nil {
		_ = rawDB.Close()
		t.Fatalf("ping raw postgres DB (%s): %v", dbName, err)
	}
	return rawDB
}

func QueryChunkGraph(t *testing.T, dbconn *sql.DB, fileID int64) []ChunkRecord {
	t.Helper()
	rows, err := dbconn.Query(`
		SELECT fc.chunk_order, c.chunk_hash, c.Size
		FROM file_chunk fc
		JOIN chunk c ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, fileID)
	if err != nil {
		t.Fatalf("query chunk graph: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("rows.Close error: %v", err)
		}
	}()

	var records []ChunkRecord
	for rows.Next() {
		var r ChunkRecord
		if err := rows.Scan(&r.Order, &r.Hash, &r.Size); err != nil {
			t.Fatalf("scan chunk graph row: %v", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk graph: %v", err)
	}
	return records
}

func FindRepoFixtureDir(t *testing.T, fixtureDir string) string {
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

func CopyDirTree(t *testing.T, srcDir, dstDir string) {
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

func CollectFileHashesByCount(t *testing.T, root string) map[string]int {
	t.Helper()

	hashCount := make(map[string]int)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		hashCount[SHA256File(t, path)]++
		return nil
	})
	if err != nil {
		t.Fatalf("collect file hashes from %s: %v", root, err)
	}

	return hashCount
}

func RunFixtureFolderEndToEnd(t *testing.T, fixtureDir string) {
	t.Helper()
	RequireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	ResetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	ApplySchema(t, dbconn)
	ResetDB(t, dbconn)

	fixturePath := FindRepoFixtureDir(t, fixtureDir)
	inputDir := filepath.Join(tmp, "input")
	CopyDirTree(t, fixturePath, inputDir)

	expectedHashCounts := CollectFileHashesByCount(t, inputDir)
	expectedUniqueCount := len(expectedHashCounts)
	expectedUniqueHashes := make(map[string]bool, len(expectedHashCounts))
	for Hash := range expectedHashCounts {
		expectedUniqueHashes[Hash] = true
	}

	if err := storage.StoreFolderWithStorageContext(NewTestContext(dbconn), inputDir); err != nil {
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
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("rows.Close error: %v", err)
		}
	}()

	restoredCount := 0
	for rows.Next() {
		var id int64
		var Hash string
		if err := rows.Scan(&id, &Hash); err != nil {
			t.Fatalf("scan logical_file row for %s: %v", fixtureDir, err)
		}

		outPath := filepath.Join(outDir, fmt.Sprintf("%d.restore.bin", id))
		if err := storage.RestoreFileWithDB(dbconn, id, outPath); err != nil {
			t.Fatalf("restore file id %d for %s: %v", id, fixtureDir, err)
		}

		gotHash := SHA256File(t, outPath)
		if gotHash != Hash {
			t.Fatalf("restored Hash mismatch for file id %d in %s: want %s got %s", id, fixtureDir, Hash, gotHash)
		}

		if !expectedUniqueHashes[gotHash] {
			t.Fatalf("unexpected restored Hash %s for %s", gotHash, fixtureDir)
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

	for Hash := range expectedUniqueHashes {
		t.Fatalf("missing restored file for Hash %s in %s", Hash, fixtureDir)
	}
}

func RunFixtureFolderRestoreAll(t *testing.T, fixtureDir string) {
	t.Helper()
	RequireDB(t)

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	ResetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	ApplySchema(t, dbconn)
	ResetDB(t, dbconn)

	fixturePath := FindRepoFixtureDir(t, fixtureDir)
	inputDir := filepath.Join(tmp, "input")
	CopyDirTree(t, fixturePath, inputDir)

	expectedHashCounts := CollectFileHashesByCount(t, inputDir)
	expectedUniqueCount := len(expectedHashCounts)
	hashToPath := make(map[string]string, expectedUniqueCount)
	if walkErr := filepath.WalkDir(inputDir, func(path string, d os.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return e
		}
		Hash := SHA256File(t, path)
		if _, exists := hashToPath[Hash]; !exists {
			hashToPath[Hash] = path
		}
		return nil
	}); walkErr != nil {
		t.Fatalf("walk input dir: %v", walkErr)
	}

	if err := storage.StoreFolderWithStorageContext(NewTestContext(dbconn), inputDir); err != nil {
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
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("rows.Close error: %v", err)
		}
	}()

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

		// Verify restored Hash matches what the DB recorded.
		gotHash := SHA256File(t, outPath)
		if gotHash != storedHash {
			t.Errorf("restored Hash mismatch for id=%d name=%s: db_hash=%s restored_hash=%s",
				id, origName, storedHash, gotHash)
		}

		if _, found := expectedHashCounts[storedHash]; !found {
			t.Errorf("unexpected stored Hash for id=%d name=%s: %s", id, origName, storedHash)
			continue
		}
		if seenHashes[storedHash] {
			t.Errorf("duplicate logical_file row for Hash %s (id=%d name=%s)", storedHash, id, origName)
			continue
		}

		// Verify restored bytes match a representative source file for this Hash.
		srcPath, found := hashToPath[storedHash]
		if !found {
			t.Errorf("original source not found for Hash=%s id=%d name=%s", storedHash, id, origName)
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
