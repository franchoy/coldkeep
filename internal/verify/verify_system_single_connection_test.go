package verify_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

const (
	verifySingleConnectionModeEnv          = "COLDKEEP_VERIFY_SINGLE_CONNECTION_MODE"
	verifySingleConnectionDBPathEnv        = "COLDKEEP_VERIFY_SINGLE_CONNECTION_DB_PATH"
	verifySingleConnectionContainersDirEnv = "COLDKEEP_VERIFY_SINGLE_CONNECTION_CONTAINERS_DIR"
	verifySingleConnectionOperationEnv     = "COLDKEEP_DB_OPERATION_TIMEOUT_MS"

	verifySingleConnectionFixtureMode = "fixture"
	verifySingleConnectionSuccessMode = "success"
	verifySingleConnectionBlockedMode = "blocked"

	verifySingleConnectionOperationTimeout = 250 * time.Millisecond
	verifySingleConnectionOuterTimeout     = 10 * time.Second
)

func TestVerifySystemDeepPackedSQLiteSingleConnection(t *testing.T) {
	switch mode := os.Getenv(verifySingleConnectionModeEnv); mode {
	case "":
		runVerifySystemDeepPackedSQLiteSingleConnectionParent(t)
	case verifySingleConnectionFixtureMode:
		buildVerifySystemDeepPackedSQLiteSingleConnectionFixture(t)
	case verifySingleConnectionSuccessMode:
		runVerifySystemDeepPackedSQLiteSingleConnectionSuccess(t)
	case verifySingleConnectionBlockedMode:
		runVerifySystemDeepPackedSQLiteSingleConnectionBlocked(t)
	default:
		t.Fatalf("unknown single-connection verification child mode %q", mode)
	}
}

func runVerifySystemDeepPackedSQLiteSingleConnectionParent(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	dbPath := filepath.Join(root, "coldkeep.db")
	containersDir := filepath.Join(root, "containers")
	for _, mode := range []string{
		verifySingleConnectionFixtureMode,
		verifySingleConnectionSuccessMode,
		verifySingleConnectionBlockedMode,
	} {
		runVerifySystemDeepPackedSQLiteSingleConnectionChild(t, mode, dbPath, containersDir)
	}
}

func runVerifySystemDeepPackedSQLiteSingleConnectionChild(t *testing.T, mode, dbPath, containersDir string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), verifySingleConnectionOuterTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVerifySystemDeepPackedSQLiteSingleConnection$", "-test.count=1")
	env := make([]string, 0, len(os.Environ())+4)
	for _, entry := range os.Environ() {
		if isVerifySingleConnectionControlledEnvironment(entry) {
			continue
		}
		env = append(env, entry)
	}
	env = append(env,
		verifySingleConnectionModeEnv+"="+mode,
		verifySingleConnectionDBPathEnv+"="+dbPath,
		verifySingleConnectionContainersDirEnv+"="+containersDir,
	)
	if mode != verifySingleConnectionFixtureMode {
		env = append(env, verifySingleConnectionOperationEnv+"=250")
	}
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("single-connection verification %s child exceeded outer timeout: %v\n%s", mode, ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("single-connection verification %s child failed: %v\n%s", mode, err, output)
	}
}

func isVerifySingleConnectionControlledEnvironment(entry string) bool {
	for _, name := range []string{
		verifySingleConnectionModeEnv,
		verifySingleConnectionDBPathEnv,
		verifySingleConnectionContainersDirEnv,
		verifySingleConnectionOperationEnv,
	} {
		if strings.HasPrefix(entry, name+"=") {
			return true
		}
	}
	return false
}

func verifySingleConnectionFixturePaths(t *testing.T) (string, string) {
	t.Helper()

	dbPath := os.Getenv(verifySingleConnectionDBPathEnv)
	if dbPath == "" {
		t.Fatalf("%s must be set for child mode", verifySingleConnectionDBPathEnv)
	}
	containersDir := os.Getenv(verifySingleConnectionContainersDirEnv)
	if containersDir == "" {
		t.Fatalf("%s must be set for child mode", verifySingleConnectionContainersDirEnv)
	}
	return dbPath, containersDir
}

func buildVerifySystemDeepPackedSQLiteSingleConnectionFixture(t *testing.T) {
	t.Helper()

	if _, present := os.LookupEnv(verifySingleConnectionOperationEnv); present {
		t.Fatalf("fixture child must not inherit %s", verifySingleConnectionOperationEnv)
	}
	if timeout := db.DefaultOperationTimeout(); timeout <= verifySingleConnectionOperationTimeout {
		t.Fatalf("fixture operation timeout = %v, want greater than %v", timeout, verifySingleConnectionOperationTimeout)
	}

	dbPath, containersDir := verifySingleConnectionFixturePaths(t)
	dbconn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open SQLite fixture: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = dbconn.Close()
		}
	}()

	if err := db.ApplySQLiteSessionPragmas(dbconn); err != nil {
		t.Fatalf("apply SQLite fixture pragmas: %v", err)
	}
	if err := db.EnsureSchema(dbconn); err != nil {
		t.Fatalf("ensure current schema: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	storageContext := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
	source := filepath.Join(filepath.Dir(dbPath), "packed-source.bin")
	if err := os.WriteFile(source, []byte("single-connection packed deep verification fixture"), 0o600); err != nil {
		t.Fatalf("write source fixture: %v", err)
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResult(storageContext, source, blocks.CodecPlain); err != nil {
		t.Fatalf("store packed fixture: %v", err)
	}
	requireVerifySingleConnectionPackedBlock(t, dbconn)

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close SQLite fixture database: %v", err)
	}
	closed = true
}

func runVerifySystemDeepPackedSQLiteSingleConnectionSuccess(t *testing.T) {
	t.Helper()

	requireVerifySingleConnectionOperationTimeout(t)
	dbPath, containersDir := verifySingleConnectionFixturePaths(t)
	dbconn := openVerifySystemDeepPackedSQLiteSingleConnectionFixture(t, dbPath)
	closed := false
	defer func() {
		if !closed {
			_ = dbconn.Close()
		}
	}()

	requireVerifySingleConnectionPackedBlock(t, dbconn)
	if err := verify.VerifySystemDeepWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("deep verification with one SQLite connection: %v", err)
	}
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close successful verification database: %v", err)
	}
	closed = true
}

func runVerifySystemDeepPackedSQLiteSingleConnectionBlocked(t *testing.T) {
	t.Helper()

	requireVerifySingleConnectionOperationTimeout(t)
	dbPath, containersDir := verifySingleConnectionFixturePaths(t)
	dbconn := openVerifySystemDeepPackedSQLiteSingleConnectionFixture(t, dbPath)
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = dbconn.Close()
		}
	}()

	requireVerifySingleConnectionPackedBlock(t, dbconn)
	acquireCtx, cancelAcquire := context.WithTimeout(context.Background(), verifySingleConnectionOperationTimeout)
	heldConn, err := dbconn.Conn(acquireCtx)
	cancelAcquire()
	if err != nil {
		t.Fatalf("acquire sole SQLite connection: %v", err)
	}
	connClosed := false
	defer func() {
		if !connClosed {
			_ = heldConn.Close()
		}
	}()

	stats := dbconn.Stats()
	if stats.MaxOpenConnections != 1 || stats.OpenConnections != 1 || stats.InUse != 1 || stats.Idle != 0 {
		t.Fatalf("held single-connection pool stats = max:%d open:%d in-use:%d idle:%d, want 1/1/1/0",
			stats.MaxOpenConnections, stats.OpenConnections, stats.InUse, stats.Idle)
	}

	verifyErr := verify.VerifySystemDeepWithContainersDir(dbconn, containersDir)
	if err := heldConn.Close(); err != nil {
		t.Fatalf("release held SQLite connection: %v", err)
	}
	connClosed = true
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close blocked verification database: %v", err)
	}
	dbClosed = true

	if verifyErr == nil {
		t.Fatal("deep verification with the sole SQLite connection held unexpectedly succeeded")
	}
	if !errors.Is(verifyErr, context.DeadlineExceeded) {
		t.Fatalf("deep verification with the sole SQLite connection held error = %v, want context deadline exceeded", verifyErr)
	}
}

func requireVerifySingleConnectionOperationTimeout(t *testing.T) {
	t.Helper()

	if timeout := db.DefaultOperationTimeout(); timeout != verifySingleConnectionOperationTimeout {
		t.Fatalf("verification child operation timeout = %v, want %v", timeout, verifySingleConnectionOperationTimeout)
	}
}

func openVerifySystemDeepPackedSQLiteSingleConnectionFixture(t *testing.T, dbPath string) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open completed SQLite fixture: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	if err := db.ApplySQLiteSessionPragmas(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("apply SQLite verification pragmas: %v", err)
	}
	if maxOpen := dbconn.Stats().MaxOpenConnections; maxOpen != 1 {
		_ = dbconn.Close()
		t.Fatalf("verification SQLite max open connections = %d, want 1", maxOpen)
	}
	return dbconn
}

func requireVerifySingleConnectionPackedBlock(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var storageBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlockCount); err != nil {
		t.Fatalf("count packed storage blocks: %v", err)
	}
	if storageBlockCount < 1 {
		t.Fatal("packed fixture must contain at least one storage_blocks row")
	}
}
