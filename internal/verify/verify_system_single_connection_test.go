package verify_test

import (
	"context"
	"database/sql"
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

const verifySingleConnectionChild = "COLDKEEP_VERIFY_SINGLE_CONNECTION_CHILD"

func TestVerifySystemDeepPackedSQLiteSingleConnection(t *testing.T) {
	if os.Getenv(verifySingleConnectionChild) == "1" {
		runVerifySystemDeepPackedSQLiteSingleConnection(t)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVerifySystemDeepPackedSQLiteSingleConnection$", "-test.count=1")
	env := make([]string, 0, len(os.Environ())+2)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, verifySingleConnectionChild+"=") ||
			strings.HasPrefix(entry, "COLDKEEP_DB_OPERATION_TIMEOUT_MS=") {
			continue
		}
		env = append(env, entry)
	}
	cmd.Env = append(env,
		verifySingleConnectionChild+"=1",
		"COLDKEEP_DB_OPERATION_TIMEOUT_MS=250",
	)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("single-connection deep verification child exceeded outer timeout: %v\n%s", ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("single-connection deep verification child failed: %v\n%s", err, output)
	}
}

func runVerifySystemDeepPackedSQLiteSingleConnection(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	dbconn, err := sql.Open("sqlite3", filepath.Join(root, "coldkeep.db"))
	if err != nil {
		t.Fatalf("open SQLite fixture: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	if err := db.ApplySQLiteSessionPragmas(dbconn); err != nil {
		t.Fatalf("apply SQLite pragmas: %v", err)
	}
	if err := db.EnsureSchema(dbconn); err != nil {
		t.Fatalf("ensure current schema: %v", err)
	}

	containersDir := filepath.Join(root, "containers")
	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	storageContext := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
	source := filepath.Join(root, "packed-source.bin")
	if err := os.WriteFile(source, []byte("single-connection packed deep verification fixture"), 0o600); err != nil {
		t.Fatalf("write source fixture: %v", err)
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResult(storageContext, source, blocks.CodecPlain); err != nil {
		t.Fatalf("store packed fixture: %v", err)
	}
	if err := writer.FinalizeContainer(); err != nil {
		t.Fatalf("finalize packed fixture container: %v", err)
	}

	var storageBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlockCount); err != nil {
		t.Fatalf("count packed storage blocks: %v", err)
	}
	if storageBlockCount < 1 {
		t.Fatal("packed fixture must contain at least one storage_blocks row")
	}

	if err := verify.VerifySystemDeepWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("deep verification with one SQLite connection: %v", err)
	}
}
