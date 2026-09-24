package storage

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

type simulationReuseGraphCounts struct {
	logicalFiles  int
	chunks        int
	blocks        int
	storageBlocks int
	chunkRefs     int
	containers    int
	physicalFiles int
}

func TestSimulatedStoreDuplicateUsesGraphOnlyReuse(t *testing.T) {
	testCases := []struct {
		name        string
		codec       blocks.Codec
		compression string
		semantic    string
	}{
		{name: "plain none off", codec: blocks.CodecPlain, compression: "none", semantic: "off"},
		{name: "plain zstd suspicious", codec: blocks.CodecPlain, compression: "zstd", semantic: "suspicious"},
		{name: "aes-gcm none always", codec: blocks.CodecAESGCM, compression: "none", semantic: "always"},
		{name: "aes-gcm zstd always", codec: blocks.CodecAESGCM, compression: "zstd", semantic: "always"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COLDKEEP_KEY", strings.Repeat("01", 32))
			t.Setenv("COLDKEEP_COMPRESSION", tc.compression)
			t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", tc.semantic)
			assertSimulatedStoreDuplicateUsesGraphOnlyReuse(t, tc.codec)
		})
	}
}

func assertSimulatedStoreDuplicateUsesGraphOnlyReuse(t *testing.T, codec blocks.Codec) {
	t.Helper()

	sgctx, err := ParseStorageContext(string(SimulatedStorage))
	if err != nil {
		t.Fatalf("create simulated storage context: %v", err)
	}
	t.Cleanup(func() {
		if err := sgctx.Close(); err != nil {
			t.Errorf("close simulated storage context: %v", err)
		}
	})

	sourceDir := t.TempDir()
	payload := []byte(strings.Repeat("phase-9-simulated-duplicate-reuse-", 64))
	firstPath := filepath.Join(sourceDir, "first.bin")
	secondPath := filepath.Join(sourceDir, "second.bin")
	for _, path := range []string{firstPath, secondPath} {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatalf("write duplicate source %s: %v", path, err)
		}
	}

	first, err := StoreFileWithStorageContextAndCodecResult(sgctx, firstPath, codec)
	if err != nil {
		t.Fatalf("store first simulated file: %v", err)
	}
	before := readSimulationReuseGraphCounts(t, sgctx.DB)

	second, err := StoreFileWithStorageContextAndCodecResult(sgctx, secondPath, codec)
	if err != nil {
		t.Fatalf("store duplicate simulated file: %v", err)
	}
	after := readSimulationReuseGraphCounts(t, sgctx.DB)

	if second.FileID != first.FileID {
		t.Fatalf("duplicate logical ID = %d, want %d", second.FileID, first.FileID)
	}
	if !second.AlreadyStored {
		t.Fatal("duplicate simulated Store must report AlreadyStored=true")
	}
	if before.logicalFiles != after.logicalFiles || before.chunks != after.chunks ||
		before.blocks != after.blocks || before.storageBlocks != after.storageBlocks ||
		before.chunkRefs != after.chunkRefs || before.containers != after.containers {
		t.Fatalf("duplicate changed reusable graph cardinality: before=%+v after=%+v", before, after)
	}
	if after.physicalFiles != before.physicalFiles+1 {
		t.Fatalf("physical path count = %d, want %d", after.physicalFiles, before.physicalFiles+1)
	}

	var logicalRetries int
	if err := sgctx.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, first.FileID).Scan(&logicalRetries); err != nil {
		t.Fatalf("read logical-file retry count: %v", err)
	}
	var chunkRetries int
	if err := sgctx.DB.QueryRow(`
		SELECT COALESCE(SUM(c.retry_count), 0)
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1
	`, first.FileID).Scan(&chunkRetries); err != nil {
		t.Fatalf("read chunk retry count: %v", err)
	}
	if logicalRetries != 0 || chunkRetries != 0 {
		t.Fatalf("duplicate caused reuse rebuild retries: logical=%d chunks=%d", logicalRetries, chunkRetries)
	}
}

func TestSimulationGraphOnlyReuseValidationFailsClosedOnInvalidScopeAndCatalogBounds(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	fileID := insertReusableTestLogicalFile(t, dbconn, 64)
	containerID := insertReusableTestContainer(t, dbconn, "catalog-only.bin", false)
	chunkID := insertReusableTestChunk(t, dbconn, "catalog-only-chunk", status.ChunkCompleted)
	insertReusableTestBlock(t, dbconn, chunkID, containerID, int64(container.ContainerHdrLen))
	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	invalidPolicy := reusableValidationPolicy{}
	if err := validateReusableLogicalFileGraphWithPolicy(ctx, dbconn, fileID, invalidPolicy); err == nil || !strings.Contains(err.Error(), "invalid reusable validation scope") {
		t.Fatalf("zero validation scope error = %v, want fail-closed rejection", err)
	}

	graphOnly := reusableValidationPolicy{scope: reusableValidationSimulationGraphOnly}
	if err := validateReusableLogicalFileGraphWithPolicy(ctx, dbconn, fileID, graphOnly); err != nil {
		t.Fatalf("valid graph-only catalog unexpectedly required payload access: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, container.ContainerHdrLen+32, containerID); err != nil {
		t.Fatalf("corrupt simulated container size metadata: %v", err)
	}
	if err := validateReusableLogicalFileGraphWithPolicy(ctx, dbconn, fileID, graphOnly); err == nil || !strings.Contains(err.Error(), "out-of-bounds placement") {
		t.Fatalf("invalid graph-only catalog error = %v, want bounds rejection", err)
	}
}

func TestRepositoryReuseValidationRejectsBlankContainerDirectory(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	fileID := insertReusableTestLogicalFile(t, dbconn, 64)
	containerID := insertReusableTestContainer(t, dbconn, "graph-only.bin", false)
	chunkID := insertReusableTestChunk(t, dbconn, "graph-only-chunk", status.ChunkCompleted)
	insertReusableTestBlock(t, dbconn, chunkID, containerID, int64(container.ContainerHdrLen))
	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	err = validateReusableLogicalFileGraphWithContext(ctx, dbconn, fileID, "")
	if err == nil || !strings.Contains(err.Error(), "container directory is required") {
		t.Fatalf("blank full-validation container directory error = %v, want required-directory rejection", err)
	}
}

func readSimulationReuseGraphCounts(t *testing.T, dbconn *sql.DB) simulationReuseGraphCounts {
	t.Helper()
	var counts simulationReuseGraphCounts
	queries := []struct {
		name string
		dest *int
	}{
		{name: "logical_file", dest: &counts.logicalFiles},
		{name: "chunk", dest: &counts.chunks},
		{name: "blocks", dest: &counts.blocks},
		{name: "storage_blocks", dest: &counts.storageBlocks},
		{name: "chunk_block_refs", dest: &counts.chunkRefs},
		{name: "container", dest: &counts.containers},
		{name: "physical_file", dest: &counts.physicalFiles},
	}
	for _, query := range queries {
		if err := dbconn.QueryRow("SELECT COUNT(*) FROM " + query.name).Scan(query.dest); err != nil {
			t.Fatalf("count %s rows: %v", query.name, err)
		}
	}
	return counts
}
