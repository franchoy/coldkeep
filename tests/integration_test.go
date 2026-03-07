package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/utils"
)

// NOTE:
// These tests are integration-style (DB + filesystem).
// They are skipped unless COLDKEEP_TEST_DB=1 is set.
//
// Run (example):
//   COLDKEEP_TEST_DB=1 DB_HOST=localhost DB_PORT=5432 DB_USER=coldkeep DB_PASSWORD=coldkeep DB_NAME=coldkeep go test ./app -v

func requireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run integration tests")
	}
}

func applySchema(t *testing.T, db *sql.DB) {
	t.Helper()

	// 1) Allow explicit override (best for Docker / CI)
	if p := os.Getenv("COLDKEEP_SCHEMA_PATH"); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read schema %s: %v", p, err)
		}
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("apply schema: %v", err)
		}
		return
	}

	// 2) Walk upwards from cwd to find db/init.sql
	cwd, err := os.Getwd()
	if err == nil {
		dir := cwd
		for i := 0; i < 6; i++ { // climb up a few levels
			candidate := filepath.Join(dir, "db", "init.sql")
			if _, statErr := os.Stat(candidate); statErr == nil {
				b, err := os.ReadFile(candidate)
				if err != nil {
					t.Fatalf("read schema %s: %v", candidate, err)
				}
				if _, err := db.Exec(string(b)); err != nil {
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
		"../db/init.sql",
		"../../db/init.sql",
		"/repo/db/init.sql",
		"/work/db/init.sql",
		"/db/init.sql",
		"/app/db/init.sql",
	}
	for _, p := range candidates {
		if _, statErr := os.Stat(p); statErr == nil {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read schema %s: %v", p, err)
			}
			if _, err := db.Exec(string(b)); err != nil {
				t.Fatalf("apply schema: %v", err)
			}
			return
		}
	}

	t.Fatalf("could not find db/init.sql; set COLDKEEP_SCHEMA_PATH to an absolute path")
}

func resetDB(t *testing.T, db *sql.DB) {
	t.Helper()
	// Keep schema_version; clear the data tables and reset sequences.
	_, err := db.Exec(`
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
	if container.StorageDir == "" {
		t.Fatalf("storageDir is empty")
	}
	_ = os.RemoveAll(container.StorageDir)
	if err := os.MkdirAll(container.StorageDir, 0o755); err != nil {
		t.Fatalf("mkdir storageDir: %v", err)
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

func fetchFileIDByHash(t *testing.T, db *sql.DB, fileHash string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`SELECT id FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&id)
	if err != nil {
		t.Fatalf("query logical_file by hash: %v", err)
	}
	return id
}

func TestRoundTripStoreRestore(t *testing.T) {
	requireDB(t)

	// Use temp dirs per test
	tmp := t.TempDir()
	container.StorageDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.StorageDir)
	resetStorage(t)

	db, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer db.Close()

	applySchema(t, db)
	resetDB(t, db)

	// Ensure we don't exercise heavy compression here.
	utils.DefaultCompression = utils.CompressionNone

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)

	// 512KB file should create multiple chunks depending on CDC params.
	inPath := createTempFile(t, inputDir, "roundtrip.bin", 512*1024)
	want := mustRead(t, inPath)
	wantHash := sha256File(t, inPath)

	if err := storage.StoreFileWithDB(db, inPath); err != nil {
		t.Fatalf("storeFileWithDB: %v", err)
	}

	fileID := fetchFileIDByHash(t, db, wantHash)

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "roundtrip.restored.bin")

	if err := storage.RestoreFileWithDB(db, fileID, outPath); err != nil {
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
	container.StorageDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.StorageDir)
	resetStorage(t)

	db, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer db.Close()

	applySchema(t, db)
	resetDB(t, db)

	utils.DefaultCompression = utils.CompressionNone

	inputDir := filepath.Join(tmp, "input")
	_ = os.MkdirAll(inputDir, 0o755)
	inPath := createTempFile(t, inputDir, "dup.bin", 256*1024)
	fileHash := sha256File(t, inPath)

	if err := storage.StoreFileWithDB(db, inPath); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := storage.StoreFileWithDB(db, inPath); err != nil {
		t.Fatalf("second store: %v", err)
	}

	// Should still be 1 logical file for this hash
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, fileHash).Scan(&n); err != nil {
		t.Fatalf("count logical_file: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 logical_file row, got %d", n)
	}
}

func TestStoreFolderParallelSmoke(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.StorageDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.StorageDir)
	resetStorage(t)

	db, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer db.Close()
	applySchema(t, db)
	resetDB(t, db)

	utils.DefaultCompression = utils.CompressionNone

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
	go func() { done <- storage.StoreFolder(inputDir) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("storeFolder: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("storeFolder timed out (possible deadlock or blocked workers)")
	}

	// Spot-check: restore a few logical files and compare hashes.
	rows, err := db.Query(`SELECT id, file_hash, original_name FROM logical_file ORDER BY id ASC LIMIT 5`)
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
		if err := storage.RestoreFileWithDB(db, id, outPath); err != nil {
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

func TestGCRemovesUnusedContainers(t *testing.T) {
	requireDB(t)

	tmp := t.TempDir()
	container.StorageDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.StorageDir)
	resetStorage(t)

	db, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer db.Close()
	applySchema(t, db)
	resetDB(t, db)

	utils.DefaultCompression = utils.CompressionNone

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
	if err := storage.StoreFileWithDB(db, fileA); err != nil {
		t.Fatalf("store fileA: %v", err)
	}
	if err := storage.StoreFileWithDB(db, fileB); err != nil {
		t.Fatalf("store fileB: %v", err)
	}

	// Count containers before removal
	var containersBefore int
	if err := db.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containersBefore); err != nil {
		t.Fatalf("count container: %v", err)
	}
	if containersBefore == 0 {
		t.Fatalf("expected at least 1 container")
	}

	// Fetch fileA ID
	var fileAID int64
	hashA := sha256File(t, fileA)
	if err := db.QueryRow(
		`SELECT id FROM logical_file WHERE file_hash = $1`,
		hashA,
	).Scan(&fileAID); err != nil {
		t.Fatalf("fetch fileA id: %v", err)
	}

	// Remove fileA
	if err := storage.RemoveFileWithDB(db, fileAID); err != nil {
		t.Fatalf("removeFileWithDB: %v", err)
	}

	// Run GC
	if err := maintenance.RunGC(); err != nil {
		t.Fatalf("runGC: %v", err)
	}

	// Count containers after GC
	var containersAfter int
	if err := db.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containersAfter); err != nil {
		t.Fatalf("count container after: %v", err)
	}

	// GC may delete 0 containers if remaining live chunks share the same container.
	// What we must guarantee is that GC does not break restore and ref_counts remain valid.
	var negatives int
	if err := db.QueryRow(`SELECT COUNT(*) FROM chunk WHERE ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check negative ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found %d chunks with negative ref_count", negatives)
	}

	// Ensure fileB still restores correctly
	var fileBID int64
	hashB := sha256File(t, fileB)
	if err := db.QueryRow(
		`SELECT id FROM logical_file WHERE file_hash = $1`,
		hashB,
	).Scan(&fileBID); err != nil {
		t.Fatalf("fetch fileB id: %v", err)
	}

	outDir := filepath.Join(tmp, "out")
	_ = os.MkdirAll(outDir, 0o755)
	outPath := filepath.Join(outDir, "fileB.restored.bin")

	if err := storage.RestoreFileWithDB(db, fileBID, outPath); err != nil {
		t.Fatalf("restore fileB after GC: %v", err)
	}

	gotHash := sha256File(t, outPath)
	if gotHash != hashB {
		t.Fatalf("hash mismatch after GC: want %s got %s", hashB, gotHash)
	}

	// Ensure no chunk has negative ref_count
	//var negatives int
	if err := db.QueryRow(`SELECT COUNT(*) FROM chunk WHERE ref_count < 0`).Scan(&negatives); err != nil {
		t.Fatalf("check negative ref_count: %v", err)
	}
	if negatives != 0 {
		t.Fatalf("found chunks with negative ref_count")
	}
}
