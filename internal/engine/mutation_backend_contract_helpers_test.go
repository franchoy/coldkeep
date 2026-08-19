package engine_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

var mutationBackendCWDMu sync.Mutex

type mutationBackendFixture struct {
	backend      backendtest.Backend
	engine       *engine.DefaultEngine
	writer       *container.LocalWriter
	storeContext storage.StorageContext
	inputRoot    string
	containerDir string
	finalized    bool
}

func newMutationBackendFixture(t *testing.T, backend backendtest.Backend) *mutationBackendFixture {
	t.Helper()
	inputRoot := filepath.Join(t.TempDir(), "input")
	containerDir := filepath.Join(t.TempDir(), "containers")
	if err := os.MkdirAll(inputRoot, 0o700); err != nil {
		t.Fatalf("create mutation input root: %v", err)
	}
	writer := container.NewLocalWriterWithDirAndDB(
		containerDir,
		container.GetContainerMaxSize(),
		backend.DB,
	)
	storeContext := storage.StorageContext{
		DB:           backend.DB,
		Writer:       writer,
		ContainerDir: containerDir,
	}
	eng, err := engine.New(engine.Config{
		DB:           backend.DB,
		ContainerDir: containerDir,
		StoreContext: &storeContext,
	})
	if err != nil {
		t.Fatalf("engine.New mutation fixture: %v", err)
	}
	fixture := &mutationBackendFixture{
		backend: backend, engine: eng, writer: writer, storeContext: storeContext,
		inputRoot: inputRoot, containerDir: containerDir,
	}
	t.Cleanup(func() {
		if !fixture.finalized {
			_ = fixture.writer.FinalizeContainer()
		}
	})
	return fixture
}

func (f *mutationBackendFixture) store(t *testing.T, storedPath string, payload []byte) engine.StoreResult {
	t.Helper()
	cleanPath := filepath.FromSlash(storedPath)
	inputPath := filepath.Join(f.inputRoot, cleanPath)
	if err := os.MkdirAll(filepath.Dir(inputPath), 0o700); err != nil {
		t.Fatalf("create input parent for %q: %v", storedPath, err)
	}
	if err := os.WriteFile(inputPath, payload, 0o600); err != nil {
		t.Fatalf("write input %q: %v", storedPath, err)
	}
	return f.storeExisting(t, storedPath)
}

func (f *mutationBackendFixture) storeExisting(t *testing.T, storedPath string) engine.StoreResult {
	t.Helper()
	sourcePath := filepath.Join(f.inputRoot, filepath.FromSlash(storedPath))
	absoluteSource, err := filepath.Abs(sourcePath)
	if err != nil {
		t.Fatalf("resolve Store source %q: %v", storedPath, err)
	}
	if _, err := f.backend.DB.ExecContext(context.Background(), `
		UPDATE physical_file
		SET path = $1
		WHERE path = $2`, absoluteSource, filepath.ToSlash(storedPath)); err != nil {
		t.Fatalf("prepare stable replacement path %q: %v", storedPath, err)
	}
	var result engine.StoreResult
	mutationBackendWithCWD(t, f.inputRoot, func() {
		var err error
		result, err = f.engine.Store(context.Background(), engine.StoreRequest{
			SourcePath: filepath.ToSlash(storedPath),
			Codec:      "plain",
		})
		if err != nil {
			t.Fatalf("Store %q: %v", storedPath, err)
		}
	})
	if _, err := f.backend.DB.ExecContext(context.Background(), `
		UPDATE physical_file
		SET path = $1
		WHERE path = $2`, filepath.ToSlash(storedPath), result.StoredPath); err != nil {
		t.Fatalf("normalize fixture stored path %q: %v", storedPath, err)
	}
	return result
}

func (f *mutationBackendFixture) finalize(t *testing.T) {
	t.Helper()
	if f.finalized {
		return
	}
	if err := f.writer.FinalizeContainer(); err != nil {
		t.Fatalf("finalize mutation fixture container: %v", err)
	}
	f.finalized = true
}

func (f *mutationBackendFixture) restartWriter(t *testing.T) {
	t.Helper()
	if !f.finalized {
		t.Fatal("restart mutation writer before finalizing previous container")
	}
	writer := container.NewLocalWriterWithDirAndDB(
		f.containerDir,
		container.GetContainerMaxSize(),
		f.backend.DB,
	)
	f.writer = writer
	f.storeContext = storage.StorageContext{
		DB:           f.backend.DB,
		Writer:       writer,
		ContainerDir: f.containerDir,
	}
	eng, err := engine.New(engine.Config{
		DB:           f.backend.DB,
		ContainerDir: f.containerDir,
		StoreContext: &f.storeContext,
	})
	if err != nil {
		t.Fatalf("engine.New restarted mutation fixture: %v", err)
	}
	f.engine = eng
	f.finalized = false
}

func (f *mutationBackendFixture) readEngine(t *testing.T) *engine.DefaultEngine {
	t.Helper()
	storeContext := storage.StorageContext{
		DB:           f.backend.DB,
		ContainerDir: f.containerDir,
	}
	eng, err := engine.New(engine.Config{
		DB:           f.backend.DB,
		ContainerDir: f.containerDir,
		StoreContext: &storeContext,
	})
	if err != nil {
		t.Fatalf("engine.New finalized mutation fixture: %v", err)
	}
	return eng
}

func (f *mutationBackendFixture) useAbsoluteStoredPath(t *testing.T, storedPath string) string {
	t.Helper()
	absolutePath := filepath.Join(f.inputRoot, filepath.FromSlash(storedPath))
	if _, err := f.backend.DB.ExecContext(context.Background(), `
		UPDATE physical_file
		SET path = $1
		WHERE path = $2`, absolutePath, filepath.ToSlash(storedPath)); err != nil {
		t.Fatalf("set absolute fixture stored path %q: %v", storedPath, err)
	}
	return absolutePath
}

func mutationBackendWithCWD(t *testing.T, dir string, fn func()) {
	t.Helper()
	mutationBackendCWDMu.Lock()
	defer mutationBackendCWDMu.Unlock()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("change working directory to %q: %v", dir, err)
	}
	defer func() {
		if err := os.Chdir(cwd); err != nil {
			t.Fatalf("restore working directory to %q: %v", cwd, err)
		}
	}()
	fn()
}

type mutationRepositoryFingerprint struct {
	tableCounts   map[string]int64
	logical       []string
	physical      []string
	fileChunks    []string
	chunks        []string
	legacy        []string
	packed        []string
	packedRefs    []string
	containers    []string
	snapshots     []string
	snapshotPaths []string
	members       []string
	files         []string
}

func captureMutationRepositoryFingerprint(
	t *testing.T,
	dbconn *sql.DB,
	containerDir string,
) mutationRepositoryFingerprint {
	t.Helper()
	fingerprint := mutationRepositoryFingerprint{tableCounts: make(map[string]int64)}
	for _, table := range []string{
		"logical_file", "physical_file", "file_chunk", "chunk", "blocks",
		"storage_blocks", "chunk_block_refs", "container", "snapshot",
		"snapshot_path", "snapshot_file",
	} {
		fingerprint.tableCounts[table] = mutationBackendInt64(
			t, dbconn, "SELECT COUNT(*) FROM "+table,
		)
	}
	fingerprint.logical = mutationBackendRows(t, dbconn, `
		SELECT file_hash, original_name, total_size, status, ref_count,
		       retry_count, chunker_version
		FROM logical_file
		ORDER BY file_hash, total_size`)
	fingerprint.physical = mutationBackendRows(t, dbconn, `
		SELECT pf.path, lf.file_hash, COALESCE(pf.mode, -1),
		       COALESCE(pf.uid, -1), COALESCE(pf.gid, -1),
		       pf.is_metadata_complete
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		ORDER BY pf.path, lf.file_hash`)
	fingerprint.fileChunks = mutationBackendRows(t, dbconn, `
		SELECT lf.file_hash, fc.chunk_order, c.chunk_hash, c.size
		FROM file_chunk fc
		JOIN logical_file lf ON lf.id = fc.logical_file_id
		JOIN chunk c ON c.id = fc.chunk_id
		ORDER BY lf.file_hash, fc.chunk_order, c.chunk_hash`)
	fingerprint.chunks = mutationBackendRows(t, dbconn, `
		SELECT chunk_hash, size, status, live_ref_count, pin_count,
		       retry_count, chunker_version
		FROM chunk
		ORDER BY chunk_hash, size`)
	fingerprint.legacy = mutationBackendRows(t, dbconn, `
		SELECT c.chunk_hash, b.codec, b.format_version, b.plaintext_size,
		       b.stored_size, co.filename, b.block_offset
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		JOIN container co ON co.id = b.container_id
		ORDER BY c.chunk_hash, co.filename, b.block_offset`)
	fingerprint.packed = mutationBackendRows(t, dbconn, `
		SELECT co.filename, sb.container_offset, sb.format_version, sb.codec,
		       sb.plaintext_size, sb.compression_codec,
		       COALESCE(sb.compression_level, -1), sb.stored_size,
		       sb.block_hash
		FROM storage_blocks sb
		JOIN container co ON co.id = sb.container_id
		ORDER BY co.filename, sb.container_offset`)
	fingerprint.packedRefs = mutationBackendRows(t, dbconn, `
		SELECT c.chunk_hash, co.filename, sb.container_offset,
		       cbr.offset_in_block, cbr.size_in_block
		FROM chunk_block_refs cbr
		JOIN chunk c ON c.id = cbr.chunk_id
		JOIN storage_blocks sb ON sb.id = cbr.block_id
		JOIN container co ON co.id = sb.container_id
		ORDER BY c.chunk_hash, co.filename, sb.container_offset`)
	fingerprint.containers = mutationBackendRows(t, dbconn, `
		SELECT filename, sealed, sealing, quarantine, current_size, max_size,
		       COALESCE(container_hash, '')
		FROM container
		ORDER BY filename`)
	fingerprint.snapshots = mutationBackendRows(t, dbconn, `
		SELECT id, type, COALESCE(label, ''), COALESCE(parent_id, '')
		FROM snapshot
		ORDER BY id`)
	fingerprint.snapshotPaths = mutationBackendRows(t, dbconn, `
		SELECT path
		FROM snapshot_path
		ORDER BY path`)
	fingerprint.members = mutationBackendRows(t, dbconn, `
		SELECT sf.snapshot_id, sp.path, lf.file_hash,
		       COALESCE(sf.size, -1), COALESCE(sf.mode, -1)
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		JOIN logical_file lf ON lf.id = sf.logical_file_id
		ORDER BY sf.snapshot_id, sp.path, lf.file_hash`)
	fingerprint.files = mutationFileManifest(t, containerDir)
	return fingerprint
}

func assertMutationFingerprintEqual(
	t *testing.T,
	before, after mutationRepositoryFingerprint,
) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("repository/container state changed unexpectedly:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func mutationBackendRows(t *testing.T, dbconn *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := dbconn.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("query mutation fingerprint: %v\nquery: %s", err, query)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("mutation fingerprint columns: %v", err)
	}
	result := make([]string, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatalf("scan mutation fingerprint row: %v", err)
		}
		parts := make([]string, len(values))
		for i, value := range values {
			switch typed := value.(type) {
			case []byte:
				parts[i] = hex.EncodeToString(typed)
			default:
				parts[i] = fmt.Sprint(typed)
			}
		}
		result = append(result, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate mutation fingerprint rows: %v", err)
	}
	return result
}

func mutationBackendInt64(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var value int64
	if err := dbconn.QueryRowContext(context.Background(), query, args...).Scan(&value); err != nil {
		t.Fatalf("query mutation integer: %v\nquery: %s", err, query)
	}
	return value
}

func mutationBackendString(t *testing.T, dbconn *sql.DB, query string, args ...any) string {
	t.Helper()
	var value string
	if err := dbconn.QueryRowContext(context.Background(), query, args...).Scan(&value); err != nil {
		t.Fatalf("query mutation string: %v\nquery: %s", err, query)
	}
	return value
}

func mutationFileManifest(t *testing.T, root string) []string {
	t.Helper()
	manifest := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		manifest = append(manifest, fmt.Sprintf(
			"%s|%d|%s",
			filepath.ToSlash(relative),
			len(data),
			hex.EncodeToString(sum[:]),
		))
		return nil
	})
	if err != nil {
		t.Fatalf("capture file manifest under %q: %v", root, err)
	}
	sort.Strings(manifest)
	return manifest
}

func assertMutationFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read mutation output %q: %v", path, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mutation output %q mismatch: got %q want %q", path, got, want)
	}
}

func mutationManifestEntry(relativePath string, payload []byte) string {
	sum := sha256.Sum256(payload)
	return fmt.Sprintf(
		"%s|%d|%s",
		filepath.ToSlash(relativePath),
		len(payload),
		hex.EncodeToString(sum[:]),
	)
}

func assertMutationFileManifest(t *testing.T, root string, want []string) {
	t.Helper()
	got := mutationFileManifest(t, root)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("destination manifest mismatch under %q: got %v want %v", root, got, want)
	}
}

func assertStoreResultShape(t *testing.T, result engine.StoreResult) {
	t.Helper()
	if result.SourcePath == "" || result.StoredPath == "" ||
		result.LogicalFileID <= 0 || result.FileHash == "" {
		t.Fatalf("Store returned incomplete stable fields: %+v", result)
	}
}

func seedMutationDeadContainer(
	t *testing.T,
	dbconn *sql.DB,
	containerDir, filename string,
	payload []byte,
) {
	t.Helper()
	if err := os.MkdirAll(containerDir, 0o700); err != nil {
		t.Fatalf("create dead-container directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(containerDir, filename), payload, 0o600); err != nil {
		t.Fatalf("write dead-container fixture: %v", err)
	}
	var containerID int64
	if err := dbconn.QueryRowContext(context.Background(), `
		INSERT INTO container
			(filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, TRUE, FALSE)
		RETURNING id`,
		filename, int64(len(payload)), container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert dead container: %v", err)
	}
	sum := sha256.Sum256(payload)
	var chunkID int64
	if err := dbconn.QueryRowContext(context.Background(), `
		INSERT INTO chunk
			(chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id`,
		hex.EncodeToString(sum[:]), int64(len(payload)),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	if _, err := dbconn.ExecContext(context.Background(), `
		INSERT INTO blocks
			(chunk_id, codec, format_version, plaintext_size, stored_size,
			 container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $2, $3, 0)`,
		chunkID, int64(len(payload)), containerID,
	); err != nil {
		t.Fatalf("insert dead block: %v", err)
	}
}
