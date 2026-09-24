package verify

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	filestate "github.com/franchoy/coldkeep/internal/status"
)

type recordingDownstreamReader struct {
	delegate ContainerReader
	faults   map[int64]byte
	calls    map[int64]int
}

func (r *recordingDownstreamReader) ReadStoredPayload(ctx context.Context, meta BlockStorageMetadata) ([]byte, error) {
	if r.calls == nil {
		r.calls = make(map[int64]int)
	}
	r.calls[meta.BlockID]++
	payload, err := r.delegate.ReadStoredPayload(ctx, meta)
	if err != nil {
		return nil, err
	}
	result := append([]byte(nil), payload...)
	if mask, ok := r.faults[meta.BlockID]; ok {
		if len(result) == 0 {
			return nil, fmt.Errorf("test fault target block %d returned an empty payload", meta.BlockID)
		}
		result[0] ^= mask
	}
	return result, nil
}

type deepAggregationFixture struct {
	dbconn        *sql.DB
	containersDir string
	blockIDs      []int64
	containerPath string
}

func newDeepAggregationFixture(t *testing.T) deepAggregationFixture {
	t.Helper()
	dbconn := openVerifyTestDB(t)
	t.Cleanup(func() { _ = dbconn.Close() })
	containersDir := t.TempDir()

	blockA, chunksA := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ck014-downstream-one")}, nil)
	blockB, chunksB := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ck014-downstream-two")}, nil)
	chunkIDs := append(chunksA, chunksB...)
	for _, chunkID := range chunkIDs {
		result, err := dbconn.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkID)
		if err != nil {
			t.Fatalf("mark packed fixture chunk %d completed: %v", chunkID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			t.Fatalf("mark packed fixture chunk %d completed affected=%d err=%v", chunkID, rows, err)
		}
	}

	var filename string
	var containerCount int
	if err := dbconn.QueryRow(`
		SELECT MIN(c.filename), COUNT(DISTINCT c.id)
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE sb.id IN ($1, $2)
	`, blockA, blockB).Scan(&filename, &containerCount); err != nil {
		t.Fatalf("query deep aggregation fixture container: %v", err)
	}
	if containerCount != 1 {
		t.Fatalf("deep aggregation fixture must use one container, got %d", containerCount)
	}

	fixture := deepAggregationFixture{
		dbconn:        dbconn,
		containersDir: containersDir,
		blockIDs:      []int64{blockA, blockB},
		containerPath: filepath.Join(containersDir, filename),
	}
	if err := VerifySystemFullWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("deep aggregation fixture full preflight: %v", err)
	}
	return fixture
}

func deepAggregationCatalogSnapshot(t *testing.T, dbconn *sql.DB) []string {
	t.Helper()
	queries := []string{
		`SELECT id, status, live_ref_count FROM chunk ORDER BY id`,
		`SELECT id, container_id, container_offset, stored_size, hex(block_hash), hex(physical_hash) FROM storage_blocks ORDER BY id`,
		`SELECT chunk_id, block_id, offset_in_block, size_in_block FROM chunk_block_refs ORDER BY chunk_id`,
		`SELECT id, filename, sealed, sealing, quarantine, current_size, max_size, COALESCE(container_hash, '') FROM container ORDER BY id`,
	}
	var snapshot []string
	for _, query := range queries {
		rows, err := dbconn.Query(query)
		if err != nil {
			t.Fatalf("snapshot deep aggregation catalog query: %v", err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatalf("snapshot deep aggregation catalog columns: %v", err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				_ = rows.Close()
				t.Fatalf("snapshot deep aggregation catalog row: %v", err)
			}
			parts := make([]string, len(values))
			for i, value := range values {
				parts[i] = fmt.Sprint(value)
			}
			snapshot = append(snapshot, strings.Join(parts, "|"))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("snapshot deep aggregation catalog iteration: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("snapshot deep aggregation catalog close: %v", err)
		}
	}
	return snapshot
}

func TestVerifySystemDeepPreflightFailureDoesNotInvokeDownstreamReader(t *testing.T) {
	fixture := newDeepAggregationFixture(t)
	targetBlock := fixture.blockIDs[0]
	CorruptContainerByte(t, verifyCorruptionRepo{dbconn: fixture.dbconn, containersDir: fixture.containersDir}, targetBlock, 0)

	reader := &recordingDownstreamReader{
		delegate: FilesystemContainerReader{ContainersDir: fixture.containersDir},
		calls:    make(map[int64]int),
	}
	err := verifySystemDeepWithContainersDirContextUsingReader(context.Background(), fixture.dbconn, fixture.containersDir, nil, reader)
	if err == nil {
		t.Fatal("expected full preflight to reject persisted corruption")
	}
	var failure *VerifyFailure
	if !errors.As(err, &failure) || failure.Category != verifyErrPhysicalHashMismatch || failure.BlockID == nil || *failure.BlockID != targetBlock {
		t.Fatalf("expected targeted physical preflight failure for block %d, got: %v", targetBlock, err)
	}
	if len(reader.calls) != 0 {
		t.Fatalf("downstream reader was reached after preflight failure: calls=%v", reader.calls)
	}
}

func TestVerifySystemDeepCollectsTwoInjectedDownstreamPhysicalFaults(t *testing.T) {
	fixture := newDeepAggregationFixture(t)
	beforeFile, err := os.ReadFile(fixture.containerPath)
	if err != nil {
		t.Fatalf("read fixture container before verification: %v", err)
	}
	beforeCatalog := deepAggregationCatalogSnapshot(t, fixture.dbconn)

	reader := &recordingDownstreamReader{
		delegate: FilesystemContainerReader{ContainersDir: fixture.containersDir},
		faults: map[int64]byte{
			fixture.blockIDs[0]: 0x01,
			fixture.blockIDs[1]: 0x02,
		},
		calls: make(map[int64]int),
	}

	var logs bytes.Buffer
	priorWriter := log.Writer()
	priorFlags := log.Flags()
	priorPrefix := log.Prefix()
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(priorWriter)
		log.SetFlags(priorFlags)
		log.SetPrefix(priorPrefix)
	})

	err = verifySystemDeepWithContainersDirContextUsingReader(context.Background(), fixture.dbconn, fixture.containersDir, nil, reader)
	if err == nil || err.Error() != "found 2 errors in deep verification of container files" {
		t.Fatalf("expected controlled two-error downstream failure, got: %v", err)
	}
	for _, blockID := range fixture.blockIDs {
		if reader.calls[blockID] != 1 {
			t.Fatalf("target block %d downstream reads=%d want=1; calls=%v", blockID, reader.calls[blockID], reader.calls)
		}
		marker := fmt.Sprintf("physical_hash_mismatch: stage=physical_payload: block_id=%d", blockID)
		if count := strings.Count(logs.String(), marker); count != 1 {
			t.Fatalf("target block %d collected diagnostic count=%d want=1\nlogs:\n%s", blockID, count, logs.String())
		}
	}
	if len(reader.calls) != len(fixture.blockIDs) {
		t.Fatalf("unexpected downstream block reads: %v", reader.calls)
	}

	afterFile, err := os.ReadFile(fixture.containerPath)
	if err != nil {
		t.Fatalf("read fixture container after verification: %v", err)
	}
	if !bytes.Equal(afterFile, beforeFile) {
		t.Fatal("injected copied-buffer faults changed the persisted container")
	}
	afterCatalog := deepAggregationCatalogSnapshot(t, fixture.dbconn)
	if !equalStringSlices(afterCatalog, beforeCatalog) {
		t.Fatalf("injected downstream faults changed catalog state\nbefore=%v\nafter=%v", beforeCatalog, afterCatalog)
	}
}

func TestVerifySystemDeepInjectedDownstreamReaderCleanPipelinePasses(t *testing.T) {
	fixture := newDeepAggregationFixture(t)
	reader := &recordingDownstreamReader{
		delegate: FilesystemContainerReader{ContainersDir: fixture.containersDir},
		calls:    make(map[int64]int),
	}
	if err := verifySystemDeepWithContainersDirContextUsingReader(context.Background(), fixture.dbconn, fixture.containersDir, nil, reader); err != nil {
		t.Fatalf("clean production deep pipeline: %v", err)
	}
	for _, blockID := range fixture.blockIDs {
		if reader.calls[blockID] != 1 {
			t.Fatalf("clean target block %d downstream reads=%d want=1", blockID, reader.calls[blockID])
		}
	}
}

func TestVerifySystemDeepRejectsNilDownstreamReaderAfterPreflight(t *testing.T) {
	fixture := newDeepAggregationFixture(t)
	if err := verifySystemDeepWithContainersDirContextUsingReader(context.Background(), fixture.dbconn, fixture.containersDir, nil, nil); err == nil || !strings.Contains(err.Error(), "downstream container reader is nil") {
		t.Fatalf("expected post-preflight nil-reader rejection, got: %v", err)
	}
}

func equalStringSlices(left, right []string) bool {
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	return strings.Join(leftCopy, "\x00") == strings.Join(rightCopy, "\x00")
}
