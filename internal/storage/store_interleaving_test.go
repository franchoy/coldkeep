package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

type storeInterleavingGate struct {
	eventCh   chan TestStoreInterleavingHookEvent
	releaseCh chan struct{}
	once      sync.Once
}

func (g *storeInterleavingGate) Await(t *testing.T) TestStoreInterleavingHookEvent {
	t.Helper()
	select {
	case event := <-g.eventCh:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for interleaving gate")
		return TestStoreInterleavingHookEvent{}
	}
}

func (g *storeInterleavingGate) Release() {
	g.once.Do(func() {
		close(g.releaseCh)
	})
}

type storeInterleavingCoordinator struct {
	mu     sync.Mutex
	events []TestStoreInterleavingHookEvent
	holds  []*storeInterleavingGateBinding
}

type storeInterleavingGateBinding struct {
	gate  *storeInterleavingGate
	match func(TestStoreInterleavingHookEvent) bool
	fired bool
}

func newStoreInterleavingCoordinator() *storeInterleavingCoordinator {
	return &storeInterleavingCoordinator{}
}

func (c *storeInterleavingCoordinator) Hold(match func(TestStoreInterleavingHookEvent) bool) *storeInterleavingGate {
	c.mu.Lock()
	defer c.mu.Unlock()
	gate := &storeInterleavingGate{
		eventCh:   make(chan TestStoreInterleavingHookEvent, 1),
		releaseCh: make(chan struct{}),
	}
	c.holds = append(c.holds, &storeInterleavingGateBinding{gate: gate, match: match})
	return gate
}

func (c *storeInterleavingCoordinator) Hook(_ context.Context, event TestStoreInterleavingHookEvent) error {
	c.mu.Lock()
	c.events = append(c.events, event)
	var waits []chan struct{}
	for _, hold := range c.holds {
		if hold.fired || !hold.match(event) {
			continue
		}
		hold.fired = true
		hold.gate.eventCh <- event
		waits = append(waits, hold.gate.releaseCh)
	}
	c.mu.Unlock()

	for _, wait := range waits {
		<-wait
	}
	return nil
}

func (c *storeInterleavingCoordinator) Snapshot() []TestStoreInterleavingHookEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TestStoreInterleavingHookEvent, len(c.events))
	copy(out, c.events)
	return out
}

func openSharedStoreInterleavingDB(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("store-interleaving-%d.sqlite", time.Now().UnixNano()))
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL", dbPath)
	dbconn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open primary sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })
	if _, err := dbconn.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("enable sqlite wal mode: %v", err)
	}
	if err := db.ApplySQLiteSessionPragmas(dbconn); err != nil {
		t.Fatalf("configure primary sqlite session: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	observer, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open observer sqlite db: %v", err)
	}
	observer.SetMaxOpenConns(1)
	observer.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = observer.Close() })
	if err := db.ApplySQLiteSessionPragmas(observer); err != nil {
		t.Fatalf("configure observer sqlite session: %v", err)
	}
	return dbconn, observer
}

func storeInterleavingHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func newInterleavingStorageContext(dbconn *sql.DB, dir string, payload []byte) StorageContext {
	return StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(dir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: dir,
		Chunker: scriptedChunker{
			version:  chunk.VersionV1SimpleRolling,
			payloads: [][]byte{payload},
		},
	}
}

func installInterleavingHooksOnContexts(
	t *testing.T,
	onEvent func(context.Context, TestStoreInterleavingHookEvent) error,
	contexts ...*StorageContext,
) {
	t.Helper()
	for _, sgctx := range contexts {
		reset := InstallTestStoreInterleavingHooks(sgctx, onEvent)
		t.Cleanup(reset)
	}
}

type interleavingChunkFinalState struct {
	chunkID             int64
	chunkStatus         string
	packedMappings      int
	legacyMappings      int
	logicalFileRefs     int
	physicalFileRefs    int
	storageBlockRefs    int
	storageBlocks       int
	orphanStorageBlocks int
	sealedContainers    int
	quarantinedConts    int
	validCompanionState bool
}

func loadInterleavingChunkFinalState(t *testing.T, dbconn *sql.DB, chunkHash string, size int) interleavingChunkFinalState {
	t.Helper()
	var state interleavingChunkFinalState
	if err := dbconn.QueryRow(
		`SELECT id, status FROM chunk WHERE chunk_hash = $1 AND size = $2`,
		chunkHash,
		size,
	).Scan(&state.chunkID, &state.chunkStatus); err != nil {
		t.Fatalf("load chunk row for final state: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1`, state.chunkID).Scan(&state.packedMappings); err != nil {
		t.Fatalf("count packed mappings: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, state.chunkID).Scan(&state.legacyMappings); err != nil {
		t.Fatalf("count legacy mappings: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, state.chunkID).Scan(&state.logicalFileRefs); err != nil {
		t.Fatalf("count logical file refs: %v", err)
	}
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM physical_file pf
		 WHERE pf.logical_file_id IN (
		   SELECT DISTINCT logical_file_id FROM file_chunk WHERE chunk_id = $1
		 )`,
		state.chunkID,
	).Scan(&state.physicalFileRefs); err != nil {
		t.Fatalf("count physical file refs: %v", err)
	}
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM storage_blocks sb
		 JOIN chunk_block_refs r ON r.block_id = sb.id
		 WHERE r.chunk_id = $1`,
		state.chunkID,
	).Scan(&state.storageBlockRefs); err != nil {
		t.Fatalf("count storage block refs: %v", err)
	}
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM storage_blocks sb
		 WHERE EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.block_id = sb.id AND r.chunk_id = $1)`,
		state.chunkID,
	).Scan(&state.storageBlocks); err != nil {
		t.Fatalf("count storage blocks: %v", err)
	}
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM storage_blocks sb
		 WHERE NOT EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.block_id = sb.id)`,
	).Scan(&state.orphanStorageBlocks); err != nil {
		t.Fatalf("count orphan storage blocks: %v", err)
	}
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM container ctr
		 WHERE ctr.id IN (
		   SELECT DISTINCT COALESCE(sb.container_id, b.container_id)
		   FROM chunk c
		   LEFT JOIN blocks b ON b.chunk_id = c.id
		   LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		   LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		   WHERE c.id = $1
		 )
		 AND ctr.sealed = TRUE`,
		state.chunkID,
	).Scan(&state.sealedContainers); err != nil {
		t.Fatalf("count sealed containers: %v", err)
	}
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM container ctr
		 WHERE ctr.id IN (
		   SELECT DISTINCT COALESCE(sb.container_id, b.container_id)
		   FROM chunk c
		   LEFT JOIN blocks b ON b.chunk_id = c.id
		   LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		   LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		   WHERE c.id = $1
		 )
		 AND ctr.quarantine = TRUE`,
		state.chunkID,
	).Scan(&state.quarantinedConts); err != nil {
		t.Fatalf("count quarantined containers: %v", err)
	}
	if state.packedMappings > 0 && state.legacyMappings > 0 {
		valid, err := validateReusableChunkCompanionMappingWithContext(context.Background(), dbconn, state.chunkID)
		if err != nil {
			t.Fatalf("validate companion mapping: %v", err)
		}
		state.validCompanionState = valid
	}
	return state
}

func assertInterleavingChunkFinalState(
	t *testing.T,
	dbconn *sql.DB,
	containersDir string,
	chunkHash string,
	size int,
	want interleavingChunkFinalState,
) {
	t.Helper()
	got := loadInterleavingChunkFinalState(t, dbconn, chunkHash, size)
	if got.chunkStatus != want.chunkStatus ||
		got.packedMappings != want.packedMappings ||
		got.legacyMappings != want.legacyMappings ||
		got.logicalFileRefs != want.logicalFileRefs ||
		got.physicalFileRefs != want.physicalFileRefs ||
		got.storageBlockRefs != want.storageBlockRefs ||
		got.storageBlocks != want.storageBlocks ||
		got.orphanStorageBlocks != want.orphanStorageBlocks ||
		got.sealedContainers != want.sealedContainers ||
		got.quarantinedConts != want.quarantinedConts ||
		got.validCompanionState != want.validCompanionState {
		t.Fatalf("unexpected final state: got=%+v want=%+v", got, want)
	}
	if got.packedMappings > 0 && got.legacyMappings > 0 && !got.validCompanionState {
		t.Fatalf("invalid dual mapping survived: %+v", got)
	}
	if err := verifypkg.VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("full verify after final-state assertion: %v", err)
	}
}

type packedFixtureChunkSeed struct {
	chunkID         int64
	hash            string
	size            int
	blockID         int64
	containerID     int64
	containerOffset int64
	offsetInBlock   int64
}

type packedFixtureChunkSpec struct {
	hash          string
	payload       []byte
	liveRefCount  int
	withCompanion bool
}

func seedPackedFixtureBlock(
	t *testing.T,
	dbconn *sql.DB,
	containersDir string,
	specs ...packedFixtureChunkSpec,
) []packedFixtureChunkSeed {
	t.Helper()

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get plain transformer for packed fixture: %v", err)
	}

	builder := blocks.NewBlockBuilder(1 << 20)
	seeds := make([]packedFixtureChunkSeed, 0, len(specs))
	for _, spec := range specs {
		chunkID := insertReusableTestChunk(t, dbconn, spec.hash, filestate.ChunkProcessing)
		if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = $1 WHERE id = $2`, spec.liveRefCount, chunkID); err != nil {
			t.Fatalf("set live_ref_count for packed fixture chunk %s: %v", spec.hash, err)
		}
		if err := builder.Add(blocks.PendingChunk{
			ChunkID: chunkID,
			Data:    spec.payload,
			Size:    int64(len(spec.payload)),
		}); err != nil {
			t.Fatalf("build packed fixture chunk %s: %v", spec.hash, err)
		}
		seeds = append(seeds, packedFixtureChunkSeed{
			chunkID: chunkID,
			hash:    spec.hash,
			size:    len(spec.payload),
		})
	}

	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin packed fixture tx: %v", err)
	}
	persisted, err := storePackedBlockWithWriter(context.Background(), tx, writer, transformer, storeRuntimeCompression{}, builder)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("store packed fixture block: %v", err)
	}

	for i, spec := range specs {
		segment, ok := persisted.Segments[seeds[i].chunkID]
		if !ok {
			_ = tx.Rollback()
			t.Fatalf("missing packed fixture segment for chunk %s", spec.hash)
		}
		if _, err := tx.Exec(
			`UPDATE chunk SET status = $1 WHERE id = $2`,
			filestate.ChunkCompleted,
			seeds[i].chunkID,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("mark packed fixture chunk completed: %v", err)
		}
		if spec.withCompanion {
			if err := insertLegacyCompanionBlockRowWithContext(
				context.Background(),
				tx,
				seeds[i].chunkID,
				"plain",
				[]byte{},
				persisted.Placement.ContainerID,
				persisted.Placement.Offset+segment.Offset,
				int64(len(spec.payload)),
				int64(len(spec.payload)),
			); err != nil {
				_ = tx.Rollback()
				t.Fatalf("insert packed fixture companion row: %v", err)
			}
		}
		seeds[i].blockID = persisted.BlockID
		seeds[i].containerID = persisted.Placement.ContainerID
		seeds[i].containerOffset = persisted.Placement.Offset
		seeds[i].offsetInBlock = segment.Offset
	}

	if err := container.UpdateContainerSize(tx, persisted.Placement.ContainerID, persisted.Placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update packed fixture container size: %v", err)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("commit packed fixture tx: %v", err)
	}
	acknowledgeWriterAppendCommitted(writer)
	return seeds
}

func startInterleavingStore(
	t *testing.T,
	sgctx StorageContext,
	path string,
	codec blocks.Codec,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
		done <- err
	}()
	return done
}

func waitStoreDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("store failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for store completion")
	}
}

func TestStoreInterleavingSameChunkClaimDoesNotForkPlacement(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "same.bin")
	payload := []byte("deterministic-same-chunk-payload")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	coord := newStoreInterleavingCoordinator()
	sgctxA := newInterleavingStorageContext(dbconn, workDir, payload)
	sgctxB := newInterleavingStorageContext(dbconn, workDir, payload)
	installInterleavingHooksOnContexts(t, coord.Hook, &sgctxA, &sgctxB)

	hash := storeInterleavingHash(payload)
	gate := coord.Hold(func(event TestStoreInterleavingHookEvent) bool {
		return event.Event == TestStoreInterleavingEventAfterChunkClaim && event.ChunkHash == hash
	})
	t.Cleanup(gate.Release)

	doneA := startInterleavingStore(t, sgctxA, inPath, blocks.CodecPlain)
	first := gate.Await(t)
	doneB := startInterleavingStore(t, sgctxB, inPath, blocks.CodecPlain)

	var chunkRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash = $1 AND size = $2`, hash, len(payload)).Scan(&chunkRows); err != nil {
		t.Fatalf("count chunk rows while first worker paused: %v", err)
	}
	if chunkRows != 1 {
		t.Fatalf("expected one chunk row while first worker is paused, got %d", chunkRows)
	}

	gate.Release()
	waitStoreDone(t, doneA)
	waitStoreDone(t, doneB)

	assertInterleavingChunkFinalState(t, dbconn, workDir, hash, len(payload), interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      1,
		logicalFileRefs:     1,
		physicalFileRefs:    1,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: true,
	})

	if first.StoreOpID == "" {
		t.Fatal("expected first interleaving event to include store op id")
	}
}

func TestStoreInterleavingPackedMetadataIsInvisibleBeforeCommit(t *testing.T) {
	dbconn, observer := openSharedStoreInterleavingDB(t)
	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "packed-invisible.bin")
	payload := []byte("packed-metadata-visibility-payload")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	coord := newStoreInterleavingCoordinator()
	sgctx := newInterleavingStorageContext(dbconn, workDir, payload)
	installInterleavingHooksOnContexts(t, coord.Hook, &sgctx)

	hash := storeInterleavingHash(payload)
	gate := coord.Hold(func(event TestStoreInterleavingHookEvent) bool {
		return event.Event == TestStoreInterleavingEventAfterPackedMetadata && event.ChunkHash == hash
	})
	t.Cleanup(gate.Release)

	done := startInterleavingStore(t, sgctx, inPath, blocks.CodecPlain)
	gate.Await(t)

	var packedRows int
	if err := observer.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&packedRows); err != nil {
		t.Fatalf("count chunk_block_refs before commit: %v", err)
	}
	if packedRows != 0 {
		t.Fatalf("expected no committed packed mappings before commit, got %d", packedRows)
	}

	var legacyRows int
	if err := observer.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&legacyRows); err != nil {
		t.Fatalf("count blocks before commit: %v", err)
	}
	if legacyRows != 0 {
		t.Fatalf("expected no committed legacy rows before commit, got %d", legacyRows)
	}

	gate.Release()
	waitStoreDone(t, done)

	assertInterleavingChunkFinalState(t, dbconn, workDir, hash, len(payload), interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      1,
		logicalFileRefs:     1,
		physicalFileRefs:    1,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: true,
	})
}

func TestStoreInterleavingCompanionIsInvisibleBeforeCommit(t *testing.T) {
	dbconn, observer := openSharedStoreInterleavingDB(t)
	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "companion-invisible.bin")
	payload := []byte("companion-visibility-payload")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	coord := newStoreInterleavingCoordinator()
	sgctx := newInterleavingStorageContext(dbconn, workDir, payload)
	installInterleavingHooksOnContexts(t, coord.Hook, &sgctx)

	hash := storeInterleavingHash(payload)
	gate := coord.Hold(func(event TestStoreInterleavingHookEvent) bool {
		return event.Event == TestStoreInterleavingEventAfterLegacyCompanionInsert && event.ChunkHash == hash
	})
	t.Cleanup(gate.Release)

	done := startInterleavingStore(t, sgctx, inPath, blocks.CodecPlain)
	gate.Await(t)

	var packedRows int
	if err := observer.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&packedRows); err != nil {
		t.Fatalf("count chunk_block_refs before companion commit: %v", err)
	}
	if packedRows != 0 {
		t.Fatalf("expected no committed packed mappings before companion commit, got %d", packedRows)
	}

	var legacyRows int
	if err := observer.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&legacyRows); err != nil {
		t.Fatalf("count blocks before companion commit: %v", err)
	}
	if legacyRows != 0 {
		t.Fatalf("expected no committed legacy rows before companion commit, got %d", legacyRows)
	}

	gate.Release()
	waitStoreDone(t, done)

	assertInterleavingChunkFinalState(t, dbconn, workDir, hash, len(payload), interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      1,
		logicalFileRefs:     1,
		physicalFileRefs:    1,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: true,
	})
}

func TestStoreInterleavingRebuildCleanupDeletesBothMappings(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	containerID := insertReusableTestContainer(t, dbconn, "rebuild-cleanup.bin", false)
	containerPath := filepath.Join(containersDir, "rebuild-cleanup.bin")
	if err := writeReusableTestContainerFileWithPayload(containerPath, make([]byte, 256)); err != nil {
		t.Fatalf("write reusable container file: %v", err)
	}

	chunkHash := "rebuild-cleanup-hash"
	chunkID := insertReusableTestChunk(t, dbconn, chunkHash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 0 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("zero live_ref_count for rebuild cleanup fixture: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, 64, 64, $2, 65)`,
		chunkID,
		containerID,
	); err != nil {
		t.Fatalf("insert invalid legacy row: %v", err)
	}

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', 64, 64, $1, 64, x'00')
		 RETURNING id`,
		containerID,
	).Scan(&blockID); err != nil {
		t.Fatalf("insert packed block row: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, 0, 64)`,
		chunkID,
		blockID,
	); err != nil {
		t.Fatalf("insert packed ref: %v", err)
	}

	var events []TestStoreInterleavingHookEvent
	ctx := withStoreInterleavingState(context.Background(), &storeInterleavingState{
		hooks: &storeInterleavingHooks{
			onEvent: func(_ context.Context, event storeInterleavingHookEvent) error {
				events = append(events, event)
				return nil
			},
		},
		storeOpID: "rebuild-cleanup-op",
		codec:     "plain",
		fileHash:  chunkHash,
	})
	claimedID, claimedStatus, isNew, err := claimChunkWithContext(ctx, dbconn, chunkHash, 64, string(chunk.DefaultChunkerVersion), containersDir)
	if err != nil {
		t.Fatalf("claim invalid reusable chunk: %v", err)
	}
	if claimedID != chunkID || isNew || claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected reclaim result: id=%d status=%s isNew=%v", claimedID, claimedStatus, isNew)
	}

	var packedRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1`, chunkID).Scan(&packedRows); err != nil {
		t.Fatalf("count packed rows after rebuild cleanup: %v", err)
	}
	if packedRows != 0 {
		t.Fatalf("expected packed rows to be deleted during rebuild cleanup, got %d", packedRows)
	}

	var legacyRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, chunkID).Scan(&legacyRows); err != nil {
		t.Fatalf("count legacy rows after rebuild cleanup: %v", err)
	}
	if legacyRows != 0 {
		t.Fatalf("expected legacy rows to be deleted during rebuild cleanup, got %d", legacyRows)
	}

	if len(events) < 2 || events[0].Event != TestStoreInterleavingEventBeforeMarkChunkForRebuild || events[1].Event != TestStoreInterleavingEventAfterMarkChunkForRebuild {
		t.Fatalf("expected rebuild cleanup hook sequence, got %+v", events)
	}
	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, 64, interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkProcessing,
		packedMappings:      0,
		legacyMappings:      0,
		logicalFileRefs:     0,
		physicalFileRefs:    0,
		storageBlockRefs:    0,
		storageBlocks:       0,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: false,
	})
}

func TestStoreInterleavingRebuildCleanupRetainsSharedStorageBlock(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	losingPayload := bytes.Repeat([]byte("L"), 64)
	winningPayload := bytes.Repeat([]byte("W"), 64)
	losingHash := storeInterleavingHash(losingPayload)
	winningHash := storeInterleavingHash(winningPayload)
	seeds := seedPackedFixtureBlock(
		t,
		dbconn,
		containersDir,
		packedFixtureChunkSpec{
			hash:          losingHash,
			payload:       losingPayload,
			liveRefCount:  0,
			withCompanion: false,
		},
		packedFixtureChunkSpec{
			hash:          winningHash,
			payload:       winningPayload,
			liveRefCount:  0,
			withCompanion: false,
		},
	)
	losingChunkID := seeds[0].chunkID
	blockID := seeds[0].blockID

	if err := markChunkForRebuildWithContext(context.Background(), dbconn, losingChunkID); err != nil {
		t.Fatalf("mark losing chunk for rebuild: %v", err)
	}

	var losingPacked int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1`, losingChunkID).Scan(&losingPacked); err != nil {
		t.Fatalf("count losing packed rows: %v", err)
	}
	if losingPacked != 0 {
		t.Fatalf("expected losing chunk packed rows removed, got %d", losingPacked)
	}

	var losingLegacy int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, losingChunkID).Scan(&losingLegacy); err != nil {
		t.Fatalf("count losing legacy rows: %v", err)
	}
	if losingLegacy != 0 {
		t.Fatalf("expected losing chunk legacy rows removed, got %d", losingLegacy)
	}

	var blockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, blockID).Scan(&blockRows); err != nil {
		t.Fatalf("count shared storage block rows: %v", err)
	}
	if blockRows != 1 {
		t.Fatalf("expected shared storage block to remain for winner, got %d rows", blockRows)
	}

	// This synthetic fixture intentionally seeds one packed payload that still
	// encodes both chunks, then removes only one chunk_block_refs row. That
	// leaves the surviving storage_blocks row still referenced, which is the
	// cleanup-retention property under test, but the packed payload can no longer
	// satisfy full payload-level verification for the deleted chunk entry.
	gotWinner := loadInterleavingChunkFinalState(t, dbconn, winningHash, 64)
	wantWinner := interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      0,
		logicalFileRefs:     0,
		physicalFileRefs:    0,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: false,
	}
	if gotWinner.chunkStatus != wantWinner.chunkStatus ||
		gotWinner.packedMappings != wantWinner.packedMappings ||
		gotWinner.legacyMappings != wantWinner.legacyMappings ||
		gotWinner.logicalFileRefs != wantWinner.logicalFileRefs ||
		gotWinner.physicalFileRefs != wantWinner.physicalFileRefs ||
		gotWinner.storageBlockRefs != wantWinner.storageBlockRefs ||
		gotWinner.storageBlocks != wantWinner.storageBlocks ||
		gotWinner.orphanStorageBlocks != wantWinner.orphanStorageBlocks ||
		gotWinner.sealedContainers != wantWinner.sealedContainers ||
		gotWinner.quarantinedConts != wantWinner.quarantinedConts ||
		gotWinner.validCompanionState != wantWinner.validCompanionState {
		t.Fatalf("unexpected shared-reference survivor state: got=%+v want=%+v", gotWinner, wantWinner)
	}

	var losingStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, losingChunkID).Scan(&losingStatus); err != nil {
		t.Fatalf("load losing chunk status: %v", err)
	}
	if losingStatus != filestate.ChunkAborted {
		t.Fatalf("expected losing chunk status ABORTED, got %s", losingStatus)
	}
}

func TestStoreInterleavingRebuildCleanupRollbackRestoresRows(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	rollbackPayload := bytes.Repeat([]byte("R"), 64)
	chunkHash := storeInterleavingHash(rollbackPayload)
	seeds := seedPackedFixtureBlock(
		t,
		dbconn,
		containersDir,
		packedFixtureChunkSpec{
			hash:          chunkHash,
			payload:       rollbackPayload,
			liveRefCount:  0,
			withCompanion: true,
		},
	)
	chunkID := seeds[0].chunkID
	if _, err := dbconn.Exec(
		`CREATE TRIGGER fail_storage_block_delete
		 BEFORE DELETE ON storage_blocks
		 BEGIN
		   SELECT RAISE(ABORT, 'forced rebuild rollback');
		 END`,
	); err != nil {
		t.Fatalf("create rollback trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = dbconn.Exec(`DROP TRIGGER IF EXISTS fail_storage_block_delete`)
	})

	err := markChunkForRebuildWithContext(context.Background(), dbconn, chunkID)
	if err == nil || !strings.Contains(err.Error(), "forced rebuild rollback") {
		t.Fatalf("expected forced rollback error, got %v", err)
	}

	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, 64, interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      1,
		logicalFileRefs:     0,
		physicalFileRefs:    0,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: true,
	})
}

func TestStoreInterleavingRebuildCleanupIsIdempotent(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	containerID := insertReusableTestContainer(t, dbconn, "idempotent-rebuild.bin", false)
	containerPath := filepath.Join(containersDir, "idempotent-rebuild.bin")
	if err := writeReusableTestContainerFileWithPayload(containerPath, make([]byte, 256)); err != nil {
		t.Fatalf("write idempotent rebuild fixture: %v", err)
	}

	chunkHash := "idempotent-rebuild-hash"
	chunkID := insertReusableTestChunk(t, dbconn, chunkHash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 0 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("zero live_ref_count for idempotence fixture: %v", err)
	}

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', 64, 64, $1, 64, zeroblob(32))
		 RETURNING id`,
		containerID,
	).Scan(&blockID); err != nil {
		t.Fatalf("insert idempotence storage block: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, 0, 64)`,
		chunkID, blockID,
	); err != nil {
		t.Fatalf("insert idempotence packed ref: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, 64, 64, $2, 64)`,
		chunkID, containerID,
	); err != nil {
		t.Fatalf("insert idempotence legacy row: %v", err)
	}

	if err := markChunkForRebuildWithContext(context.Background(), dbconn, chunkID); err != nil {
		t.Fatalf("first rebuild cleanup: %v", err)
	}
	if err := markChunkForRebuildWithContext(context.Background(), dbconn, chunkID); err != nil {
		t.Fatalf("second rebuild cleanup: %v", err)
	}

	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, 64, interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkAborted,
		packedMappings:      0,
		legacyMappings:      0,
		logicalFileRefs:     0,
		physicalFileRefs:    0,
		storageBlockRefs:    0,
		storageBlocks:       0,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: false,
	})
}

func TestStoreInterleavingRetryClaimSignalsCASBoundary(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	if db.BackendFromDB(dbconn) == db.BackendSQLite {
		t.Skip("sqlite cannot exercise nested retry-CAS transaction flow without backend lock contention; postgres parity test covers this path")
	}
	hash := "retry-claim-hash"
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, 0, 0, $4)
		 RETURNING id`,
		hash,
		64,
		filestate.ChunkAborted,
		string(chunk.DefaultChunkerVersion),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert aborted chunk: %v", err)
	}

	var events []TestStoreInterleavingHookEvent
	ctx := withStoreInterleavingState(context.Background(), &storeInterleavingState{
		hooks: &storeInterleavingHooks{
			onEvent: func(_ context.Context, event storeInterleavingHookEvent) error {
				events = append(events, event)
				return nil
			},
		},
		storeOpID: "retry-claim-op",
		codec:     "plain",
		fileHash:  hash,
	})
	claimedID, claimedStatus, isNew, err := claimChunkWithContext(ctx, dbconn, hash, 64, string(chunk.DefaultChunkerVersion), t.TempDir())
	if err != nil {
		t.Fatalf("claim aborted chunk: %v", err)
	}
	if claimedID != chunkID || isNew || claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected retry claim result: id=%d status=%s isNew=%v", claimedID, claimedStatus, isNew)
	}

	var sawRetryCAS bool
	for _, event := range events {
		if event.ChunkID != chunkID {
			continue
		}
		if event.Event == TestStoreInterleavingEventBeforeChunkRetryCAS {
			sawRetryCAS = true
		}
	}
	if !sawRetryCAS {
		t.Fatalf("expected retry claim to hit CAS boundary, events=%+v", events)
	}
}
