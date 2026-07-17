package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
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
	dbconn := openStoreInterleavingDB(t, dsn, "primary")
	if _, err := dbconn.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("enable sqlite wal mode: %v", err)
	}
	if err := db.ApplySQLiteSessionPragmas(dbconn); err != nil {
		t.Fatalf("configure primary sqlite session: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	observer := openStoreInterleavingDB(t, dsn, "observer")
	if err := db.ApplySQLiteSessionPragmas(observer); err != nil {
		t.Fatalf("configure observer sqlite session: %v", err)
	}
	return dbconn, observer
}

func openStoreInterleavingDB(t *testing.T, dsn, role string) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open %s sqlite db: %v", role, err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })
	return dbconn
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
	mustScanInterleavingRow(t, "load chunk row for final state", dbconn.QueryRow(
		`SELECT id, status FROM chunk WHERE chunk_hash = $1 AND size = $2`, chunkHash, size,
	), &state.chunkID, &state.chunkStatus)
	state.packedMappings = scanInterleavingCount(t, "count packed mappings", dbconn.QueryRow(
		`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1`, state.chunkID,
	))
	state.legacyMappings = scanInterleavingCount(t, "count legacy mappings", dbconn.QueryRow(
		`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, state.chunkID,
	))
	state.logicalFileRefs = scanInterleavingCount(t, "count logical file refs", dbconn.QueryRow(
		`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, state.chunkID,
	))
	state.physicalFileRefs = scanInterleavingCount(t, "count physical file refs", dbconn.QueryRow(
		`SELECT COUNT(*) FROM physical_file pf WHERE pf.logical_file_id IN (
			SELECT DISTINCT logical_file_id FROM file_chunk WHERE chunk_id = $1
		)`, state.chunkID,
	))
	state.storageBlockRefs = scanInterleavingCount(t, "count storage block refs", dbconn.QueryRow(
		`SELECT COUNT(*) FROM storage_blocks sb
		 JOIN chunk_block_refs r ON r.block_id = sb.id WHERE r.chunk_id = $1`, state.chunkID,
	))
	state.storageBlocks = scanInterleavingCount(t, "count storage blocks", dbconn.QueryRow(
		`SELECT COUNT(*) FROM storage_blocks sb WHERE EXISTS (
			SELECT 1 FROM chunk_block_refs r WHERE r.block_id = sb.id AND r.chunk_id = $1
		)`, state.chunkID,
	))
	state.orphanStorageBlocks = scanInterleavingCount(t, "count orphan storage blocks", dbconn.QueryRow(
		`SELECT COUNT(*) FROM storage_blocks sb WHERE NOT EXISTS (
			SELECT 1 FROM chunk_block_refs r WHERE r.block_id = sb.id
		)`,
	))
	state.sealedContainers = scanInterleavingSealedContainerCount(t, dbconn, state.chunkID)
	state.quarantinedConts = scanInterleavingQuarantinedContainerCount(t, dbconn, state.chunkID)
	state.validCompanionState = loadInterleavingCompanionState(t, dbconn, state)
	return state
}

func mustScanInterleavingRow(t *testing.T, label string, row *sql.Row, destinations ...any) {
	t.Helper()
	if err := row.Scan(destinations...); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

func scanInterleavingCount(t *testing.T, label string, row *sql.Row) int {
	t.Helper()
	var count int
	mustScanInterleavingRow(t, label, row, &count)
	return count
}

func scanInterleavingSealedContainerCount(t *testing.T, dbconn *sql.DB, chunkID int64) int {
	t.Helper()
	return scanInterleavingCount(t, "count sealed containers", dbconn.QueryRow(
		`SELECT COUNT(*) FROM container ctr WHERE ctr.id IN (
		SELECT DISTINCT COALESCE(sb.container_id, b.container_id)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE c.id = $1
	) AND ctr.sealed = TRUE`, chunkID,
	))
}

func scanInterleavingQuarantinedContainerCount(t *testing.T, dbconn *sql.DB, chunkID int64) int {
	t.Helper()
	return scanInterleavingCount(t, "count quarantined containers", dbconn.QueryRow(
		`SELECT COUNT(*) FROM container ctr WHERE ctr.id IN (
		SELECT DISTINCT COALESCE(sb.container_id, b.container_id)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE c.id = $1
	) AND ctr.quarantine = TRUE`, chunkID,
	))
}

func loadInterleavingCompanionState(t *testing.T, dbconn *sql.DB, state interleavingChunkFinalState) bool {
	t.Helper()
	if state.packedMappings == 0 || state.legacyMappings == 0 {
		return false
	}
	valid, err := validateReusableChunkCompanionMappingWithContext(context.Background(), dbconn, state.chunkID)
	if err != nil {
		t.Fatalf("validate companion mapping: %v", err)
	}
	return valid
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
	if comparableInterleavingState(got) != comparableInterleavingState(want) {
		t.Fatalf("unexpected final state: got=%+v want=%+v", got, want)
	}
	if got.packedMappings > 0 && got.legacyMappings > 0 && !got.validCompanionState {
		t.Fatalf("invalid dual mapping survived: %+v", got)
	}
	if err := verifypkg.VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("full verify after final-state assertion: %v", err)
	}
}

func comparableInterleavingState(state interleavingChunkFinalState) interleavingChunkFinalState {
	state.chunkID = 0
	return state
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
	transformer := mustGetPackedFixtureTransformer(t)
	builder, seeds := buildPackedFixtureChunks(t, dbconn, specs)
	tx := beginPackedFixtureTransaction(t, dbconn)
	persisted := persistPackedFixtureBlock(t, tx, writer, transformer, builder)
	populatePackedFixtureSeeds(t, tx, persisted, specs, seeds)
	commitPackedFixture(t, tx, writer, persisted)
	return seeds
}

func mustGetPackedFixtureTransformer(t *testing.T) blocks.Transformer {
	t.Helper()
	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get plain transformer for packed fixture: %v", err)
	}
	return transformer
}

func buildPackedFixtureChunks(t *testing.T, dbconn *sql.DB, specs []packedFixtureChunkSpec) (*blocks.BlockBuilder, []packedFixtureChunkSeed) {
	t.Helper()
	builder := blocks.NewBlockBuilder(1 << 20)
	seeds := make([]packedFixtureChunkSeed, 0, len(specs))
	for _, spec := range specs {
		chunkID := insertReusableTestChunk(t, dbconn, spec.hash, filestate.ChunkProcessing)
		if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = $1 WHERE id = $2`, spec.liveRefCount, chunkID); err != nil {
			t.Fatalf("set live_ref_count for packed fixture chunk %s: %v", spec.hash, err)
		}
		if err := builder.Add(blocks.PendingChunk{ChunkID: chunkID, Data: spec.payload, Size: int64(len(spec.payload))}); err != nil {
			t.Fatalf("build packed fixture chunk %s: %v", spec.hash, err)
		}
		seeds = append(seeds, packedFixtureChunkSeed{chunkID: chunkID, hash: spec.hash, size: len(spec.payload)})
	}
	return builder, seeds
}

func beginPackedFixtureTransaction(t *testing.T, dbconn *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin packed fixture tx: %v", err)
	}
	return tx
}

func persistPackedFixtureBlock(t *testing.T, tx *sql.Tx, writer payloadStatefulWriter, transformer blocks.Transformer, builder *blocks.BlockBuilder) packedBlockPersistResult {
	t.Helper()
	persisted, err := storePackedBlockWithWriter(context.Background(), tx, writer, transformer, storeRuntimeCompression{}, builder)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("store packed fixture block: %v", err)
	}
	return persisted
}

func populatePackedFixtureSeeds(t *testing.T, tx *sql.Tx, persisted packedBlockPersistResult, specs []packedFixtureChunkSpec, seeds []packedFixtureChunkSeed) {
	t.Helper()
	for i, spec := range specs {
		segment, ok := persisted.Segments[seeds[i].chunkID]
		if !ok {
			_ = tx.Rollback()
			t.Fatalf("missing packed fixture segment for chunk %s", spec.hash)
		}
		markPackedFixtureChunkCompleted(t, tx, persisted, segment, spec, seeds[i].chunkID)
		seeds[i].blockID = persisted.BlockID
		seeds[i].containerID = persisted.Placement.ContainerID
		seeds[i].containerOffset = persisted.Placement.Offset
		seeds[i].offsetInBlock = segment.Offset
	}
}

func markPackedFixtureChunkCompleted(t *testing.T, tx *sql.Tx, persisted packedBlockPersistResult, segment packedChunkSegment, spec packedFixtureChunkSpec, chunkID int64) {
	t.Helper()
	if _, err := tx.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("mark packed fixture chunk completed: %v", err)
	}
	if !spec.withCompanion {
		return
	}
	if err := insertLegacyCompanionBlockRowWithContext(
		context.Background(), tx, chunkID, "plain", []byte{}, persisted.Placement.ContainerID,
		persisted.Placement.Offset+segment.Offset, int64(len(spec.payload)), int64(len(spec.payload)),
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert packed fixture companion row: %v", err)
	}
}

func commitPackedFixture(t *testing.T, tx *sql.Tx, writer payloadStatefulWriter, persisted packedBlockPersistResult) {
	t.Helper()
	if err := container.UpdateContainerSize(tx, persisted.Placement.ContainerID, persisted.Placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update packed fixture container size: %v", err)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("commit packed fixture tx: %v", err)
	}
	acknowledgeWriterAppendCommitted(writer)
}

func startInterleavingStore(t *testing.T, sgctx StorageContext, path string, codec blocks.Codec) <-chan error {
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
