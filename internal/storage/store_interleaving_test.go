package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

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
