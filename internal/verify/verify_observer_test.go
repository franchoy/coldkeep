package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"sync"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/catalog"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

func compressedObserverFixture(t *testing.T, blockID int64) (BlockStorageMetadata, staticContainerReader) {
	t.Helper()
	logicalPayload := buildPipelineEncodedBytes(t, []byte("verification-observer-payload"))
	zstd, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	compressedPayload, err := zstd.Compress(logicalPayload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	level := 3
	return BlockStorageMetadata{
		BlockID: blockID, ContainerID: blockID + 1000, ContainerOffset: 64,
		ContainerName: "observer.ck", ContainerMaxSize: 1 << 20,
		FormatVersion: 1, Codec: "none", PlaintextSize: int64(len(logicalPayload)),
		StoredSize: int64(len(compressedPayload)), CompressionCodec: "zstd", CompressionLevel: &level,
		LogicalHash: blocks.HashLogical(logicalPayload), CompressedHash: blocks.HashCompressed(compressedPayload),
		PhysicalHash: blocks.HashPhysical(compressedPayload),
	}, staticContainerReader{payload: compressedPayload}
}

func TestVerificationStageObserverNilPreservesBehavior(t *testing.T) {
	t.Parallel()
	meta, reader := compressedObserverFixture(t, 301)
	want, err := VerifyStoredBlock(context.Background(), meta, reader)
	if err != nil {
		t.Fatalf("VerifyStoredBlock default: %v", err)
	}
	got, err := VerifyStoredBlock(withVerificationStageObserver(context.Background(), nil), meta, reader)
	if err != nil {
		t.Fatalf("VerifyStoredBlock nil observer: %v", err)
	}
	if !bytes.Equal(got.LogicalPayload, want.LogicalPayload) || !reflect.DeepEqual(got.Metadata, want.Metadata) {
		t.Fatalf("nil observer changed verification result")
	}
}

func TestVerificationStageObserverReportsSuccessfulPackedStages(t *testing.T) {
	t.Parallel()
	meta, reader := compressedObserverFixture(t, 302)
	var got []verificationStageObservation
	ctx := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) {
		got = append(got, observation)
	})
	if _, err := VerifyStoredBlock(ctx, meta, reader); err != nil {
		t.Fatalf("VerifyStoredBlock: %v", err)
	}
	wantStages := []verificationObservedStage{
		verificationObservedPhysicalHash,
		verificationObservedCompressedHash,
		verificationObservedDecompression,
		verificationObservedLogicalHash,
		verificationObservedBlockComplete,
	}
	if len(got) != len(wantStages) {
		t.Fatalf("observations = %+v, want stages %v", got, wantStages)
	}
	for i, wantStage := range wantStages {
		if got[i].Layout != verificationLayoutPacked || got[i].BlockID != meta.BlockID || got[i].Stage != wantStage {
			t.Fatalf("observation[%d] = %+v, want packed block %d stage %s", i, got[i], meta.BlockID, wantStage)
		}
	}
}

func TestVerificationStageObserverDoesNotReportFailedStage(t *testing.T) {
	t.Parallel()
	meta, reader := compressedObserverFixture(t, 303)
	meta.LogicalHash = bytes.Repeat([]byte{0xff}, sha256.Size)
	var got []verificationObservedStage
	ctx := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) {
		got = append(got, observation.Stage)
	})
	if _, err := VerifyStoredBlock(ctx, meta, reader); err == nil {
		t.Fatal("VerifyStoredBlock unexpectedly succeeded")
	}
	want := []verificationObservedStage{
		verificationObservedPhysicalHash,
		verificationObservedCompressedHash,
		verificationObservedDecompression,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observed stages = %v, want %v", got, want)
	}
}

func TestVerificationStageObserverIsInvocationScoped(t *testing.T) {
	t.Parallel()
	metaA, readerA := compressedObserverFixture(t, 304)
	metaB, readerB := compressedObserverFixture(t, 305)
	var observedA, observedB []verificationStageObservation
	ctxA := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) {
		observedA = append(observedA, observation)
	})
	ctxB := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) {
		observedB = append(observedB, observation)
	})
	if _, err := VerifyStoredBlock(ctxA, metaA, readerA); err != nil {
		t.Fatalf("invocation A: %v", err)
	}
	if _, err := VerifyStoredBlock(ctxB, metaB, readerB); err != nil {
		t.Fatalf("invocation B: %v", err)
	}
	for _, observation := range observedA {
		if observation.BlockID != metaA.BlockID {
			t.Fatalf("invocation A received %+v", observation)
		}
	}
	for _, observation := range observedB {
		if observation.BlockID != metaB.BlockID {
			t.Fatalf("invocation B received %+v", observation)
		}
	}
}

func TestVerificationStageObserverDoesNotCrossConcurrentInvocations(t *testing.T) {
	t.Parallel()
	metaA, readerA := compressedObserverFixture(t, 306)
	metaB, readerB := compressedObserverFixture(t, 307)
	observedA := make(chan verificationStageObservation, 8)
	observedB := make(chan verificationStageObservation, 8)
	ctxA := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) { observedA <- observation })
	ctxB := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) { observedB <- observation })
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := VerifyStoredBlock(ctxA, metaA, readerA)
		errCh <- err
	}()
	go func() {
		defer wg.Done()
		_, err := VerifyStoredBlock(ctxB, metaB, readerB)
		errCh <- err
	}()
	wg.Wait()
	close(errCh)
	close(observedA)
	close(observedB)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent verification: %v", err)
		}
	}
	for observation := range observedA {
		if observation.BlockID != metaA.BlockID {
			t.Fatalf("observer A received %+v", observation)
		}
	}
	for observation := range observedB {
		if observation.BlockID != metaB.BlockID {
			t.Fatalf("observer B received %+v", observation)
		}
	}
}

func TestVerificationStageObserverReportsSuccessfulLegacyStages(t *testing.T) {
	t.Parallel()
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()
	payload := []byte("legacy-observer-payload")
	chunkID := seedVerifyLegacyBlockFixture(t, dbconn, containersDir, payload)
	var blockID, containerID, offset, plaintextSize, storedSize, maxSize int64
	var codec, filename string
	var formatVersion int
	var nonce []byte
	if err := dbconn.QueryRow(`
		SELECT b.id, b.codec, b.format_version, b.plaintext_size, b.stored_size,
		       b.nonce, b.container_id, b.block_offset, c.filename, c.max_size
		FROM blocks b JOIN container c ON c.id = b.container_id WHERE b.chunk_id = $1
	`, chunkID).Scan(&blockID, &codec, &formatVersion, &plaintextSize, &storedSize, &nonce, &containerID, &offset, &filename, &maxSize); err != nil {
		t.Fatalf("load legacy placement: %v", err)
	}
	hash := sha256.Sum256(payload)
	placement := catalog.ChunkPlacementRef{
		ChunkID: chunkID, ChunkHash: hex.EncodeToString(hash[:]), ChunkSize: int64(len(payload)),
		Kind: catalog.PlacementLegacy,
		Legacy: &catalog.LegacyChunkPlacement{
			BlockID: blockID, Codec: codec, FormatVersion: formatVersion,
			PlaintextSize: plaintextSize, StoredSize: storedSize, Nonce: nonce,
			Container:       catalog.ContainerPlacementRef{ID: containerID, Filename: filename, MaxSize: maxSize},
			ContainerOffset: offset,
		},
	}
	var got []verificationStageObservation
	ctx := withVerificationStageObserver(context.Background(), func(observation verificationStageObservation) { got = append(got, observation) })
	state := filePlacementVerifyState{containersDir: containersDir, packedBlocks: make(map[int64]*VerifiedBlock)}
	if err := state.verify(ctx, placement); err != nil {
		t.Fatalf("verify legacy placement: %v", err)
	}
	want := []verificationStageObservation{
		{Layout: verificationLayoutLegacy, BlockID: blockID, Stage: verificationObservedLogicalHash},
		{Layout: verificationLayoutLegacy, BlockID: blockID, Stage: verificationObservedBlockComplete},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy observations = %+v, want %+v", got, want)
	}
}
