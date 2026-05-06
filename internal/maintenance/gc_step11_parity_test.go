package maintenance_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
)

func TestStep11DryRunPackedReclaimableMatchesActualGC(t *testing.T) {
	requireParityDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applyParitySchema(t, dbconn)
	resetParityDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	livePayload := []byte("step11-dryrun-live-packed-block")
	deadPayload := []byte("step11-dryrun-dead-packed-block")

	writeContainer := func(filename string, payload []byte) int64 {
		t.Helper()
		containerPath := filepath.Join(containersDir, filename)
		if err := os.WriteFile(containerPath, payload, 0o644); err != nil {
			t.Fatalf("write container file %s: %v", filename, err)
		}
		var containerID int64
		if err := dbconn.QueryRow(
			`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
			 VALUES ($1, $2, $3, TRUE, FALSE)
			 RETURNING id`,
			filename,
			int64(len(payload)),
			container.GetContainerMaxSize(),
		).Scan(&containerID); err != nil {
			t.Fatalf("insert container %s: %v", filename, err)
		}
		return containerID
	}

	liveContainerID := writeContainer("step11-dryrun-live.bin", livePayload)
	deadContainerID := writeContainer("step11-dryrun-dead.bin", deadPayload)

	insertChunk := func(hash string, size int64, liveRefCount int64) int64 {
		t.Helper()
		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
			 VALUES ($1, $2, 'COMPLETED', $3, 0, 'v2-fastcdc')
			 RETURNING id`,
			hash,
			size,
			liveRefCount,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return chunkID
	}

	liveHash := sha256.Sum256(livePayload)
	deadHash := sha256.Sum256(deadPayload)
	liveChunkID := insertChunk(hex.EncodeToString(liveHash[:]), int64(len(livePayload)), 1)
	deadChunkID := insertChunk(hex.EncodeToString(deadHash[:]), int64(len(deadPayload)), 0)

	insertPackedBlock := func(containerID int64, payload []byte, chunkID int64) {
		t.Helper()
		blockHash := sha256.Sum256(payload)
		var blockID int64
		if err := dbconn.QueryRow(
			`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
			 VALUES (1, 'none', $1, $2, $3, 0, $4)
			 RETURNING id`,
			int64(len(payload)),
			int64(len(payload)),
			containerID,
			blockHash[:],
		).Scan(&blockID); err != nil {
			t.Fatalf("insert storage_block: %v", err)
		}
		if _, err := dbconn.Exec(
			`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, 0, $3)`,
			chunkID,
			blockID,
			int64(len(payload)),
		); err != nil {
			t.Fatalf("insert chunk_block_ref: %v", err)
		}
	}

	insertPackedBlock(liveContainerID, livePayload, liveChunkID)
	insertPackedBlock(deadContainerID, deadPayload, deadChunkID)

	var blocksBefore int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksBefore); err != nil {
		t.Fatalf("count blocks before: %v", err)
	}
	var bytesBefore int64
	if err := dbconn.QueryRow(`SELECT COALESCE(SUM(stored_size), 0) FROM storage_blocks`).Scan(&bytesBefore); err != nil {
		t.Fatalf("sum bytes before: %v", err)
	}

	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("new observability service: %v", err)
	}
	simulated, err := svc.Simulate(context.Background(), observability.SimulationOptions{Kind: observability.SimulationKindGC})
	if err != nil {
		t.Fatalf("simulate gc: %v", err)
	}
	if simulated == nil || simulated.GC == nil {
		t.Fatal("expected gc simulation output")
	}

	gcResult, err := maintenance.RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if gcResult.AffectedContainers != int(simulated.GC.Summary.FullyReclaimableContainers) {
		t.Fatalf("affected containers = %d, want %d", gcResult.AffectedContainers, simulated.GC.Summary.FullyReclaimableContainers)
	}

	var blocksAfter int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksAfter); err != nil {
		t.Fatalf("count blocks after: %v", err)
	}
	var bytesAfter int64
	if err := dbconn.QueryRow(`SELECT COALESCE(SUM(stored_size), 0) FROM storage_blocks`).Scan(&bytesAfter); err != nil {
		t.Fatalf("sum bytes after: %v", err)
	}

	actualDeletedBlocks := blocksBefore - blocksAfter
	actualDeletedBytes := bytesBefore - bytesAfter

	if actualDeletedBlocks != int64(simulated.GC.Summary.PackedBlocksDead) {
		t.Fatalf("actual deleted packed blocks = %d, want %d", actualDeletedBlocks, simulated.GC.Summary.PackedBlocksDead)
	}
	if actualDeletedBytes != simulated.GC.Summary.PackedBytesReclaimable {
		t.Fatalf("actual deleted packed bytes = %d, want %d", actualDeletedBytes, simulated.GC.Summary.PackedBytesReclaimable)
	}
}
