package engine_test

import (
	"context"
	"os"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineGarbageCollectionPlanAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		result, err := eng.PlanGarbageCollection(context.Background(), engine.GarbageCollectionPlanRequest{})
		if err != nil {
			t.Fatalf("PlanGarbageCollection: %v", err)
		}
		if result.Summary != (engine.GarbageCollectionPlanSummary{}) || len(result.Containers) != 0 || len(result.Warnings) != 0 {
			t.Fatalf("empty repository plan=%+v", result)
		}
	})
}

func TestPhase8AccountingContractAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		codec := "none"
		if os.Getenv("COLDKEEP_CODEC") == "aes-gcm" {
			codec = "aes-gcm"
		}
		var currentLogicalID, snapshotLogicalID int64
		if err := backend.DB.QueryRow(
			`INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version) VALUES ($1, 1, $2, 2, 'COMPLETED', 'v2-fastcdc') RETURNING id`,
			"phase8-current.txt", "phase8-current-hash",
		).Scan(&currentLogicalID); err != nil {
			t.Fatalf("insert current logical file: %v", err)
		}
		if err := backend.DB.QueryRow(
			`INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version) VALUES ($1, 120, $2, 0, 'COMPLETED', 'v2-fastcdc') RETURNING id`,
			"phase8-snapshot.txt", "phase8-snapshot-hash",
		).Scan(&snapshotLogicalID); err != nil {
			t.Fatalf("insert snapshot logical file: %v", err)
		}
		for _, path := range []string{"/phase8/a.txt", "/phase8/b.txt"} {
			if _, err := backend.DB.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2)`, path, currentLogicalID); err != nil {
				t.Fatalf("insert physical mapping %q: %v", path, err)
			}
		}

		chunkIDs := make([]int64, 4)
		for i, fixture := range []struct {
			hash string
			size int64
			pin  int64
		}{
			{hash: "phase8-snapshot", size: 120},
			{hash: "phase8-dead-slice", size: 80},
			{hash: "phase8-pinned", size: 40, pin: 1},
			{hash: "phase8-dead-block", size: 50},
		} {
			if err := backend.DB.QueryRow(
				`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ($1, $2, 'COMPLETED', 0, $3, 'v2-fastcdc') RETURNING id`,
				fixture.hash, fixture.size, fixture.pin,
			).Scan(&chunkIDs[i]); err != nil {
				t.Fatalf("insert chunk %q: %v", fixture.hash, err)
			}
		}
		if _, err := backend.DB.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, snapshotLogicalID, chunkIDs[0]); err != nil {
			t.Fatalf("insert snapshot recipe: %v", err)
		}
		if _, err := backend.DB.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('phase8-snapshot', CURRENT_TIMESTAMP, 'full')`); err != nil {
			t.Fatalf("insert snapshot: %v", err)
		}
		var snapshotPathID int64
		if err := backend.DB.QueryRow(`INSERT INTO snapshot_path (path) VALUES ('phase8/snapshot.txt') RETURNING id`).Scan(&snapshotPathID); err != nil {
			t.Fatalf("insert snapshot path: %v", err)
		}
		if _, err := backend.DB.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('phase8-snapshot', $1, $2)`, snapshotPathID, snapshotLogicalID); err != nil {
			t.Fatalf("insert snapshot membership: %v", err)
		}

		var containerID int64
		if err := backend.DB.QueryRow(
			`INSERT INTO container (filename, sealed, sealing, quarantine, current_size, max_size) VALUES ('phase8-accounting.bin', TRUE, FALSE, FALSE, 290, 1024) RETURNING id`,
		).Scan(&containerID); err != nil {
			t.Fatalf("insert container: %v", err)
		}
		var protectedBlockID, deadBlockID int64
		if err := backend.DB.QueryRow(
			`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, $1, 240, 240, $2, 0, $3) RETURNING id`,
			codec, containerID, []byte{1, 2, 3},
		).Scan(&protectedBlockID); err != nil {
			t.Fatalf("insert protected storage block: %v", err)
		}
		if err := backend.DB.QueryRow(
			`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, $1, 50, 50, $2, 240, $3) RETURNING id`,
			codec, containerID, []byte{4, 5, 6},
		).Scan(&deadBlockID); err != nil {
			t.Fatalf("insert dead storage block: %v", err)
		}
		for _, ref := range []struct {
			chunkID int64
			blockID int64
			offset  int64
			size    int64
		}{
			{chunkID: chunkIDs[0], blockID: protectedBlockID, offset: 0, size: 120},
			{chunkID: chunkIDs[1], blockID: protectedBlockID, offset: 120, size: 80},
			{chunkID: chunkIDs[2], blockID: protectedBlockID, offset: 200, size: 40},
			{chunkID: chunkIDs[3], blockID: deadBlockID, offset: 0, size: 50},
		} {
			if _, err := backend.DB.Exec(
				`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, $3, $4)`,
				ref.chunkID, ref.blockID, ref.offset, ref.size,
			); err != nil {
				t.Fatalf("insert packed reference for chunk %d: %v", ref.chunkID, err)
			}
		}

		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		stats, err := eng.Stats(context.Background(), engine.StatsRequest{IncludeContainers: true})
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if stats.Physical.TotalPhysicalFiles != 2 || stats.Containers.LiveBlockBytes != 240 || stats.Containers.DeadBlockBytes != 50 {
			t.Fatalf("stats physical accounting=%+v", stats)
		}

		plan, err := eng.PlanGarbageCollection(context.Background(), engine.GarbageCollectionPlanRequest{})
		if err != nil {
			t.Fatalf("PlanGarbageCollection: %v", err)
		}
		want := engine.GarbageCollectionPlanSummary{
			TotalChunks:                        4,
			ReachableChunks:                    1,
			UnreachableChunks:                  3,
			LogicallyReclaimableBytes:          130,
			PhysicallyReclaimableBytes:         0,
			FullyReclaimableContainers:         0,
			PartiallyDeadContainers:            1,
			PackedBlocksLive:                   1,
			PackedBlocksDead:                   1,
			PackedBytesLive:                    240,
			PackedBytesReclaimable:             50,
			RetainedDeadBytesDueToPackedBlocks: 80,
		}
		if plan.Summary != want {
			t.Fatalf("plan summary=%+v, want %+v", plan.Summary, want)
		}
		if len(plan.Containers) != 1 || plan.Containers[0].TotalChunks != 2 || plan.Containers[0].ReclaimableChunks != 1 || plan.Containers[0].ReclaimableBytes != 50 {
			t.Fatalf("container accounting=%+v", plan.Containers)
		}
	})
}
