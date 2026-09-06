package gc

import (
	"context"
	"testing"
)

func TestRetiredLegacyExtentIsPlannedAsReclaimablePhysicalOccupancy(t *testing.T) {
	dbconn := openTestDB(t)
	logicalID := insertLogicalFile(t, dbconn, "retired-plan")
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, 'hash-retired-plan', 100, 'fingerprint', 'PUBLISHED') RETURNING id`, logicalID,
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert plan repair attempt: %v", err)
	}
	containerID := insertContainer(t, dbconn, "retired-plan.bin", 80)
	if _, err := dbconn.Exec(`
		INSERT INTO retired_legacy_block_extent
		 (container_id, block_offset, stored_size, plaintext_size, codec, format_version,
		  historical_block_id, historical_chunk_id, repair_attempt_id)
		VALUES ($1, 64, 16, 16, 'plain', 1, 91, 92, $2)`, containerID, attemptID); err != nil {
		t.Fatalf("insert retired plan extent: %v", err)
	}

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("build retired occupancy plan: %v", err)
	}
	if len(plan.AffectedContainers) != 1 {
		t.Fatalf("affected containers=%+v, want one", plan.AffectedContainers)
	}
	impact := plan.AffectedContainers[0]
	if !impact.FullyReclaimable || impact.ReclaimableBytes != 16 || impact.TotalChunks != 1 || plan.PhysicallyReclaimableBytes != 80 {
		t.Fatalf("retired occupancy impact=%+v physical=%d", impact, plan.PhysicallyReclaimableBytes)
	}
}
