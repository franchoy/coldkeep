package verify

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/retention"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

// SnapshotReachabilityIntegritySummary captures snapshot-specific retention
// consistency checks audited during verify.
type SnapshotReachabilityIntegritySummary struct {
	SnapshotFileRows               int64
	OrphanSnapshotPathRefs         int64
	SnapshotReferencedLogicalFiles int64
	SnapshotOnlyLogicalFiles       int64
	SharedLogicalFiles             int64
	OrphanSnapshotLogicalRefs      int64
	InvalidSnapshotLifecycleStates int64
	RetainedMissingChunkGraph      int64
}

func (s SnapshotReachabilityIntegritySummary) totalIssues() int64 {
	return s.OrphanSnapshotPathRefs + s.OrphanSnapshotLogicalRefs + s.InvalidSnapshotLifecycleStates + s.RetainedMissingChunkGraph
}

func snapshotGraphInvariantCode(summary SnapshotReachabilityIntegritySummary) string {
	onlyOrphans := (summary.OrphanSnapshotLogicalRefs > 0 || summary.OrphanSnapshotPathRefs > 0) && summary.InvalidSnapshotLifecycleStates == 0 && summary.RetainedMissingChunkGraph == 0
	onlyLifecycle := summary.OrphanSnapshotPathRefs == 0 && summary.OrphanSnapshotLogicalRefs == 0 && summary.InvalidSnapshotLifecycleStates > 0 && summary.RetainedMissingChunkGraph == 0

	switch {
	case onlyOrphans:
		return invariants.CodeSnapshotGraphOrphanLogicalRef
	case onlyLifecycle:
		return invariants.CodeSnapshotGraphInvalidLifecycle
	default:
		return invariants.CodeSnapshotGraphIntegrity
	}
}

// CheckSnapshotReachabilityIntegrity audits persisted snapshot metadata against
// logical-file lifecycle and retention expectations.
func CheckSnapshotReachabilityIntegrity(dbconn *sql.DB) (SnapshotReachabilityIntegritySummary, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking snapshot reachability integrity...")

	var summary SnapshotReachabilityIntegritySummary
	var errorList []error

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot_file`).Scan(&summary.SnapshotFileRows); err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("count snapshot_file rows: %w", err)
	}

	reachability, err := retention.ComputeReachabilitySummary(ctx, dbconn)
	if err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("compute retention reachability summary: %w", err)
	}
	summary.SnapshotReferencedLogicalFiles = int64(len(reachability.SnapshotLogicalIDs))

	for id := range reachability.SnapshotLogicalIDs {
		_, current := reachability.CurrentLogicalIDs[id]
		if current {
			summary.SharedLogicalFiles++
		} else {
			summary.SnapshotOnlyLogicalFiles++
		}
	}

	orphanPathRows, err := dbconn.QueryContext(ctx, `
		SELECT sf.id, sf.snapshot_id, sf.path_id
		FROM snapshot_file sf
		LEFT JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sp.id IS NULL
		ORDER BY sf.id ASC
	`)
	if err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("query orphan snapshot_file path refs: %w", err)
	}
	defer func() { _ = orphanPathRows.Close() }()

	for orphanPathRows.Next() {
		var rowID int64
		var snapshotID string
		var pathID int64
		if err := orphanPathRows.Scan(&rowID, &snapshotID, &pathID); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("scan orphan snapshot_file path ref: %w", err))
			summary.OrphanSnapshotPathRefs++
			continue
		}
		summary.OrphanSnapshotPathRefs++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("snapshot_file orphan path reference: snapshot_file_id=%d snapshot_id=%q path_id=%d", rowID, snapshotID, pathID))
	}
	if err := orphanPathRows.Err(); err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("iterate orphan snapshot_file path refs: %w", err)
	}

	orphanRows, err := dbconn.QueryContext(ctx, `
		SELECT sf.id, sf.snapshot_id, sp.path, sf.logical_file_id
		FROM snapshot_file sf
		LEFT JOIN snapshot_path sp ON sp.id = sf.path_id
		LEFT JOIN logical_file lf ON lf.id = sf.logical_file_id
		WHERE lf.id IS NULL
		ORDER BY sf.id ASC
	`)
	if err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("query orphan snapshot_file logical refs: %w", err)
	}
	defer func() { _ = orphanRows.Close() }()

	for orphanRows.Next() {
		var rowID int64
		var snapshotID string
		var path sql.NullString
		var logicalFileID int64
		if err := orphanRows.Scan(&rowID, &snapshotID, &path, &logicalFileID); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("scan orphan snapshot_file logical ref: %w", err))
			summary.OrphanSnapshotLogicalRefs++
			continue
		}
		summary.OrphanSnapshotLogicalRefs++
		pathText := "<missing>"
		if path.Valid {
			pathText = path.String
		}
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("snapshot_file orphan logical reference: snapshot_file_id=%d snapshot_id=%q path=%q logical_file_id=%d", rowID, snapshotID, pathText, logicalFileID))
	}
	if err := orphanRows.Err(); err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("iterate orphan snapshot_file logical refs: %w", err)
	}

	invalidLifecycleRows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT lf.id, lf.status, lf.ref_count
		FROM logical_file lf
		JOIN snapshot_file sf ON sf.logical_file_id = lf.id
		WHERE lf.status <> $1 OR lf.ref_count < 0
		ORDER BY lf.id ASC
	`, filestate.LogicalFileCompleted)
	if err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("query invalid snapshot lifecycle rows: %w", err)
	}
	defer func() { _ = invalidLifecycleRows.Close() }()

	for invalidLifecycleRows.Next() {
		var logicalFileID int64
		var status string
		var refCount int64
		if err := invalidLifecycleRows.Scan(&logicalFileID, &status, &refCount); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("scan invalid snapshot lifecycle row: %w", err))
			summary.InvalidSnapshotLifecycleStates++
			continue
		}
		summary.InvalidSnapshotLifecycleStates++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("snapshot references logical_file in invalid lifecycle state: logical_file_id=%d status=%q ref_count=%d", logicalFileID, status, refCount))
	}
	if err := invalidLifecycleRows.Err(); err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("iterate invalid snapshot lifecycle rows: %w", err)
	}

	missingChunkGraphRows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT lf.id, lf.total_size
		FROM logical_file lf
		JOIN snapshot_file sf ON sf.logical_file_id = lf.id
		LEFT JOIN file_chunk fc ON fc.logical_file_id = lf.id
		WHERE lf.total_size > 0
		AND fc.chunk_id IS NULL
		ORDER BY lf.id ASC
	`)
	if err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("query snapshot-retained logical files missing chunk graph: %w", err)
	}
	defer func() { _ = missingChunkGraphRows.Close() }()

	for missingChunkGraphRows.Next() {
		var logicalFileID int64
		var totalSize int64
		if err := missingChunkGraphRows.Scan(&logicalFileID, &totalSize); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("scan snapshot-retained missing chunk graph row: %w", err))
			summary.RetainedMissingChunkGraph++
			continue
		}
		summary.RetainedMissingChunkGraph++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("snapshot-retained logical_file has no chunk graph: logical_file_id=%d total_size=%d", logicalFileID, totalSize))
	}
	if err := missingChunkGraphRows.Err(); err != nil {
		return SnapshotReachabilityIntegritySummary{}, fmt.Errorf("iterate snapshot-retained logical files missing chunk graph: %w", err)
	}

	if summary.totalIssues() > 0 {
		log.Println(" ERROR ")
		log.Printf(
			"Found %d snapshot reachability integrity errors: orphan_snapshot_path_refs=%d orphan_snapshot_logical_refs=%d invalid_snapshot_lifecycle_states=%d retained_missing_chunk_graph=%d",
			summary.totalIssues(),
			summary.OrphanSnapshotPathRefs,
			summary.OrphanSnapshotLogicalRefs,
			summary.InvalidSnapshotLifecycleStates,
			summary.RetainedMissingChunkGraph,
		)
		if len(errorList) > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		message := fmt.Sprintf(
			"found %d errors in snapshot reachability checks: orphan_snapshot_path_refs=%d orphan_snapshot_logical_refs=%d invalid_snapshot_lifecycle_states=%d retained_missing_chunk_graph=%d",
			summary.totalIssues(),
			summary.OrphanSnapshotPathRefs,
			summary.OrphanSnapshotLogicalRefs,
			summary.InvalidSnapshotLifecycleStates,
			summary.RetainedMissingChunkGraph,
		)
		return summary, invariants.New(snapshotGraphInvariantCode(summary), message, nil)
	}

	log.Println(" SUCCESS ")
	return summary, nil
}
