package verify

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

// PhysicalFileIntegritySummary captures the current-state physical mapping
// invariants introduced in v1.2. This type is intentionally small and cheap to
// compute so standard verify can audit it on every run.
type PhysicalFileIntegritySummary struct {
	OrphanPhysicalFileRows    int64
	LogicalRefCountMismatches int64
	NegativeLogicalRefCounts  int64
}

func (s PhysicalFileIntegritySummary) totalIssues() int64 {
	return s.OrphanPhysicalFileRows + s.LogicalRefCountMismatches + s.NegativeLogicalRefCounts
}

func checkPhysicalFileGraphIntegrity(dbconn *sql.DB) (PhysicalFileIntegritySummary, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking v1.2 physical_file graph integrity...")

	var summary PhysicalFileIntegritySummary
	var errorList []error

	orphanRows, err := dbconn.QueryContext(ctx, `
		SELECT pf.path, pf.logical_file_id
		FROM physical_file pf
		LEFT JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.id IS NULL
		ORDER BY pf.path ASC
	`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query orphan physical_file rows: %v", err)
		return PhysicalFileIntegritySummary{}, fmt.Errorf("failed to query orphan physical_file rows: %w", err)
	}
	defer func() { _ = orphanRows.Close() }()

	for orphanRows.Next() {
		var path string
		var logicalFileID int64
		if err := orphanRows.Scan(&path, &logicalFileID); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan orphan physical_file row: %w", err))
			summary.OrphanPhysicalFileRows++
			continue
		}
		summary.OrphanPhysicalFileRows++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("orphan physical_file row: path=%q logical_file_id=%d", path, logicalFileID))
	}
	if err := orphanRows.Err(); err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed iterating orphan physical_file rows: %v", err)
		return PhysicalFileIntegritySummary{}, fmt.Errorf("failed iterating orphan physical_file rows: %w", err)
	}

	mismatchRows, err := dbconn.QueryContext(ctx, `
		SELECT lf.id, lf.ref_count, COALESCE(p.actual_count, 0)
		FROM logical_file lf
		LEFT JOIN (
			SELECT logical_file_id, COUNT(*) AS actual_count
			FROM physical_file
			GROUP BY logical_file_id
		) p ON p.logical_file_id = lf.id
		WHERE lf.ref_count <> COALESCE(p.actual_count, 0)
		ORDER BY lf.id ASC
	`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query logical_file.ref_count mismatches: %v", err)
		return PhysicalFileIntegritySummary{}, fmt.Errorf("failed to query logical_file.ref_count mismatches: %w", err)
	}
	defer func() { _ = mismatchRows.Close() }()

	for mismatchRows.Next() {
		var logicalFileID int64
		var expectedRefCount int64
		var actualMappings int64
		if err := mismatchRows.Scan(&logicalFileID, &expectedRefCount, &actualMappings); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan logical_file.ref_count mismatch row: %w", err))
			summary.LogicalRefCountMismatches++
			continue
		}
		summary.LogicalRefCountMismatches++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("logical_file.ref_count mismatch: logical_file_id=%d expected=%d actual=%d", logicalFileID, expectedRefCount, actualMappings))
	}
	if err := mismatchRows.Err(); err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed iterating logical_file.ref_count mismatches: %v", err)
		return PhysicalFileIntegritySummary{}, fmt.Errorf("failed iterating logical_file.ref_count mismatches: %w", err)
	}

	negativeRows, err := dbconn.QueryContext(ctx, `
		SELECT id, ref_count
		FROM logical_file
		WHERE ref_count < 0
		ORDER BY id ASC
	`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query negative logical_file.ref_count rows: %v", err)
		return PhysicalFileIntegritySummary{}, fmt.Errorf("failed to query negative logical_file.ref_count rows: %w", err)
	}
	defer func() { _ = negativeRows.Close() }()

	for negativeRows.Next() {
		var logicalFileID int64
		var refCount int64
		if err := negativeRows.Scan(&logicalFileID, &refCount); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan negative logical_file.ref_count row: %w", err))
			summary.NegativeLogicalRefCounts++
			continue
		}
		summary.NegativeLogicalRefCounts++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("negative logical_file.ref_count: logical_file_id=%d ref_count=%d", logicalFileID, refCount))
	}
	if err := negativeRows.Err(); err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed iterating negative logical_file.ref_count rows: %v", err)
		return PhysicalFileIntegritySummary{}, fmt.Errorf("failed iterating negative logical_file.ref_count rows: %w", err)
	}

	if summary.totalIssues() > 0 {
		log.Println(" ERROR ")
		log.Printf(
			"Found %d physical mapping integrity errors: orphan physical_file rows=%d logical ref_count mismatches=%d negative logical ref_count rows=%d",
			summary.totalIssues(),
			summary.OrphanPhysicalFileRows,
			summary.LogicalRefCountMismatches,
			summary.NegativeLogicalRefCounts,
		)
		if len(errorList) > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return summary, fmt.Errorf(
			"found %d errors in checkPhysicalFileGraphIntegrity checks: orphan physical_file rows=%d logical ref_count mismatches=%d negative logical ref_count rows=%d",
			summary.totalIssues(),
			summary.OrphanPhysicalFileRows,
			summary.LogicalRefCountMismatches,
			summary.NegativeLogicalRefCounts,
		)
	}

	log.Println(" SUCCESS ")
	return summary, nil
}
