package verify

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

// ChunkerVersionMetadataSummary captures repository-level version-metadata
// integrity. These checks are intentionally structural: read-side flows should
// reject missing metadata without depending on the current active chunker.
//
// Semantics guardrail:
//   - logical_file.chunker_version identifies recipe provenance metadata
//   - chunk.chunker_version identifies chunk-origin metadata
//   - neither field is used here to enforce cross-row equality constraints
//     because deduplicated chunks may be referenced across version eras
type ChunkerVersionMetadataSummary struct {
	EmptyLogicalFileVersions int64
	EmptyChunkVersions       int64
}

func (s ChunkerVersionMetadataSummary) totalIssues() int64 {
	return s.EmptyLogicalFileVersions + s.EmptyChunkVersions
}

func CheckChunkerVersionMetadataIntegrity(dbconn *sql.DB) (ChunkerVersionMetadataSummary, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking chunker-version metadata integrity...")

	var summary ChunkerVersionMetadataSummary
	var errorList []error

	logicalRows, err := dbconn.QueryContext(ctx, `
		SELECT id
		FROM logical_file
		WHERE chunker_version IS NULL OR TRIM(chunker_version) = ''
		ORDER BY id ASC
	`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query logical_file chunker_version integrity: %v", err)
		return ChunkerVersionMetadataSummary{}, fmt.Errorf("failed to query logical_file chunker_version integrity: %w", err)
	}
	defer func() { _ = logicalRows.Close() }()

	for logicalRows.Next() {
		var logicalFileID int64
		if err := logicalRows.Scan(&logicalFileID); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan logical_file chunker_version inconsistency: %w", err))
			summary.EmptyLogicalFileVersions++
			continue
		}
		summary.EmptyLogicalFileVersions++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("logical_file has empty chunker_version: logical_file_id=%d", logicalFileID))
	}
	if err := logicalRows.Err(); err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed iterating logical_file chunker_version integrity rows: %v", err)
		return ChunkerVersionMetadataSummary{}, fmt.Errorf("failed iterating logical_file chunker_version integrity rows: %w", err)
	}

	chunkRows, err := dbconn.QueryContext(ctx, `
		SELECT id
		FROM chunk
		WHERE chunker_version IS NULL OR TRIM(chunker_version) = ''
		ORDER BY id ASC
	`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query chunk chunker_version integrity: %v", err)
		return ChunkerVersionMetadataSummary{}, fmt.Errorf("failed to query chunk chunker_version integrity: %w", err)
	}
	defer func() { _ = chunkRows.Close() }()

	for chunkRows.Next() {
		var chunkID int64
		if err := chunkRows.Scan(&chunkID); err != nil {
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan chunk chunker_version inconsistency: %w", err))
			summary.EmptyChunkVersions++
			continue
		}
		summary.EmptyChunkVersions++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk has empty chunker_version: chunk_id=%d", chunkID))
	}
	if err := chunkRows.Err(); err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed iterating chunk chunker_version integrity rows: %v", err)
		return ChunkerVersionMetadataSummary{}, fmt.Errorf("failed iterating chunk chunker_version integrity rows: %w", err)
	}

	if summary.totalIssues() > 0 {
		log.Println(" ERROR ")
		log.Printf(
			"Found %d chunker-version metadata integrity errors: empty logical_file chunker_version rows=%d empty chunk chunker_version rows=%d",
			summary.totalIssues(),
			summary.EmptyLogicalFileVersions,
			summary.EmptyChunkVersions,
		)
		if len(errorList) > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		message := fmt.Sprintf(
			"found %d errors in CheckChunkerVersionMetadataIntegrity checks: empty logical_file chunker_version rows=%d empty chunk chunker_version rows=%d",
			summary.totalIssues(),
			summary.EmptyLogicalFileVersions,
			summary.EmptyChunkVersions,
		)
		return summary, invariants.New(invariants.CodePhysicalGraphIntegrity, message, nil)
	}

	log.Println(" SUCCESS ")
	return summary, nil
}
