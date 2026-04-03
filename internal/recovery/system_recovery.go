package recovery

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/utils_env"
)

type recoveryStats struct {
	abortedLogicalFiles    int64
	abortedChunks          int64
	quarantinedMissing     int64
	quarantinedCorruptTail int64
	quarantinedOrphan      int64
	skippedDirEntries      int64
	totalContainersChecked int64
	totalDiskFilesChecked  int64
	sealingCompleted       int64
	sealingQuarantined     int64
}

type Report struct {
	AbortedLogicalFiles    int64 `json:"aborted_logical_files"`
	AbortedChunks          int64 `json:"aborted_chunks"`
	QuarantinedMissing     int64 `json:"quarantined_missing"`
	QuarantinedCorruptTail int64 `json:"quarantined_corrupt_tail"`
	QuarantinedOrphan      int64 `json:"quarantined_orphan"`
	SkippedDirEntries      int64 `json:"skipped_dir_entries"`
	CheckedContainerRecord int64 `json:"checked_container_record"`
	CheckedDiskFiles       int64 `json:"checked_disk_files"`
	SealingCompleted       int64 `json:"sealing_completed"`
	SealingQuarantined     int64 `json:"sealing_quarantined"`
}

// isStrictRecovery returns true unless COLDKEEP_STRICT_RECOVERY is explicitly
// set to "false". In strict mode (default) suspicious orphan container
// conflicts abort corrective recovery with an error. In non-strict mode they are
// logged as warnings and corrective recovery continues, which is safer under
// benign duplicate / restart-race scenarios.
func isStrictRecovery() bool {
	return utils_env.GetenvOrDefault("COLDKEEP_STRICT_RECOVERY", "true") != "false"
}

func logRecoveryEvent(action string, fields ...string) {
	line := "event=recovery action=" + action
	if len(fields) > 0 {
		line += " " + strings.Join(fields, " ")
	}
	log.Println(line)
}

func SystemRecoveryWithContainersDir(containersDir string) error {
	_, err := SystemRecoveryReportWithContainersDir(containersDir)
	return err
}

// SystemRecoveryReportWithContainersDir is the primary corrective recovery entry
// point. It modifies database state: PROCESSING logical files and chunks are
// aborted, containers with unresolvable state enter the retirement/quarantine
// path. Callers (including the doctor command and normal startup) must treat
// changed row counts in the returned Report as expected, successful corrective
// outcomes, not as errors.
func SystemRecoveryReportWithContainersDir(containersDir string) (Report, error) {
	stats := &recoveryStats{}
	logRecoveryEvent("start", "containers_dir="+containersDir)
	dbconn, err := db.ConnectDB()
	if err != nil {
		return buildReport(stats), fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	err = abortProcessingLogicalFiles(dbconn, stats)
	if err != nil {
		return buildReport(stats), err
	}
	err = abortProcessingChunks(dbconn, stats)
	if err != nil {
		return buildReport(stats), err
	}
	err = recoverSealingContainers(dbconn, containersDir, stats)
	if err != nil {
		return buildReport(stats), err
	}
	err = quarantineMissingContainers(dbconn, containersDir, stats)
	if err != nil {
		return buildReport(stats), err
	}
	err = quarantineCorruptActiveContainerTails(dbconn, containersDir, stats)
	if err != nil {
		return buildReport(stats), err
	}
	err = quarantineOrphanContainers(dbconn, containersDir, stats)
	if err != nil {
		return buildReport(stats), err
	}

	logRecoveryEvent(
		"summary",
		fmt.Sprintf("aborted_logical_files=%d", stats.abortedLogicalFiles),
		fmt.Sprintf("aborted_chunks=%d", stats.abortedChunks),
		fmt.Sprintf("sealing_completed=%d", stats.sealingCompleted),
		fmt.Sprintf("sealing_quarantined=%d", stats.sealingQuarantined),
		fmt.Sprintf("quarantined_missing_containers=%d", stats.quarantinedMissing),
		fmt.Sprintf("quarantined_corrupt_tail_containers=%d", stats.quarantinedCorruptTail),
		fmt.Sprintf("quarantined_orphan_containers=%d", stats.quarantinedOrphan),
		fmt.Sprintf("checked_container_records=%d", stats.totalContainersChecked),
		fmt.Sprintf("checked_disk_files=%d", stats.totalDiskFilesChecked),
		fmt.Sprintf("skipped_dir_entries=%d", stats.skippedDirEntries),
	)

	return buildReport(stats), nil
}

func buildReport(stats *recoveryStats) Report {
	return Report{
		AbortedLogicalFiles:    stats.abortedLogicalFiles,
		AbortedChunks:          stats.abortedChunks,
		QuarantinedMissing:     stats.quarantinedMissing,
		QuarantinedCorruptTail: stats.quarantinedCorruptTail,
		QuarantinedOrphan:      stats.quarantinedOrphan,
		SkippedDirEntries:      stats.skippedDirEntries,
		CheckedContainerRecord: stats.totalContainersChecked,
		CheckedDiskFiles:       stats.totalDiskFilesChecked,
		SealingCompleted:       stats.sealingCompleted,
		SealingQuarantined:     stats.sealingQuarantined,
	}
}

func abortProcessingLogicalFiles(dbconn *sql.DB, stats *recoveryStats) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	logRecoveryEvent("abort_processing_logical_files_start")
	result, err := dbconn.ExecContext(ctx, `UPDATE logical_file SET status = $1 WHERE status = $2`, filestate.LogicalFileAborted, filestate.LogicalFileProcessing)
	if err != nil {
		return fmt.Errorf("query update logical_file to ABORTED: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for logical_file update: %w", err)
	}
	stats.abortedLogicalFiles += affected
	logRecoveryEvent("abort_processing_logical_files_done", fmt.Sprintf("aborted_count=%d", affected))
	return nil
}

func abortProcessingChunks(dbconn *sql.DB, stats *recoveryStats) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	logRecoveryEvent("abort_processing_chunks_start")
	result, err := dbconn.ExecContext(ctx, `UPDATE chunk SET status = $1 WHERE status = $2`, filestate.ChunkAborted, filestate.ChunkProcessing)
	if err != nil {
		return fmt.Errorf("query update chunk to ABORTED: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for chunk update: %w", err)
	}
	stats.abortedChunks += affected
	logRecoveryEvent("abort_processing_chunks_done", fmt.Sprintf("aborted_count=%d", affected))
	return nil
}

func recoverSealingContainers(dbconn *sql.DB, containersDir string, stats *recoveryStats) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	logRecoveryEvent("recover_sealing_containers_start")

	// Clean up stale markers where a previous run sealed successfully but did not
	// clear sealing (e.g., interrupted runs or manual DB edits).
	if _, err := dbconn.ExecContext(ctx, `UPDATE container SET sealing = FALSE WHERE sealed = TRUE AND sealing = TRUE`); err != nil {
		return fmt.Errorf("clear stale sealing markers: %w", err)
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = FALSE AND sealing = TRUE AND quarantine = FALSE
	`)
	if err != nil {
		return fmt.Errorf("query sealing containers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int64
		var filename string
		var currentSize int64
		if err := rows.Scan(&id, &filename, &currentSize); err != nil {
			return fmt.Errorf("scan sealing container row: %w", err)
		}

		path := filepath.Join(containersDir, filename)
		fileInfo, statErr := os.Stat(path)
		if statErr == nil && fileInfo.Size() != currentSize {
			if _, qErr := dbconn.ExecContext(ctx,
				`UPDATE container SET quarantine = TRUE, sealing = FALSE, current_size = $2, max_size = $2 WHERE id = $1`,
				id,
				fileInfo.Size(),
			); qErr != nil {
				return fmt.Errorf("quarantine sealing container %d after size mismatch: %w", id, qErr)
			}

			stats.sealingQuarantined++
			logRecoveryEvent(
				"recover_sealing_container_quarantined",
				fmt.Sprintf("container_id=%d", id),
				"filename="+filename,
				"reason=size_mismatch",
				fmt.Sprintf("db_current_size=%d", currentSize),
				fmt.Sprintf("physical_size=%d", fileInfo.Size()),
			)
			continue
		}

		sealErr := container.SealContainerInDir(dbconn, id, filename, containersDir)
		if sealErr == nil {
			stats.sealingCompleted++
			logRecoveryEvent(
				"recover_sealing_container_completed",
				fmt.Sprintf("container_id=%d", id),
				"filename="+filename,
			)
			continue
		}

		// Physical file missing/unreadable: execute retirement/quarantine path and
		// clear sealing marker.
		if _, qErr := dbconn.ExecContext(ctx,
			`UPDATE container SET quarantine = TRUE, sealing = FALSE WHERE id = $1`,
			id,
		); qErr != nil {
			return fmt.Errorf("quarantine unresolved sealing container %d after seal error %v: %w", id, sealErr, qErr)
		}

		stats.sealingQuarantined++
		logRecoveryEvent(
			"recover_sealing_container_quarantined",
			fmt.Sprintf("container_id=%d", id),
			"filename="+filename,
			"reason=seal_failed",
			"error="+sealErr.Error(),
		)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	logRecoveryEvent(
		"recover_sealing_containers_done",
		fmt.Sprintf("completed=%d", stats.sealingCompleted),
		fmt.Sprintf("quarantined=%d", stats.sealingQuarantined),
	)
	return nil
}

func quarantineMissingContainers(dbconn *sql.DB, containersDir string, stats *recoveryStats) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	logRecoveryEvent("quarantine_missing_containers_start")
	rows, err := dbconn.QueryContext(ctx, `SELECT id, filename FROM container WHERE quarantine = FALSE`)
	if err != nil {
		return fmt.Errorf("query retrieve container list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {

		var id int64
		var filename string

		if err := rows.Scan(&id, &filename); err != nil {
			return err
		}
		stats.totalContainersChecked++

		path := filepath.Join(containersDir, filename)

		_, err := os.Stat(path)

		if os.IsNotExist(err) {

			_, err := dbconn.ExecContext(ctx, `UPDATE container SET quarantine = TRUE WHERE id = $1`, id)
			if err != nil {
				return fmt.Errorf("query update container to quarantine due to missing file: %w", err)
			}
			stats.quarantinedMissing++
			logRecoveryEvent(
				"quarantine_missing_container",
				fmt.Sprintf("container_id=%d", id),
				"filename="+filename,
				"reason=missing_file",
			)

		} else if err != nil {
			return fmt.Errorf("stat container file: %w", err)
		}

	}

	if err := rows.Err(); err != nil {
		return err
	}
	logRecoveryEvent("quarantine_missing_containers_done", fmt.Sprintf("quarantined_count=%d", stats.quarantinedMissing))
	return nil
}

func quarantineCorruptActiveContainerTails(dbconn *sql.DB, containersDir string, stats *recoveryStats) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	logRecoveryEvent("quarantine_corrupt_active_container_tails_start")

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename, current_size
		FROM container
		WHERE quarantine = FALSE AND sealed = FALSE
	`)
	if err != nil {
		return fmt.Errorf("query retrieve active container list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int64
		var filename string
		var currentSize int64

		if err := rows.Scan(&id, &filename, &currentSize); err != nil {
			return fmt.Errorf("scan active container row: %w", err)
		}

		path := filepath.Join(containersDir, filename)
		fileInfo, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("stat active container file: %w", err)
		}

		physicalSize := fileInfo.Size()
		reason := ""
		switch {
		case currentSize > physicalSize:
			reason = "db_current_size_past_eof"
		case physicalSize > currentSize:
			// Ghost bytes on disk: payload write reached disk but DB tx rolled back.
			// Any size mismatch is treated as suspicious for v1.0 strict recovery.
			reason = "physical_size_past_db_current_size"
		default:
			// Sizes match; check completed-block bounds.
			var hasOutOfBoundsBlock bool
			err = dbconn.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM blocks b
					JOIN chunk c ON c.id = b.chunk_id
					WHERE b.container_id = $1
					  AND c.status = $2
					  AND (
						b.block_offset < 0
						OR b.stored_size <= 0
						OR b.block_offset > ($3 - b.stored_size)
					  )
				)
			`, id, filestate.ChunkCompleted, physicalSize).Scan(&hasOutOfBoundsBlock)
			if err != nil {
				return fmt.Errorf("query active container block bounds: %w", err)
			}
			if hasOutOfBoundsBlock {
				reason = "completed_block_past_eof"
			}

			// Check for interior gaps: first block not at header or non-contiguous offsets.
			if reason == "" {
				reason, err = detectInteriorGaps(ctx, dbconn, id, filestate.ChunkCompleted)
				if err != nil {
					return fmt.Errorf("query active container block continuity for container %d: %w", id, err)
				}
			}

			if reason == "" {
				hasTrailingBytes, err := hasTrailingUnreferencedBytes(ctx, dbconn, id, currentSize, filestate.ChunkCompleted)
				if err != nil {
					return fmt.Errorf("query active container trailing bytes for container %d: %w", id, err)
				}
				if hasTrailingBytes {
					reason = "trailing_unreferenced_bytes"
				}
			}
		}
		if reason == "" {
			continue
		}

		_, err = dbconn.ExecContext(ctx, `UPDATE container SET quarantine = TRUE, sealing = FALSE, current_size = $2, max_size = $2 WHERE id = $1`, id, physicalSize)
		if err != nil {
			return fmt.Errorf("query update active container to quarantine due to corrupt tail: %w", err)
		}

		stats.quarantinedCorruptTail++
		logRecoveryEvent(
			"quarantine_corrupt_active_container_tail",
			fmt.Sprintf("container_id=%d", id),
			"filename="+filename,
			fmt.Sprintf("db_current_size=%d", currentSize),
			fmt.Sprintf("physical_size=%d", physicalSize),
			"reason="+reason,
		)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	logRecoveryEvent("quarantine_corrupt_active_container_tails_done", fmt.Sprintf("quarantined_count=%d", stats.quarantinedCorruptTail))
	return nil
}

func quarantineOrphanContainers(dbconn *sql.DB, containersDir string, stats *recoveryStats) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	var reusedCount int64
	var skippedExistingCount int64
	var warningCount int64

	logRecoveryEvent("quarantine_orphan_containers_start")
	// recover files in container folder
	entries, err := os.ReadDir(containersDir)
	if os.IsNotExist(err) {
		logRecoveryEvent("quarantine_orphan_containers_skipped", "reason=containers_dir_missing")
		return nil
	}
	if err != nil {
		return fmt.Errorf("read containers dir: %w", err)
	}

	for _, file := range entries {
		if file.IsDir() {
			stats.skippedDirEntries++
			continue
		}
		fileinfo, err := file.Info()
		if err != nil {
			return fmt.Errorf("get info for file %s: %w", file.Name(), err)
		}
		name := file.Name()
		fileSize := fileinfo.Size()
		stats.totalDiskFilesChecked++

		result, err := dbconn.ExecContext(ctx, `INSERT INTO container (filename, quarantine, current_size, max_size) VALUES ($1, TRUE, $2, $3) ON CONFLICT (filename) DO NOTHING`, name, fileSize, fileSize)
		if err != nil {
			return fmt.Errorf("insert orphan container record: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rows affected for orphan container insert: %w", err)
		}

		if rowsAffected == 1 {
			stats.quarantinedOrphan++
			logRecoveryEvent(
				"quarantine_orphan_container",
				"filename="+name,
				fmt.Sprintf("size_bytes=%d", fileSize),
				"reason=orphan_file_on_disk",
			)
			continue
		}

		var existingQuarantine bool
		var existingCurrentSize int64
		var existingMaxSize int64
		err = dbconn.QueryRowContext(
			ctx,
			`SELECT quarantine, current_size, max_size FROM container WHERE filename = $1`,
			name,
		).Scan(&existingQuarantine, &existingCurrentSize, &existingMaxSize)
		if err == sql.ErrNoRows {
			if !isStrictRecovery() {
				logRecoveryEvent(
					"quarantine_orphan_container_conflict_warning",
					"filename="+name,
					"reason=insert_conflict_but_row_missing",
					"strict=false",
				)
				continue
			}
			return fmt.Errorf("suspicious orphan container conflict for filename=%s: insert reported conflict but row is missing", name)
		}
		if err != nil {
			return fmt.Errorf("query existing container after orphan conflict: %w", err)
		}

		if existingQuarantine && existingCurrentSize == fileSize && existingMaxSize == fileSize {
			reusedCount++
			continue
		}

		if !existingQuarantine {
			skippedExistingCount++
			continue
		}

		if !isStrictRecovery() {
			warningCount++
			continue
		}
		return fmt.Errorf(
			"suspicious orphan container conflict for filename=%s: existing quarantine row has current_size=%d max_size=%d expected_size=%d",
			name,
			existingCurrentSize,
			existingMaxSize,
			fileSize,
		)
	}

	logRecoveryEvent(
		"quarantine_orphan_containers_done",
		fmt.Sprintf("quarantined_count=%d", stats.quarantinedOrphan),
		fmt.Sprintf("reused_count=%d", reusedCount),
		fmt.Sprintf("skipped_existing_count=%d", skippedExistingCount),
		fmt.Sprintf("warning_count=%d", warningCount),
	)

	return nil
}

// detectInteriorGaps checks that completed blocks in the given container are
// contiguous and start immediately after the container header (offset 0 +
// ContainerHdrLen). Two conditions are detected:
//
//   - "first_block_not_at_header"   – the lowest block_offset does not equal
//     ContainerHdrLen, meaning the space between the header and the first block
//     is either missing or the block table is corrupt.
//   - "non_contiguous_block_offsets" – consecutive blocks (ordered by
//     block_offset) are not back-to-back, meaning there is an interior gap.
//
// An empty string is returned when no gap is detected or when the container has
// no completed blocks.
func detectInteriorGaps(ctx context.Context, dbconn *sql.DB, containerID int64, completedStatus string) (string, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT b.block_offset, b.stored_size
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE b.container_id = $1
		  AND c.status = $2
		ORDER BY b.block_offset
	`, containerID, completedStatus)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()

	expectedOffset := int64(container.ContainerHdrLen)
	isFirst := true
	for rows.Next() {
		var blockOffset, storedSize int64
		if err := rows.Scan(&blockOffset, &storedSize); err != nil {
			return "", err
		}
		if isFirst {
			isFirst = false
			if blockOffset != expectedOffset {
				return "first_block_not_at_header", nil
			}
		} else if blockOffset != expectedOffset {
			return "non_contiguous_block_offsets", nil
		}
		expectedOffset = blockOffset + storedSize
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return "", nil
}

func hasTrailingUnreferencedBytes(ctx context.Context, dbconn *sql.DB, containerID int64, currentSize int64, completedStatus string) (bool, error) {
	var maxEnd sql.NullInt64
	err := dbconn.QueryRowContext(ctx, `
		SELECT MAX(b.block_offset + b.stored_size)
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE b.container_id = $1
		  AND c.status = $2
	`, containerID, completedStatus).Scan(&maxEnd)
	if err != nil {
		return false, err
	}
	if !maxEnd.Valid {
		return false, nil
	}
	return maxEnd.Int64 < currentSize, nil
}
