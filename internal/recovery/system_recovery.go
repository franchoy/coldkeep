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
}

type Report struct {
	AbortedLogicalFiles    int64
	AbortedChunks          int64
	QuarantinedMissing     int64
	QuarantinedCorruptTail int64
	QuarantinedOrphan      int64
	SkippedDirEntries      int64
	CheckedContainerRecord int64
	CheckedDiskFiles       int64
}

func logRecoveryEvent(action string, fields ...string) {
	line := "event=recovery action=" + action
	if len(fields) > 0 {
		line += " " + strings.Join(fields, " ")
	}
	log.Println(line)
}

func SystemRecovery() error {
	_, err := SystemRecoveryReportWithContainersDir(container.ContainersDir)
	return err
}

func SystemRecoveryWithContainersDir(containersDir string) error {
	_, err := SystemRecoveryReportWithContainersDir(containersDir)
	return err
}

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
		if currentSize > physicalSize {
			reason = "db_current_size_past_eof"
		} else {
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
		}

		if reason == "" {
			continue
		}

		_, err = dbconn.ExecContext(ctx, `UPDATE container SET quarantine = TRUE WHERE id = $1`, id)
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
			return fmt.Errorf("suspicious orphan container conflict for filename=%s: insert reported conflict but row is missing", name)
		}
		if err != nil {
			return fmt.Errorf("query existing container after orphan conflict: %w", err)
		}

		if existingQuarantine && existingCurrentSize == fileSize && existingMaxSize == fileSize {
			logRecoveryEvent(
				"quarantine_orphan_container_reused",
				"filename="+name,
				fmt.Sprintf("size_bytes=%d", fileSize),
				"reason=duplicate_retrier",
			)
			continue
		}

		if !existingQuarantine {
			logRecoveryEvent(
				"quarantine_orphan_container_skipped",
				"filename="+name,
				fmt.Sprintf("size_bytes=%d", fileSize),
				"reason=existing_non_quarantine_row",
			)
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

	logRecoveryEvent("quarantine_orphan_containers_done", fmt.Sprintf("quarantined_count=%d", stats.quarantinedOrphan))

	return nil
}
