package recovery

import (
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
	quarantinedOrphan      int64
	skippedDirEntries      int64
	totalContainersChecked int64
	totalDiskFilesChecked  int64
}

type Report struct {
	AbortedLogicalFiles    int64
	AbortedChunks          int64
	QuarantinedMissing     int64
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
	err = quarantineOrphanContainers(dbconn, containersDir, stats)
	if err != nil {
		return buildReport(stats), err
	}

	logRecoveryEvent(
		"summary",
		fmt.Sprintf("aborted_logical_files=%d", stats.abortedLogicalFiles),
		fmt.Sprintf("aborted_chunks=%d", stats.abortedChunks),
		fmt.Sprintf("quarantined_missing_containers=%d", stats.quarantinedMissing),
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
		QuarantinedOrphan:      stats.quarantinedOrphan,
		SkippedDirEntries:      stats.skippedDirEntries,
		CheckedContainerRecord: stats.totalContainersChecked,
		CheckedDiskFiles:       stats.totalDiskFilesChecked,
	}
}

func abortProcessingLogicalFiles(dbconn *sql.DB, stats *recoveryStats) error {
	logRecoveryEvent("abort_processing_logical_files_start")
	result, err := dbconn.Exec(`UPDATE logical_file SET status = $1 WHERE status = $2`, filestate.LogicalFileAborted, filestate.LogicalFileProcessing)
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
	logRecoveryEvent("abort_processing_chunks_start")
	result, err := dbconn.Exec(`UPDATE chunk SET status = $1 WHERE status = $2`, filestate.ChunkAborted, filestate.ChunkProcessing)
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
	logRecoveryEvent("quarantine_missing_containers_start")
	rows, err := dbconn.Query(`SELECT id, filename FROM container WHERE quarantine = FALSE`)
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

			_, err := dbconn.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, id)
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

func quarantineOrphanContainers(dbconn *sql.DB, containersDir string, stats *recoveryStats) error {
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
		stats.totalDiskFilesChecked++
		// check if a container record exists for this filename
		var exists bool
		err = dbconn.QueryRow(`SELECT EXISTS(SELECT 1 FROM container WHERE filename = $1)`, name).Scan(&exists)
		if err != nil {
			return fmt.Errorf("query check container existence: %w", err)
		}
		if !exists {
			_, err := dbconn.Exec(`INSERT INTO container (filename, quarantine, current_size, max_size) VALUES ($1, TRUE, $2, $3) ON CONFLICT (filename) DO NOTHING`, name, fileinfo.Size(), fileinfo.Size())
			if err != nil {
				return fmt.Errorf("insert orphan container record: %w", err)
			}
			stats.quarantinedOrphan++
			logRecoveryEvent(
				"quarantine_orphan_container",
				"filename="+name,
				fmt.Sprintf("size_bytes=%d", fileinfo.Size()),
				"reason=orphan_file_on_disk",
			)
		}
	}

	logRecoveryEvent("quarantine_orphan_containers_done", fmt.Sprintf("quarantined_count=%d", stats.quarantinedOrphan))

	return nil
}
