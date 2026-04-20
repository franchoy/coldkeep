package testdb

import (
	"database/sql"
	"testing"
)

func EnsureSnapshotPathID(t *testing.T, dbconn *sql.DB, snapshotPath string) int64 {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path(path) VALUES ($1) ON CONFLICT(path) DO NOTHING`, snapshotPath); err != nil {
		t.Fatalf("insert snapshot_path %q: %v", snapshotPath, err)
	}
	var pathID int64
	if err := dbconn.QueryRow(`SELECT id FROM snapshot_path WHERE path = $1`, snapshotPath).Scan(&pathID); err != nil {
		t.Fatalf("lookup snapshot_path id for %q: %v", snapshotPath, err)
	}
	return pathID
}

func InsertSnapshotFileRef(t *testing.T, dbconn *sql.DB, snapshotID, snapshotPath string, logicalFileID int64) {
	t.Helper()
	pathID := EnsureSnapshotPathID(t, dbconn, snapshotPath)
	InsertSnapshotFileRefByPathID(t, dbconn, snapshotID, pathID, logicalFileID)
}

func InsertSnapshotFileRefByPathID(t *testing.T, dbconn *sql.DB, snapshotID string, pathID, logicalFileID int64) {
	t.Helper()
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)`,
		snapshotID, pathID, logicalFileID,
	); err != nil {
		t.Fatalf("insert snapshot_file snapshot_id=%q path_id=%d logical_file_id=%d: %v", snapshotID, pathID, logicalFileID, err)
	}
}
