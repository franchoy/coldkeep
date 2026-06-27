package storage

import "database/sql"

// LookupLogicalFileIDByStoredPath performs the current stored-path batch
// dry-run lookup without mutating catalog state. The caller owns trimming and
// duplicate handling semantics.
func LookupLogicalFileIDByStoredPath(dbconn *sql.DB, storedPath string) (int64, error) {
	var logicalFileID int64
	err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, storedPath).Scan(&logicalFileID)
	if err != nil {
		return 0, err
	}
	return logicalFileID, nil
}
