package listing

import (
	"fmt"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
)

// FileRecord is a single logical file entry returned by list/search.
type FileRecord struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
	CreatedAt string `json:"created_at"`
}

// ListFilesResult returns the raw records without printing them.
func ListFilesResult() ([]FileRecord, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	rows, err := dbconn.Query(`
		SELECT id, original_name, total_size, created_at
		FROM logical_file
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var records []FileRecord
	for rows.Next() {
		var id int64
		var name string
		var size int64
		var created time.Time
		if err := rows.Scan(&id, &name, &size, &created); err != nil {
			return nil, err
		}
		records = append(records, FileRecord{
			ID:        id,
			Name:      name,
			SizeBytes: size,
			CreatedAt: created.Format("2006-01-02 15:04:05"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
