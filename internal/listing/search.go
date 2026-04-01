package listing

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

// SearchFilesResult returns matching records without printing them.
func SearchFilesResult(args []string) ([]FileRecord, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	return SearchFilesResultWithDB(dbconn, args)
}

// SearchFilesResultWithDB returns matching records using a caller-managed DB connection.
func SearchFilesResultWithDB(dbconn *sql.DB, args []string) ([]FileRecord, error) {
	if dbconn == nil {
		return nil, fmt.Errorf("db connection is nil")
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	limit, offset, err := parsePaginationArgs(args)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT id, original_name, file_hash, total_size, created_at
		FROM logical_file
		WHERE status = $1
	`
	params := []interface{}{filestate.LogicalFileCompleted}
	paramIndex := 2

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing argument for --name")
			}
			i++
			query += fmt.Sprintf(" AND original_name ILIKE $%d", paramIndex)
			params = append(params, "%"+args[i]+"%")
			paramIndex++

		case "--min-size":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing argument for --min-size")
			}
			i++
			size, err := parseNonNegativeIntArg("min-size", args[i])
			if err != nil {
				return nil, err
			}
			query += fmt.Sprintf(" AND total_size >= $%d", paramIndex)
			params = append(params, size)
			paramIndex++

		case "--max-size":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing argument for --max-size")
			}
			i++
			size, err := parseNonNegativeIntArg("max-size", args[i])
			if err != nil {
				return nil, err
			}
			query += fmt.Sprintf(" AND total_size <= $%d", paramIndex)
			params = append(params, size)
			paramIndex++

		case "--limit", "--offset":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing argument for %s", args[i])
			}
			i++
		}
	}

	query += " ORDER BY created_at DESC"
	query, params = applyPagination(query, params, paramIndex, limit, offset)

	rows, err := dbconn.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var records []FileRecord
	for rows.Next() {
		var id int64
		var name string
		var fileHash string
		var size int64
		var created time.Time
		if err := rows.Scan(&id, &name, &fileHash, &size, &created); err != nil {
			return nil, err
		}
		records = append(records, FileRecord{
			ID:        id,
			Name:      name,
			FileHash:  fileHash,
			SizeBytes: size,
			CreatedAt: created.Format("2006-01-02 15:04:05"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
