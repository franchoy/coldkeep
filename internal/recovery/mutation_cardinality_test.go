package recovery

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func TestOrphanResyncFailsClosedWhenUpdateMatchesZero(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	const filename = "phase17-orphan.bin"
	if _, err := dbconn.Exec(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (?, 64, 64, FALSE, FALSE, TRUE)
	`, filename); err != nil {
		t.Fatalf("insert quarantined orphan row: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TRIGGER phase17_ignore_orphan_resync
		BEFORE UPDATE OF current_size ON container
		WHEN OLD.filename = 'phase17-orphan.bin'
		BEGIN
			SELECT RAISE(IGNORE);
		END
	`); err != nil {
		t.Fatalf("create ignored-resync trigger: %v", err)
	}

	reused, skipped, err := resolveOrphanConflictWithFS(
		context.Background(),
		dbconn,
		db.BackendSQLite,
		filename,
		128,
	)
	if !errors.Is(err, db.ErrMutationCardinality) {
		t.Fatalf("error=%v, want ErrMutationCardinality", err)
	}
	if reused || skipped {
		t.Fatalf("mismatch reported success: reused=%t skipped=%t", reused, skipped)
	}

	var currentSize, maxSize int64
	if err := dbconn.QueryRow(`SELECT current_size, max_size FROM container WHERE filename = ?`, filename).Scan(&currentSize, &maxSize); err != nil {
		t.Fatalf("read orphan row after failed resync: %v", err)
	}
	if currentSize != 64 || maxSize != 64 {
		t.Fatalf("orphan sizes changed: current=%d max=%d", currentSize, maxSize)
	}
}
