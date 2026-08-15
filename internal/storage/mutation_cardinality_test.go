package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func openMutationCardinalityStorageDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "phase17.sqlite") + "?_foreign_keys=on"
	dbconn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func insertMutationCardinalityLogicalFile(t *testing.T, dbconn *sql.DB, name, hash, status string, refCount int64) int64 {
	t.Helper()
	var fileID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES (?, 1, ?, ?, ?, 'v1-simple-rolling')
		RETURNING id
	`, name, hash, status, refCount).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	return fileID
}

func TestPhysicalFileMutationsFailClosedOnCardinalityMismatch(t *testing.T) {
	t.Run("missing-update", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		err := updatePhysicalFile(context.Background(), dbconn, "/phase17/missing", 404, physicalFileMetadata{})
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})

	t.Run("missing-increment", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		err := incrementLogicalFileRefCount(context.Background(), dbconn, 404)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})

	t.Run("zero-refcount-decrement", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		fileID := insertMutationCardinalityLogicalFile(t, dbconn, "zero-ref", "phase17-zero-ref", filestate.LogicalFileCompleted, 0)
		err := decrementLogicalFileRefCount(context.Background(), dbconn, fileID)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})

	t.Run("existing-same-value-update", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		fileID := insertMutationCardinalityLogicalFile(t, dbconn, "same-value", "phase17-same-value", filestate.LogicalFileCompleted, 1)
		const path = "/phase17/same-value"
		meta := physicalFileMetadata{IsMetadataComplete: true}
		if _, err := dbconn.Exec(`
			INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
			VALUES (?, ?, TRUE)
		`, path, fileID); err != nil {
			t.Fatalf("insert physical file: %v", err)
		}
		if err := updatePhysicalFile(context.Background(), dbconn, path, fileID, meta); err != nil {
			t.Fatalf("same-value update must match one row: %v", err)
		}
	})

	t.Run("ignored-replacement-delete", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		oldFileID := insertMutationCardinalityLogicalFile(t, dbconn, "old", "phase17-old", filestate.LogicalFileCompleted, 1)
		newFileID := insertMutationCardinalityLogicalFile(t, dbconn, "new", "phase17-new", filestate.LogicalFileCompleted, 0)
		const path = "/phase17/replace"
		if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, path, oldFileID); err != nil {
			t.Fatalf("insert physical mapping: %v", err)
		}
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_physical_replace_delete
			BEFORE DELETE ON physical_file
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create ignored-delete trigger: %v", err)
		}

		tx, err := dbconn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin replace transaction: %v", err)
		}
		err = replacePhysicalFileLogicalTargetTx(context.Background(), dbconn, tx, path, newFileID, physicalFileMetadata{})
		if !errors.Is(err, db.ErrMutationCardinality) {
			_ = tx.Rollback()
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback replacement: %v", err)
		}

		var mappedID int64
		if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = ?`, path).Scan(&mappedID); err != nil {
			t.Fatalf("read mapping after rollback: %v", err)
		}
		if mappedID != oldFileID {
			t.Fatalf("mapping changed after rollback: got=%d want=%d", mappedID, oldFileID)
		}
	})
}

func TestStoreRequiredMutationCardinalityFailuresRollBack(t *testing.T) {
	t.Run("container-sealing", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		tx, err := dbconn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin sealing transaction: %v", err)
		}
		err = markContainerSealingInTx(tx, 404)
		if !errors.Is(err, db.ErrMutationCardinality) {
			_ = tx.Rollback()
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		_ = tx.Rollback()
	})

	t.Run("linked-chunk-refcount", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		fileID := insertMutationCardinalityLogicalFile(t, dbconn, "link", "phase17-link", filestate.LogicalFileProcessing, 0)
		dbconn.SetMaxOpenConns(1)
		dbconn.SetMaxIdleConns(1)
		if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
			t.Fatalf("disable fixture foreign keys: %v", err)
		}
		tx, err := dbconn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin link transaction: %v", err)
		}
		err = linkFileChunkWithContext(context.Background(), tx, fileID, 404, 0, true)
		if !errors.Is(err, db.ErrMutationCardinality) {
			_ = tx.Rollback()
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback link transaction: %v", err)
		}
		var mappings int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = ?`, fileID).Scan(&mappings); err != nil {
			t.Fatalf("count rolled-back mappings: %v", err)
		}
		if mappings != 0 {
			t.Fatalf("link mismatch committed %d mappings", mappings)
		}
	})

	t.Run("missing-empty-logical-finalization", func(t *testing.T) {
		dbconn := openMutationCardinalityStorageDB(t)
		err := finalizeLogicalFileStorageWithContext(context.Background(), dbconn, 404, 0)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})

	t.Run("new-chunk-completion", func(t *testing.T) {
		dbconn, sgctx, path, codec := setupMutationCardinalityStore(t)
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_new_chunk_completion
			BEFORE UPDATE OF status ON chunk
			WHEN NEW.status = 'COMPLETED'
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create chunk completion trigger: %v", err)
		}

		_, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		var completed int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`).Scan(&completed); err != nil {
			t.Fatalf("count completed chunks: %v", err)
		}
		if completed != 0 {
			t.Fatalf("completion mismatch committed %d completed chunks", completed)
		}
	})

}

func setupMutationCardinalityStore(t *testing.T) (*sql.DB, StorageContext, string, blocks.Codec) {
	t.Helper()
	dbconn := openMutationCardinalityStorageDB(t)
	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, quarantine)
		 VALUES (1, 'ack_test_container.bin', ?, ?, FALSE, FALSE)`,
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert store container: %v", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "phase17-store.txt")
	if err := os.WriteFile(path, []byte("phase17 mutation cardinality store fixture"), 0o644); err != nil {
		t.Fatalf("write store fixture: %v", err)
	}
	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}
	return dbconn, StorageContext{
		DB:           dbconn,
		Writer:       &commitAckWriter{},
		ContainerDir: dir,
	}, path, codec
}

func TestRemoveFileFailsClosedWhenLogicalDeleteMatchesZero(t *testing.T) {
	dbconn := openMutationCardinalityStorageDB(t)
	fileID := insertMutationCardinalityLogicalFile(t, dbconn, "remove", "phase17-remove", filestate.LogicalFileCompleted, 0)
	if _, err := dbconn.Exec(`
		CREATE TRIGGER phase17_ignore_logical_file_delete
		BEFORE DELETE ON logical_file
		BEGIN
			SELECT RAISE(IGNORE);
		END
	`); err != nil {
		t.Fatalf("create ignored logical-delete trigger: %v", err)
	}

	_, err := RemoveFileWithDBResult(dbconn, fileID)
	if !errors.Is(err, db.ErrMutationCardinality) {
		t.Fatalf("error=%v, want ErrMutationCardinality", err)
	}
	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = ?`, fileID).Scan(&remaining); err != nil {
		t.Fatalf("count logical files after rollback: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("logical delete mismatch retained %d rows, want 1", remaining)
	}
}
