package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	_ "github.com/mattn/go-sqlite3"
)

func openMutationCardinalityMaintenanceDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestRepairRefCountsFailsClosedOnAffectedCountMismatch(t *testing.T) {
	t.Run("logical-file", func(t *testing.T) {
		dbconn := openMutationCardinalityMaintenanceDB(t)
		if _, err := dbconn.Exec(`
			INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
			VALUES ('phase17-logical', 1, 'phase17-logical-hash', 'COMPLETED', 1, 'v1-simple-rolling')
		`); err != nil {
			t.Fatalf("insert logical fixture: %v", err)
		}
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_logical_refcount_repair
			BEFORE UPDATE OF ref_count ON logical_file
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create logical repair trigger: %v", err)
		}

		_, err := RepairLogicalRefCountsResultWithDB(dbconn)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		var refCount int64
		if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file`).Scan(&refCount); err != nil {
			t.Fatalf("read logical refcount: %v", err)
		}
		if refCount != 1 {
			t.Fatalf("logical refcount changed despite rollback: %d", refCount)
		}
	})

	t.Run("chunk", func(t *testing.T) {
		dbconn := openMutationCardinalityMaintenanceDB(t)
		if _, err := dbconn.Exec(`
			INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
			VALUES ('phase17-chunk-hash', 1, 'COMPLETED', 1, 0, 'v1-simple-rolling')
		`); err != nil {
			t.Fatalf("insert chunk fixture: %v", err)
		}
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_chunk_refcount_repair
			BEFORE UPDATE OF live_ref_count ON chunk
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create chunk repair trigger: %v", err)
		}

		_, err := RepairChunkLiveRefCountsResultWithDB(dbconn)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		var liveRefCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk`).Scan(&liveRefCount); err != nil {
			t.Fatalf("read chunk refcount: %v", err)
		}
		if liveRefCount != 1 {
			t.Fatalf("chunk refcount changed despite rollback: %d", liveRefCount)
		}
	})
}

func TestGCRequiredDeletesFailClosedOnAffectedCountMismatch(t *testing.T) {
	t.Run("sealed-container-keeps-file", func(t *testing.T) {
		dbconn := openMutationCardinalityMaintenanceDB(t)
		dir := t.TempDir()
		filename := "phase17-sealed-gc.bin"
		path := filepath.Join(dir, filename)
		if err := os.WriteFile(path, []byte("phase17"), 0o600); err != nil {
			t.Fatalf("write container fixture: %v", err)
		}
		result, err := dbconn.Exec(
			`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
			 VALUES (?, 7, ?, TRUE, FALSE)`,
			filename,
			container.GetContainerMaxSize(),
		)
		if err != nil {
			t.Fatalf("insert sealed container: %v", err)
		}
		containerID, err := result.LastInsertId()
		if err != nil {
			t.Fatalf("container id: %v", err)
		}
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_sealed_container_delete
			BEFORE DELETE ON container
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create sealed-delete trigger: %v", err)
		}

		tx, err := dbconn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin GC transaction: %v", err)
		}
		err = commitGCContainerDeletion(context.Background(), tx, containerID, dir, filename, fsx.Default())
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("physical file removed after missed metadata delete: %v", err)
		}
		var remaining int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = ?`, containerID).Scan(&remaining); err != nil {
			t.Fatalf("count container rows: %v", err)
		}
		if remaining != 1 {
			t.Fatalf("container rollback count=%d, want 1", remaining)
		}
	})

	t.Run("packed-storage-block", func(t *testing.T) {
		dbconn := openMutationCardinalityMaintenanceDB(t)
		containerResult, err := dbconn.Exec(
			`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
			 VALUES ('phase17-packed-gc.bin', 0, ?, TRUE, FALSE)`,
			container.GetContainerMaxSize(),
		)
		if err != nil {
			t.Fatalf("insert packed container: %v", err)
		}
		containerID, err := containerResult.LastInsertId()
		if err != nil {
			t.Fatalf("container id: %v", err)
		}
		blockResult, err := dbconn.Exec(`
			INSERT INTO storage_blocks
				(format_version, codec, plaintext_size, compression_codec, stored_size, container_id, container_offset, block_hash)
			VALUES (1, 'none', 1, 'none', 1, ?, 0, X'01')
		`, containerID)
		if err != nil {
			t.Fatalf("insert packed block: %v", err)
		}
		blockID, err := blockResult.LastInsertId()
		if err != nil {
			t.Fatalf("block id: %v", err)
		}
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_storage_block_delete
			BEFORE DELETE ON storage_blocks
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create block-delete trigger: %v", err)
		}

		tx, err := dbconn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin packed-block transaction: %v", err)
		}
		err = deletePackedBlockMetadata(context.Background(), tx, blockID)
		if !errors.Is(err, db.ErrMutationCardinality) {
			_ = tx.Rollback()
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback packed-block transaction: %v", err)
		}
		var remaining int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = ?`, blockID).Scan(&remaining); err != nil {
			t.Fatalf("count storage blocks: %v", err)
		}
		if remaining != 1 {
			t.Fatalf("storage block rollback count=%d, want 1", remaining)
		}
	})

	t.Run("active-container-keeps-file", func(t *testing.T) {
		dbconn := openMutationCardinalityMaintenanceDB(t)
		dir := t.TempDir()
		filename := "phase17-active-gc.bin"
		path := filepath.Join(dir, filename)
		if err := os.WriteFile(path, []byte("phase17"), 0o600); err != nil {
			t.Fatalf("write active container fixture: %v", err)
		}
		result, err := dbconn.Exec(
			`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
			 VALUES (?, 7, ?, FALSE, FALSE)`,
			filename,
			container.GetContainerMaxSize(),
		)
		if err != nil {
			t.Fatalf("insert active container: %v", err)
		}
		containerID, err := result.LastInsertId()
		if err != nil {
			t.Fatalf("container id: %v", err)
		}
		if _, err := dbconn.Exec(`
			CREATE TRIGGER phase17_ignore_active_container_delete
			BEFORE DELETE ON container
			BEGIN
				SELECT RAISE(IGNORE);
			END
		`); err != nil {
			t.Fatalf("create active-delete trigger: %v", err)
		}

		err = sweepDeadActiveContainer(
			context.Background(),
			dbconn,
			dir,
			livePhysicalUnits{
				LegacyLiveContainerIDs: map[int64]struct{}{},
				PackedLiveBlockIDs:     map[int64]struct{}{},
			},
			fsx.Default(),
			containerID,
			filename,
		)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("active physical file removed after missed metadata delete: %v", err)
		}
		var remaining int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = ?`, containerID).Scan(&remaining); err != nil {
			t.Fatalf("count active container rows: %v", err)
		}
		if remaining != 1 {
			t.Fatalf("active container rollback count=%d, want 1", remaining)
		}
	})
}
