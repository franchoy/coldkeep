package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"testing"

	idb "github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openBenchmarkDB(b *testing.B) *sql.DB {
	b.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open sqlite db: %v", err)
	}
	b.Cleanup(func() { _ = dbconn.Close() })

	if err := idb.RunMigrations(dbconn); err != nil {
		b.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func seedBenchmarkPhysicalFiles(b *testing.B, dbconn *sql.DB, fileCount int) {
	b.Helper()
	ctx := context.Background()

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		b.Fatalf("begin seed transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	insertLogicalStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES ($1, $2, $3, $4)`,
	)
	if err != nil {
		b.Fatalf("prepare logical_file insert: %v", err)
	}
	defer func() { _ = insertLogicalStmt.Close() }()

	insertPhysicalStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete) VALUES ($1, $2, $3, $4, $5)`,
	)
	if err != nil {
		b.Fatalf("prepare physical_file insert: %v", err)
	}
	defer func() { _ = insertPhysicalStmt.Close() }()

	for i := 0; i < fileCount; i++ {
		logicalRes, err := insertLogicalStmt.ExecContext(
			ctx,
			fmt.Sprintf("bench-file-%06d.bin", i),
			int64(4096+i%128),
			fmt.Sprintf("bench-hash-%064d", i),
			"COMPLETED",
		)
		if err != nil {
			b.Fatalf("insert logical_file row %d: %v", i, err)
		}
		logicalID, err := logicalRes.LastInsertId()
		if err != nil {
			b.Fatalf("last insert id for logical_file row %d: %v", i, err)
		}

		if _, err := insertPhysicalStmt.ExecContext(
			ctx,
			fmt.Sprintf("bench/%05d/file-%06d.bin", i/100, i),
			logicalID,
			sql.NullInt64{},
			sql.NullTime{},
			1,
		); err != nil {
			b.Fatalf("insert physical_file row %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		b.Fatalf("commit seed transaction: %v", err)
	}
}

func benchmarkCreateSnapshotFullNFiles(b *testing.B, fileCount int) {
	dbconn := openBenchmarkDB(b)
	seedBenchmarkPhysicalFiles(b, dbconn, fileCount)

	// Keep benchmark output readable by suppressing per-row snapshot logs.
	originalLogOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		log.SetOutput(originalLogOutput)
	})

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		snapshotID := fmt.Sprintf("bench-snapshot-%d", i)
		if err := CreateSnapshot(ctx, dbconn, snapshotID, "full", nil, nil, nil); err != nil {
			b.Fatalf("CreateSnapshot iteration %d: %v", i, err)
		}
	}
}

func BenchmarkCreateSnapshotFull_1000Files(b *testing.B) {
	benchmarkCreateSnapshotFullNFiles(b, 1000)
}

func BenchmarkCreateSnapshotFull_10000Files(b *testing.B) {
	benchmarkCreateSnapshotFullNFiles(b, 10000)
}
