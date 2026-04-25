package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	idb "github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openStatsBenchmarkDB(b *testing.B) *sql.DB {
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

func seedStatsBenchmarkDataset(b *testing.B, dbconn *sql.DB, logicalFileCount, chunkCount, refsPerFile int) {
	b.Helper()

	tx, err := dbconn.Begin()
	if err != nil {
		b.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	chunkStmt, err := tx.Prepare(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		VALUES (?, ?, 'COMPLETED', 0, ?)
	`)
	if err != nil {
		b.Fatalf("prepare chunk insert: %v", err)
	}
	defer func() { _ = chunkStmt.Close() }()

	for i := 0; i < chunkCount; i++ {
		version := "v1-simple-rolling"
		if i%2 == 1 {
			version = "v2-fastcdc"
		}
		hash := fmt.Sprintf("%064x", i+1)
		size := int64(4096 + (i % 8192))
		if _, err := chunkStmt.Exec(hash, size, version); err != nil {
			b.Fatalf("insert chunk row %d: %v", i, err)
		}
	}

	logicalFileStmt, err := tx.Prepare(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		VALUES (?, ?, ?, 'COMPLETED', ?)
	`)
	if err != nil {
		b.Fatalf("prepare logical_file insert: %v", err)
	}
	defer func() { _ = logicalFileStmt.Close() }()

	fileChunkStmt, err := tx.Prepare(`
		INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		b.Fatalf("prepare file_chunk insert: %v", err)
	}
	defer func() { _ = fileChunkStmt.Close() }()

	for i := 0; i < logicalFileCount; i++ {
		logicalFileID := int64(i + 1)
		version := "v1-simple-rolling"
		if i%2 == 1 {
			version = "v2-fastcdc"
		}
		fileHash := fmt.Sprintf("%064x", 1000000+i)
		if _, err := logicalFileStmt.Exec(
			fmt.Sprintf("bench-file-%d.bin", i),
			int64(16384+(i%4096)),
			fileHash,
			version,
		); err != nil {
			b.Fatalf("insert logical_file row %d: %v", i, err)
		}

		for j := 0; j < refsPerFile; j++ {
			chunkID := int64(((i * refsPerFile) + j) % chunkCount)
			chunkID++
			if _, err := fileChunkStmt.Exec(logicalFileID, chunkID, j); err != nil {
				b.Fatalf("insert file_chunk row file=%d order=%d: %v", logicalFileID, j, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		b.Fatalf("commit benchmark seed tx: %v", err)
	}
}

func benchmarkRunStatsResultWithDBMixedRepo(b *testing.B, logicalFileCount, chunkCount, refsPerFile int) {
	dbconn := openStatsBenchmarkDB(b)
	seedStatsBenchmarkDataset(b, dbconn, logicalFileCount, chunkCount, refsPerFile)

	ctx := context.Background()

	if _, err := runStatsResultWithDB(ctx, dbconn); err != nil {
		b.Fatalf("warmup runStatsResultWithDB: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := runStatsResultWithDB(ctx, dbconn)
		if err != nil {
			b.Fatalf("runStatsResultWithDB: %v", err)
		}
		if result.TotalChunks != int64(chunkCount) {
			b.Fatalf("unexpected total chunks: got=%d want=%d", result.TotalChunks, chunkCount)
		}
	}
}

func BenchmarkRunStatsResultWithDB_MixedRepoSmall(b *testing.B) {
	benchmarkRunStatsResultWithDBMixedRepo(b, 250, 1000, 4)
}

func BenchmarkRunStatsResultWithDB_MixedRepo(b *testing.B) {
	benchmarkRunStatsResultWithDBMixedRepo(b, 3000, 12000, 4)
}

func BenchmarkRunStatsResultWithDB_MixedRepoLarge(b *testing.B) {
	benchmarkRunStatsResultWithDBMixedRepo(b, 12000, 48000, 4)
}
