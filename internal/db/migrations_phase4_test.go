package db

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestRunMigrationsCreatesUniqueOffsetIndexOnStorageBlocks verifies that
// RunMigrations (V3) creates the unique index on (container_id, container_offset).
func TestRunMigrationsCreatesUniqueOffsetIndexOnStorageBlocks(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	var count int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_storage_blocks_container_id_offset'`,
	).Scan(&count); err != nil {
		t.Fatalf("query index existence: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected idx_storage_blocks_container_id_offset to exist after RunMigrations, got count=%d", count)
	}
}

// TestStorageBlocksUniqueContainerOffsetConstraintPreventsOverlap verifies that
// inserting two storage_blocks rows with the same (container_id, container_offset)
// is rejected after the V3 migration.
func TestStorageBlocksUniqueContainerOffsetConstraintPreventsOverlap(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Insert a container.
	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ('overlap-test.ck', 128, 67108864, TRUE)
		 RETURNING id`,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	insertBlock := func(offset int64) error {
		_, err := dbconn.Exec(
			`INSERT INTO storage_blocks
			 (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
			 VALUES (1, 'none', 64, 64, $1, $2, X'deadbeef')`,
			containerID, offset,
		)
		return err
	}

	// First insertion at offset 64: must succeed.
	if err := insertBlock(64); err != nil {
		t.Fatalf("expected first storage_blocks insert to succeed, got: %v", err)
	}

	// Second insertion at the same offset: must fail.
	err = insertBlock(64)
	if err == nil {
		t.Fatal("expected duplicate (container_id, container_offset) to be rejected, got nil error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("expected UNIQUE constraint violation, got: %v", err)
	}

	// Insertion at a different offset: must succeed.
	if err := insertBlock(128); err != nil {
		t.Fatalf("expected different-offset insert to succeed, got: %v", err)
	}
}

// TestStorageBlocksUniqueOffsetMigrationPreflightBlocksDuplicates verifies that
// runSQLiteStorageBlocksUniqueOffsetConstraintMigration fails with a diagnostic
// message when duplicate (container_id, container_offset) pairs exist.
func TestStorageBlocksUniqueOffsetMigrationPreflightBlocksDuplicates(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	// Apply just the schema SQL tables without the migration (bypass the full RunMigrations).
	// We'll apply a partial schema to simulate an old install with no index yet.
	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable fk: %v", err)
	}
	if _, err := dbconn.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create schema_version: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TABLE IF NOT EXISTS container (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			filename TEXT NOT NULL UNIQUE,
			sealed INTEGER NOT NULL DEFAULT 0,
			sealing INTEGER NOT NULL DEFAULT 0,
			quarantine INTEGER NOT NULL DEFAULT 0,
			current_size INTEGER NOT NULL DEFAULT 0,
			max_size INTEGER NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		t.Fatalf("create container: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TABLE IF NOT EXISTS storage_blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			format_version INTEGER NOT NULL,
			codec TEXT NOT NULL,
			plaintext_size INTEGER NOT NULL,
			stored_size INTEGER NOT NULL,
			container_id INTEGER NOT NULL,
			container_offset INTEGER NOT NULL,
			block_hash BLOB NOT NULL
		)`); err != nil {
		t.Fatalf("create storage_blocks: %v", err)
	}

	// Insert container.
	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size) VALUES ('dup-test.ck', 128, 67108864) RETURNING id`,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	// Insert two rows with the same (container_id, container_offset) — simulates a pre-existing bug.
	for i := 0; i < 2; i++ {
		if _, err := dbconn.Exec(
			`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
			 VALUES (1, 'none', 64, 64, $1, 64, X'aabbccdd')`,
			containerID,
		); err != nil {
			t.Fatalf("insert duplicate block %d: %v", i, err)
		}
	}

	ctx := context.Background()
	err = runSQLiteStorageBlocksUniqueOffsetConstraintMigration(dbconn, ctx)
	if err == nil {
		t.Fatal("expected migration to fail on duplicate offset pairs, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate offset pair") {
		t.Fatalf("expected 'duplicate offset pair' diagnostic, got: %v", err)
	}
}

// TestRunMigrationsIdempotentForUniqueOffsetIndex verifies that calling
// RunMigrations twice does not error on the unique index creation.
func TestRunMigrationsIdempotentForUniqueOffsetIndex(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	for i := 1; i <= 2; i++ {
		if err := RunMigrations(dbconn); err != nil {
			t.Fatalf("RunMigrations call %d: %v", i, err)
		}
	}
}
