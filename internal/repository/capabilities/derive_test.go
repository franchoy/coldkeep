package capabilities

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	_ "github.com/mattn/go-sqlite3"
)

func TestDeriveFreshSQLiteRepositoryCapabilities(t *testing.T) {
	dbconn := openSQLiteWithSchema(t)
	defer func() { _ = dbconn.Close() }()

	caps, err := Derive(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("derive capabilities: %v", err)
	}

	assertSetEqual(t, caps.SupportedCompression, []string{"none", "zstd"})
	assertSetEqual(t, caps.SupportedEncryption, []string{"aes-gcm", "none"})
	assertSetEqual(t, caps.SupportedPacking, []string{"legacy-single", "packed-multi"})

	if len(caps.ObservedCompression) != 0 {
		t.Fatalf("expected no observed compression in empty repo, got %v", caps.ObservedCompression)
	}
	if len(caps.ObservedEncryption) != 0 {
		t.Fatalf("expected no observed encryption in empty repo, got %v", caps.ObservedEncryption)
	}
	if len(caps.ObservedPacking) != 0 {
		t.Fatalf("expected no observed packing in empty repo, got %v", caps.ObservedPacking)
	}

	if !caps.SupportsCompressedHash {
		t.Fatalf("expected compressed hash support")
	}
	if !caps.SupportsPhysicalHash {
		t.Fatalf("expected physical hash support")
	}
	if caps.RepositoryFormatVersion < 15 {
		t.Fatalf("expected repository format version >= 15, got %d", caps.RepositoryFormatVersion)
	}

	if caps.DefaultCompression != "none" {
		t.Fatalf("expected default compression none, got %q", caps.DefaultCompression)
	}
	if caps.DefaultCompressionLevel != 3 {
		t.Fatalf("expected default compression level 3, got %d", caps.DefaultCompressionLevel)
	}
}

func TestDeriveObservedMixedCapabilities(t *testing.T) {
	dbconn := openSQLiteWithSchema(t)
	defer func() { _ = dbconn.Close() }()

	seedRepositoryForMixedObservation(t, dbconn)

	caps, err := Derive(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("derive capabilities: %v", err)
	}

	assertSetEqual(t, caps.ObservedPacking, []string{"legacy-single", "packed-multi"})
	assertSetEqual(t, caps.ObservedCompression, []string{"none", "zstd"})
	assertSetEqual(t, caps.ObservedEncryption, []string{"aes-gcm", "none"})
}

func TestDeriveLegacyOnlyLayoutCapabilities(t *testing.T) {
	dbconn := openSQLiteWithSchema(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`DROP TABLE chunk_block_refs`); err != nil {
		t.Fatalf("drop chunk_block_refs: %v", err)
	}
	if _, err := dbconn.Exec(`DROP TABLE storage_blocks`); err != nil {
		t.Fatalf("drop storage_blocks: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES (1, 'plain', 1, 1024, 1024, X'', 1, 0)
	`); err != nil {
		t.Fatalf("insert legacy block after dropping packed tables: %v", err)
	}

	caps, err := Derive(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("derive capabilities: %v", err)
	}

	assertSetEqual(t, caps.SupportedPacking, []string{"legacy-single"})
	assertSetEqual(t, caps.SupportedCompression, []string{"none"})
	if caps.SupportsCompressedHash {
		t.Fatalf("expected compressed hash support false when storage_blocks is absent")
	}
	if caps.SupportsPhysicalHash {
		t.Fatalf("expected physical hash support false when storage_blocks is absent")
	}
	assertSetEqual(t, caps.ObservedPacking, []string{"legacy-single"})
}

func openSQLiteWithSchema(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := dbconn.Exec(dbschema.SQLiteSchema); err != nil {
		_ = dbconn.Close()
		t.Fatalf("apply sqlite schema: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO container(id, filename, max_size, current_size, sealed, created_at, updated_at)
		VALUES (1, 'container-0001.bin', 10485760, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("seed container: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO chunk(id, chunk_hash, size, status, live_ref_count, pin_count, created_at, updated_at, chunker_version)
		VALUES (1, X'01', 1024, 'COMPLETED', 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'v2-fastcdc')
	`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("seed chunk #1: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO chunk(id, chunk_hash, size, status, live_ref_count, pin_count, created_at, updated_at, chunker_version)
		VALUES (2, X'02', 1024, 'COMPLETED', 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'v2-fastcdc')
	`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("seed chunk #2: %v", err)
	}

	return dbconn
}

func seedRepositoryForMixedObservation(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES (1, 'plain', 1, 1024, 1024, X'', 1, 0)
	`); err != nil {
		t.Fatalf("insert legacy blocks row: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO storage_blocks (
			id, format_version, codec, plaintext_size, compression_codec, compression_level,
			compressed_size, stored_size, container_id, container_offset, block_hash,
			compression_ratio, payload_hash, compressed_hash, physical_hash, created_at
		)
		VALUES (
			100, 1, 'none', 1024, 'none', NULL,
			NULL, 1024, 1, 0, X'aa',
			1.0, 'hash-none', NULL, NULL, CURRENT_TIMESTAMP
		)
	`); err != nil {
		t.Fatalf("insert storage_blocks none row: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO storage_blocks (
			id, format_version, codec, plaintext_size, compression_codec, compression_level,
			compressed_size, stored_size, container_id, container_offset, block_hash,
			compression_ratio, payload_hash, compressed_hash, physical_hash, created_at
		)
		VALUES (
			101, 1, 'aes-gcm', 1024, 'zstd', 3,
			700, 760, 1, 1024, X'bb',
			1.46, 'hash-zstd', X'cc', X'dd', CURRENT_TIMESTAMP
		)
	`); err != nil {
		t.Fatalf("insert storage_blocks zstd row: %v", err)
	}
}

func assertSetEqual(t *testing.T, got, want []string) {
	t.Helper()

	gotCopy := append([]string(nil), got...)
	wantCopy := append([]string(nil), want...)
	sort.Strings(gotCopy)
	sort.Strings(wantCopy)

	if len(gotCopy) != len(wantCopy) {
		t.Fatalf("set length mismatch: got=%v want=%v", gotCopy, wantCopy)
	}
	for i := range gotCopy {
		if gotCopy[i] != wantCopy[i] {
			t.Fatalf("set mismatch: got=%v want=%v", gotCopy, wantCopy)
		}
	}
}
