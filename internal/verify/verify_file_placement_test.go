package verify

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"

	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestVerifyFileDeepRejectsPackedChunkHashMismatch(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		containersDir := t.TempDir()
		payload := []byte("packed file-deep hash contract")
		_, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{payload}, nil)
		logicalID := seedVerifyFileRecipe(t, backend.DB, "packed-corrupt.bin", [][]byte{payload}, chunkIDs)
		if err := VerifyFileDeepWithContainersDir(backend.DB, int(logicalID), containersDir); err != nil {
			t.Fatalf("healthy packed file-deep verification: %v", err)
		}

		if _, err := backend.DB.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("0", 64), chunkIDs[0]); err != nil {
			t.Fatalf("corrupt packed chunk hash metadata: %v", err)
		}
		if err := VerifyFileDeepWithContainersDir(backend.DB, int(logicalID), containersDir); err == nil || !strings.Contains(err.Error(), "chunk hash verification failed") {
			t.Fatalf("packed chunk hash mismatch must fail file-deep verification, got: %v", err)
		}
	})
}

func TestVerifyFileDeepCoversLegacyPackedAndMixedRecipes(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		legacy [][]byte
		packed [][]byte
	}{
		{name: "legacy", legacy: [][]byte{[]byte("legacy-only")}},
		{name: "packed", packed: [][]byte{[]byte("packed-one"), []byte("packed-two")}},
		{name: "mixed", legacy: [][]byte{[]byte("mixed-legacy")}, packed: [][]byte{[]byte("mixed-packed")}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			dbconn := openVerifyTestDB(t)
			defer func() { _ = dbconn.Close() }()
			containersDir := t.TempDir()
			var payloads [][]byte
			var chunkIDs []int64
			for _, payload := range fixture.legacy {
				chunkIDs = append(chunkIDs, seedVerifyLegacyBlockFixture(t, dbconn, containersDir, payload))
				payloads = append(payloads, payload)
			}
			if len(fixture.packed) > 0 {
				_, packedIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, fixture.packed, nil)
				chunkIDs = append(chunkIDs, packedIDs...)
				payloads = append(payloads, fixture.packed...)
			}
			logicalID := seedVerifyFileRecipe(t, dbconn, fixture.name+".bin", payloads, chunkIDs)
			if err := VerifyFileDeepWithContainersDir(dbconn, int(logicalID), containersDir); err != nil {
				t.Fatalf("healthy %s file-deep verification: %v", fixture.name, err)
			}
		})
	}
}

func seedVerifyFileRecipe(t *testing.T, dbconn *sql.DB, name string, payloads [][]byte, chunkIDs []int64) int64 {
	t.Helper()
	if len(payloads) != len(chunkIDs) {
		t.Fatalf("fixture payload/chunk mismatch: %d != %d", len(payloads), len(chunkIDs))
	}
	h := sha256.New()
	var totalSize int64
	for _, payload := range payloads {
		totalSize += int64(len(payload))
		_, _ = h.Write(payload)
	}
	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 1, 'v1-simple-rolling') RETURNING id`,
		name, totalSize, hex.EncodeToString(h.Sum(nil)), filestate.LogicalFileCompleted,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert verify logical file: %v", err)
	}
	for order, chunkID := range chunkIDs {
		if _, err := dbconn.Exec(`UPDATE chunk SET status = $1, live_ref_count = 1 WHERE id = $2`, filestate.ChunkCompleted, chunkID); err != nil {
			t.Fatalf("complete verify chunk %d: %v", chunkID, err)
		}
		if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`, logicalID, chunkID, order); err != nil {
			t.Fatalf("insert verify file recipe order %d: %v", order, err)
		}
	}
	return logicalID
}
