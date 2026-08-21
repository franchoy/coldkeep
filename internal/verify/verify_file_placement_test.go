package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
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
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		containersDir := t.TempDir()
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
				var payloads [][]byte
				var chunkIDs []int64
				for _, payload := range fixture.legacy {
					chunkIDs = append(chunkIDs, seedVerifyLegacyBlockFixture(t, backend.DB, containersDir, payload))
					payloads = append(payloads, payload)
				}
				if len(fixture.packed) > 0 {
					_, packedIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, fixture.packed, nil)
					chunkIDs = append(chunkIDs, packedIDs...)
					payloads = append(payloads, fixture.packed...)
				}
				logicalID := seedVerifyFileRecipe(t, backend.DB, fixture.name+".bin", payloads, chunkIDs)
				if err := VerifyFileDeepWithContainersDir(backend.DB, int(logicalID), containersDir); err != nil {
					t.Fatalf("healthy %s file-deep verification: %v", fixture.name, err)
				}
			})
		}
	})
}

func TestVerifyFileDeepFailsClosedAcrossPlacementFaults(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		containersDir := t.TempDir()
		t.Run("missing-placement", func(t *testing.T) {
			payload := []byte("missing-placement")
			_, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{payload}, nil)
			logicalID := seedVerifyFileRecipe(t, backend.DB, "missing-placement.bin", [][]byte{payload}, chunkIDs)
			if _, err := backend.DB.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
				t.Fatal(err)
			}
			assertVerifyFileDeepFails(t, backend.DB, logicalID, containersDir, "no associated block")
		})

		t.Run("incomplete-placement", func(t *testing.T) {
			payload := []byte("incomplete-placement")
			_, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{payload}, nil)
			logicalID := seedVerifyFileRecipe(t, backend.DB, "incomplete-placement.bin", [][]byte{payload}, chunkIDs)
			if _, err := backend.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkProcessing, chunkIDs[0]); err != nil {
				t.Fatal(err)
			}
			assertVerifyFileDeepFails(t, backend.DB, logicalID, containersDir, "invalid status")
		})

		t.Run("conflicting-placement", func(t *testing.T) {
			payload := []byte("conflicting-placement")
			blockID, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{payload}, nil)
			if _, err := backend.DB.Exec(`
				INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
				VALUES ($1, $2, 1, $3)`, chunkIDs[0], blockID, int64(len(payload))); err == nil {
				t.Fatal("supported schema accepted a second authoritative packed placement for one chunk")
			}
		})

		t.Run("malformed-packed-range", func(t *testing.T) {
			payload := []byte("malformed-packed-range")
			_, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{payload}, nil)
			logicalID := seedVerifyFileRecipe(t, backend.DB, "malformed-packed-range.bin", [][]byte{payload}, chunkIDs)
			if _, err := backend.DB.Exec(`UPDATE chunk_block_refs SET offset_in_block = offset_in_block + 1 WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
				t.Fatal(err)
			}
			placements, err := catalog.NewServiceFromSQL(backend.DB).LoadChunkPlacements(context.Background(), logicalID)
			if err != nil || len(placements) != 1 {
				t.Fatalf("load malformed placement: placements=%+v err=%v", placements, err)
			}
			state := filePlacementVerifyState{containersDir: containersDir, packedBlocks: make(map[int64]*VerifiedBlock)}
			if err := state.verify(context.Background(), placements[0]); err == nil || !strings.Contains(err.Error(), "table/range mismatch") {
				t.Fatalf("malformed range verification error=%v", err)
			}
			assertVerifyFileDeepFails(t, backend.DB, logicalID, containersDir, "chunk hash verification failed")
		})

		t.Run("legacy-payload-corruption", func(t *testing.T) {
			payload := []byte("legacy-corruption")
			chunkID := seedVerifyLegacyBlockFixture(t, backend.DB, containersDir, payload)
			logicalID := seedVerifyFileRecipe(t, backend.DB, "legacy-corruption.bin", [][]byte{payload}, []int64{chunkID})
			corruptPlacementByte(t, backend.DB, containersDir, "blocks", "block_offset", chunkID)
			assertVerifyFileDeepFails(t, backend.DB, logicalID, containersDir, "chunk hash verification failed")
		})

		t.Run("packed-stored-block-corruption", func(t *testing.T) {
			payload := []byte("packed-block-corruption")
			_, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{payload}, nil)
			logicalID := seedVerifyFileRecipe(t, backend.DB, "packed-block-corruption.bin", [][]byte{payload}, chunkIDs)
			corruptPlacementByte(t, backend.DB, containersDir, "storage_blocks", "container_offset", chunkIDs[0])
			assertVerifyFileDeepFails(t, backend.DB, logicalID, containersDir, "chunk hash verification failed")
		})

		t.Run("mixed-packed-subset-corruption", func(t *testing.T) {
			legacyPayload := []byte("healthy-legacy")
			packedPayload := []byte("corrupt-packed")
			legacyID := seedVerifyLegacyBlockFixture(t, backend.DB, containersDir, legacyPayload)
			_, packedIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, [][]byte{packedPayload}, nil)
			logicalID := seedVerifyFileRecipe(t, backend.DB, "mixed-corruption.bin", [][]byte{legacyPayload, packedPayload}, []int64{legacyID, packedIDs[0]})
			if _, err := backend.DB.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("f", 64), packedIDs[0]); err != nil {
				t.Fatal(err)
			}
			assertVerifyFileDeepFails(t, backend.DB, logicalID, containersDir, "chunk hash verification failed")
		})
	})
}

func TestVerifyFileDeepUsesPackedMigrationCompanionAndReportsTruthfulCounts(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		containersDir := t.TempDir()
		payloads := [][]byte{[]byte("companion-packed-one"), []byte("companion-packed-two")}
		_, chunkIDs := seedVerifyPackedBlockFixture(t, backend.DB, containersDir, payloads, nil)
		var containerID, containerOffset, storedSize int64
		if err := backend.DB.QueryRow(`SELECT container_id, container_offset, stored_size FROM storage_blocks WHERE id = (SELECT block_id FROM chunk_block_refs WHERE chunk_id = $1)`, chunkIDs[0]).Scan(&containerID, &containerOffset, &storedSize); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.DB.Exec(`
			INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
			VALUES ($1, 'plain', 1, $2, $3, NULL, $4, $5)`, chunkIDs[0], int64(len(payloads[0])), storedSize, containerID, containerOffset); err != nil {
			t.Fatal(err)
		}
		logicalID := seedVerifyFileRecipe(t, backend.DB, "companion.bin", payloads, chunkIDs)

		var output bytes.Buffer
		original := log.Writer()
		log.SetOutput(&output)
		defer log.SetOutput(original)
		if err := VerifyFileDeepWithContainersDir(backend.DB, int(logicalID), containersDir); err != nil {
			t.Fatalf("packed companion file-deep verification: %v", err)
		}
		if !strings.Contains(output.String(), "attempted placements=2 failed=0") {
			t.Fatalf("truthful placement count missing from log: %s", output.String())
		}
	})
}

func assertVerifyFileDeepFails(t *testing.T, dbconn *sql.DB, logicalID int64, containersDir, contains string) {
	t.Helper()
	err := VerifyFileDeepWithContainersDir(dbconn, int(logicalID), containersDir)
	if err == nil || !strings.Contains(err.Error(), contains) {
		t.Fatalf("VerifyFileDeep error=%v, want substring %q", err, contains)
	}
}

func corruptPlacementByte(t *testing.T, dbconn *sql.DB, containersDir, table, offsetColumn string, chunkID int64) {
	t.Helper()
	query := `SELECT c.filename, p.` + offsetColumn + `
		FROM ` + table + ` p JOIN container c ON c.id = p.container_id`
	if table == "storage_blocks" {
		query += ` JOIN chunk_block_refs r ON r.block_id = p.id WHERE r.chunk_id = $1`
	} else {
		query += ` WHERE p.chunk_id = $1`
	}
	var filename string
	var offset int64
	if err := dbconn.QueryRow(query, chunkID).Scan(&filename, &offset); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(containersDir, filename)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	var value [1]byte
	if _, err := file.ReadAt(value[:], offset); err != nil {
		t.Fatal(err)
	}
	value[0] ^= 0xff
	if _, err := file.WriteAt(value[:], offset); err != nil {
		t.Fatal(err)
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
