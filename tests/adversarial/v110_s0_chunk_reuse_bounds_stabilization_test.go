package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// v1.10-S0 — Chunk Reuse Bounds Stabilization Harness
//
// Goal:
//
//	Make intermittent chunk-reuse/container-bounds instability deterministic,
//	observable, and permanently guarded before engine extraction.
//
// Targeted paths:
//   - reclaim path around store.go:481
//   - bounds validation around store.go:1187
//   - reuse validation around store.go:1961
func TestV110S0ChunkReuseBoundsStabilizationHarness(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range []blocks.Codec{blocks.CodecPlain, blocks.CodecAESGCM} {
		codec := codec
		t.Run(string(codec), func(t *testing.T) {
			dbconn, tmp, writer := setupV110S0Env(t, codec)
			defer dbconn.Close()

			setCompressionV110S0(t, dbconn, storagecompression.CompressionZstd)
			sgctx := storage.StorageContext{DB: dbconn, Writer: writer}

			rng := rand.New(rand.NewSource(11000))
			pathsDir := filepath.Join(tmp, "inputs")
			if err := os.MkdirAll(pathsDir, 0o755); err != nil {
				t.Fatalf("mkdir input dir: %v", err)
			}
			runToken := fmt.Sprintf("%d", time.Now().UnixNano())

			storedIDs := make([]int64, 0, 256)
			snapshotIDs := make([]string, 0, 32)

			const (
				rounds          = 90
				deepVerifyEvery = 6
			)
			for round := 0; round < rounds; round++ {
				payload := payloadV110S0(round)
				inputPath := filepath.Join(pathsDir, fmt.Sprintf("round-%03d.bin", round))
				if err := os.WriteFile(inputPath, payload, 0o644); err != nil {
					writeFailureArtifactV110S0(t, dbconn, codec, round, "write_input", err)
					t.Fatalf("write input file round=%d: %v", round, err)
				}

				result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inputPath, codec)
				if err != nil {
					writeFailureArtifactV110S0(t, dbconn, codec, round, "store_primary", err)
					t.Fatalf("store primary round=%d: %v", round, err)
				}
				storedIDs = append(storedIDs, result.FileID)

				// Deterministic duplicate writes force chunk reuse paths.
				if round%2 == 0 {
					dupPath := filepath.Join(pathsDir, fmt.Sprintf("dup-%03d.bin", round))
					if err := os.WriteFile(dupPath, payload, 0o644); err != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "write_dup", err)
						t.Fatalf("write dup file round=%d: %v", round, err)
					}
					dupResult, dupErr := storage.StoreFileWithStorageContextAndCodecResult(sgctx, dupPath, codec)
					if dupErr != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "store_dup", dupErr)
						t.Fatalf("store dup round=%d: %v", round, dupErr)
					}
					storedIDs = append(storedIDs, dupResult.FileID)
				}

				// Simulate full-suite-like lifecycle churn to stress reclaim/reuse ordering.
				if round%7 == 6 && len(storedIDs) > 8 {
					victim := storedIDs[rng.Intn(len(storedIDs)-4)]
					if remErr := storage.RemoveFileWithDB(dbconn, victim); remErr != nil {
						if strings.Contains(remErr.Error(), "retained by one or more snapshots") {
							t.Logf("round=%d file_id=%d remove skipped: %v", round, victim, remErr)
						} else {
							writeFailureArtifactV110S0(t, dbconn, codec, round, "remove_file", remErr)
							t.Fatalf("remove file round=%d file_id=%d: %v", round, victim, remErr)
						}
					}
				}

				if round%9 == 8 {
					if _, gcErr := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); gcErr != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "gc_dry_run", gcErr)
						t.Fatalf("gc dry-run round=%d: %v", round, gcErr)
					}
					if _, gcErr := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); gcErr != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "gc_real", gcErr)
						t.Fatalf("gc real round=%d: %v", round, gcErr)
					}
				}

				if round%11 == 10 {
					snapID := fmt.Sprintf("v110-s0-%s-%s-%03d", strings.ToLower(string(codec)), runToken, round)
					if err := snapshot.CreateSnapshotWithOptions(context.Background(), dbconn, snapshot.SnapshotCreateOptions{
						ID:            snapID,
						Type:          "full",
						SelectionBase: pathsDir,
					}); err != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "snapshot_create", err)
						t.Fatalf("snapshot create round=%d: %v", round, err)
					}
					snapshotIDs = append(snapshotIDs, snapID)
					if len(snapshotIDs) > 4 {
						oldest := snapshotIDs[0]
						snapshotIDs = snapshotIDs[1:]
						if err := snapshot.DeleteSnapshot(context.Background(), dbconn, oldest); err != nil {
							writeFailureArtifactV110S0(t, dbconn, codec, round, "snapshot_delete", err)
							t.Fatalf("snapshot delete round=%d id=%s: %v", round, oldest, err)
						}
					}
				}

				// Deterministically inject and repair container-size drift to exercise
				// reuse validation + reclaim transitions under contaminated state.
				if round%13 == 12 {
					injected, injectErr := injectContainerSizeDriftV110S0(dbconn)
					if injectErr != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "inject_drift", injectErr)
						t.Fatalf("inject drift round=%d: %v", round, injectErr)
					}
					if injected {
						if repairErr := repairContainerCurrentSizeFromDiskV110S0(dbconn); repairErr != nil {
							writeFailureArtifactV110S0(t, dbconn, codec, round, "repair_drift", repairErr)
							t.Fatalf("repair drift round=%d: %v", round, repairErr)
						}
					}
				}

				// Keep the same lifecycle churn, but sample deep verification at a
				// fixed cadence plus the final round so the long-run race package
				// stays within the required default test timeout.
				if round%deepVerifyEvery == deepVerifyEvery-1 || round == rounds-1 {
					if verifyErr := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); verifyErr != nil {
						writeFailureArtifactV110S0(t, dbconn, codec, round, "verify_deep", verifyErr)
						t.Fatalf("verify deep round=%d: %v", round, verifyErr)
					}
				}
			}

			var rebuiltCount int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE retry_count > 0`).Scan(&rebuiltCount); err != nil {
				t.Fatalf("count rebuilt chunks: %v", err)
			}
			t.Logf("v1.10-s0 harness complete codec=%s rounds=%d rebuilt_chunks=%d", codec, rounds, rebuiltCount)
		})
	}
}

func setupV110S0Env(t *testing.T, codec blocks.Codec) (*sql.DB, string, container.ContainerWriter) {
	t.Helper()

	tmp := t.TempDir()
	origContainers := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainers })

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers: %v", err)
	}
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	if codec == blocks.CodecAESGCM {
		testutils.SetTestAESGCMKey(t)
	} else {
		t.Setenv("COLDKEEP_KEY", "")
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	writer := container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn)
	return dbconn, tmp, writer
}

func setCompressionV110S0(t *testing.T, dbconn *sql.DB, codec string) {
	t.Helper()
	t.Setenv("COLDKEEP_COMPRESSION", codec)
	t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin compression tx: %v", err)
	}
	if err := storage.SetDefaultCompression(tx, codec); err != nil {
		_ = tx.Rollback()
		t.Fatalf("set default compression: %v", err)
	}
	if err := storage.SetDefaultCompressionLevel(tx, 3); err != nil {
		_ = tx.Rollback()
		t.Fatalf("set default compression level: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit compression tx: %v", err)
	}
}

func payloadV110S0(round int) []byte {
	// Deterministic mixed corpus: repeat-heavy + pseudo-random-like content.
	size := 160*1024 + (round%17)*23*1024
	data := make([]byte, size)
	for i := range data {
		if round%3 == 0 {
			data[i] = byte('A' + (i % 11))
		} else {
			data[i] = byte((i*37 + round*19) % 251)
		}
	}
	return data
}

func injectContainerSizeDriftV110S0(dbconn *sql.DB) (bool, error) {
	var (
		containerID int64
		offset      int64
		storedSize  int64
	)
	err := dbconn.QueryRow(`
		SELECT sb.container_id, sb.container_offset, sb.stored_size
		FROM storage_blocks sb
		ORDER BY sb.id DESC
		LIMIT 1
	`).Scan(&containerID, &offset, &storedSize)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("load latest storage block for drift injection: %w", err)
	}

	newSize := offset + storedSize - 1
	if newSize < int64(container.ContainerHdrLen) {
		return false, nil
	}

	result, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, newSize, containerID)
	if err != nil {
		return false, fmt.Errorf("inject container size drift container_id=%d: %w", containerID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected while injecting container size drift: %w", err)
	}
	return rows == 1, nil
}

func repairContainerCurrentSizeFromDiskV110S0(dbconn *sql.DB) error {
	rows, err := dbconn.Query(`SELECT id, filename FROM container WHERE quarantine = FALSE`)
	if err != nil {
		return fmt.Errorf("query containers for current_size repair: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var containerID int64
		var filename string
		if err := rows.Scan(&containerID, &filename); err != nil {
			return fmt.Errorf("scan container row for repair: %w", err)
		}
		fullPath := filepath.Join(container.ContainersDir, filename)
		info, statErr := os.Stat(fullPath)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return fmt.Errorf("stat container file %s: %w", fullPath, statErr)
		}
		if _, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, info.Size(), containerID); err != nil {
			return fmt.Errorf("repair container current_size container_id=%d: %w", containerID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate containers for repair: %w", err)
	}
	return nil
}

type placementV110S0 struct {
	ContainerID     int64  `json:"container_id"`
	ContainerFile   string `json:"container_filename"`
	ContainerSize   int64  `json:"container_size"`
	MaxSize         int64  `json:"container_max_size"`
	BlockID         int64  `json:"block_id"`
	ChunkID         int64  `json:"chunk_id"`
	ChunkStatus     string `json:"chunk_status"`
	RetryCount      int64  `json:"retry_count"`
	ChunkSize       int64  `json:"chunk_size"`
	BlockOffset     int64  `json:"block_offset"`
	StoredSize      int64  `json:"stored_size"`
	PlaintextSize   int64  `json:"plaintext_size"`
	LiveRefCount    int64  `json:"live_ref_count"`
	ContainerOnDisk int64  `json:"container_file_size"`
}

type chunkRefV110S0 struct {
	LogicalFileID int64 `json:"logical_file_id"`
	ChunkID       int64 `json:"chunk_id"`
	ChunkOrder    int64 `json:"chunk_order"`
}

type snapshotRootV110S0 struct {
	LogicalFileID int64  `json:"logical_file_id"`
	SnapshotRefs  int64  `json:"snapshot_refs"`
	SnapshotIDs   string `json:"snapshot_ids"`
}

type failureArtifactV110S0 struct {
	CreatedAtUTC  string               `json:"created_at_utc"`
	Codec         string               `json:"codec"`
	Round         int                  `json:"round"`
	Operation     string               `json:"operation"`
	Error         string               `json:"error"`
	Placements    []placementV110S0    `json:"placements"`
	ChunkRefs     []chunkRefV110S0     `json:"chunk_refs"`
	SnapshotRoots []snapshotRootV110S0 `json:"snapshot_roots"`
	ArtifactPath  string               `json:"artifact_path"`
}

func writeFailureArtifactV110S0(t *testing.T, dbconn *sql.DB, codec blocks.Codec, round int, operation string, failure error) {
	t.Helper()

	artifact := failureArtifactV110S0{
		CreatedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Codec:        string(codec),
		Round:        round,
		Operation:    operation,
		Error:        failure.Error(),
	}

	artifact.Placements = collectPlacementsV110S0(t, dbconn)
	artifact.ChunkRefs = collectChunkRefsV110S0(t, dbconn)
	artifact.SnapshotRoots = collectSnapshotRootsV110S0(t, dbconn)

	artifactDir := os.Getenv("COLDKEEP_STABILIZATION_ARTIFACT_DIR")
	if strings.TrimSpace(artifactDir) == "" {
		artifactDir = filepath.Join(os.TempDir(), "coldkeep-v110-artifacts")
	}
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Logf("v110-s0 artifact mkdir failed: %v", err)
		return
	}

	fileName := fmt.Sprintf("v110-s0-chunk-reuse-%s-round-%03d-%d.json", strings.ToLower(string(codec)), round, time.Now().UnixNano())
	path := filepath.Join(artifactDir, fileName)
	artifact.ArtifactPath = path

	encoded, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Logf("v110-s0 artifact marshal failed: %v", err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Logf("v110-s0 artifact write failed: %v", err)
		return
	}
	t.Logf("v110-s0 first-failure artifact: %s", path)
}

func collectPlacementsV110S0(t *testing.T, dbconn *sql.DB) []placementV110S0 {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT
			ctr.id,
			ctr.filename,
			ctr.current_size,
			ctr.max_size,
			sb.id,
			c.id,
			c.status,
			c.retry_count,
			c.size,
			sb.container_offset,
			sb.stored_size,
			sb.plaintext_size,
			c.live_ref_count
		FROM storage_blocks sb
		JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
		JOIN chunk c ON c.id = cbr.chunk_id
		JOIN container ctr ON ctr.id = sb.container_id
		ORDER BY sb.id DESC
		LIMIT 128
	`)
	if err != nil {
		t.Logf("collect placements query failed: %v", err)
		return nil
	}
	defer func() { _ = rows.Close() }()

	collected := make([]placementV110S0, 0, 128)
	for rows.Next() {
		var p placementV110S0
		if err := rows.Scan(
			&p.ContainerID,
			&p.ContainerFile,
			&p.ContainerSize,
			&p.MaxSize,
			&p.BlockID,
			&p.ChunkID,
			&p.ChunkStatus,
			&p.RetryCount,
			&p.ChunkSize,
			&p.BlockOffset,
			&p.StoredSize,
			&p.PlaintextSize,
			&p.LiveRefCount,
		); err != nil {
			t.Logf("collect placements scan failed: %v", err)
			continue
		}
		if info, statErr := os.Stat(filepath.Join(container.ContainersDir, p.ContainerFile)); statErr == nil {
			p.ContainerOnDisk = info.Size()
		}
		collected = append(collected, p)
	}
	sort.Slice(collected, func(i, j int) bool {
		if collected[i].ContainerID == collected[j].ContainerID {
			return collected[i].BlockOffset < collected[j].BlockOffset
		}
		return collected[i].ContainerID < collected[j].ContainerID
	})
	return collected
}

func collectChunkRefsV110S0(t *testing.T, dbconn *sql.DB) []chunkRefV110S0 {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT logical_file_id, chunk_id, chunk_order
		FROM file_chunk
		ORDER BY logical_file_id DESC, chunk_order DESC
		LIMIT 256
	`)
	if err != nil {
		t.Logf("collect chunk refs query failed: %v", err)
		return nil
	}
	defer func() { _ = rows.Close() }()

	collected := make([]chunkRefV110S0, 0, 256)
	for rows.Next() {
		var r chunkRefV110S0
		if err := rows.Scan(&r.LogicalFileID, &r.ChunkID, &r.ChunkOrder); err != nil {
			t.Logf("collect chunk refs scan failed: %v", err)
			continue
		}
		collected = append(collected, r)
	}
	return collected
}

func collectSnapshotRootsV110S0(t *testing.T, dbconn *sql.DB) []snapshotRootV110S0 {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT
			sf.logical_file_id,
			COUNT(*) AS snapshot_refs,
			STRING_AGG(DISTINCT sf.snapshot_id, ',' ORDER BY sf.snapshot_id) AS snapshot_ids
		FROM snapshot_file sf
		GROUP BY sf.logical_file_id
		ORDER BY snapshot_refs DESC, sf.logical_file_id ASC
		LIMIT 128
	`)
	if err != nil {
		t.Logf("collect snapshot roots query failed: %v", err)
		return nil
	}
	defer func() { _ = rows.Close() }()

	collected := make([]snapshotRootV110S0, 0, 128)
	for rows.Next() {
		var r snapshotRootV110S0
		if err := rows.Scan(&r.LogicalFileID, &r.SnapshotRefs, &r.SnapshotIDs); err != nil {
			t.Logf("collect snapshot roots scan failed: %v", err)
			continue
		}
		collected = append(collected, r)
	}
	return collected
}
