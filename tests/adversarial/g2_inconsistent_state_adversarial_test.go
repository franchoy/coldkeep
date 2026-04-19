package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/recovery"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G2 — No exposure of partially written or inconsistent data
//
// Adversarial goals:
//   - incomplete or inconsistent lifecycle state must never be treated as healthy data
//   - recovery/doctor must converge recoverable state to a safe post-state
//   - unrelated healthy live data must keep restoring correctly after corrective actions
//
// Notes:
//   - This file runs a codec matrix for plain + aes-gcm so that corrective flows
//     are exercised under both payload modes.
//   - It stays on the Postgres-backed integration harness used by the current
//     tests/adversarial layout.

func adversarialG2Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG2Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG2Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
	t.Helper()

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	env := testutils.DefaultCLIEnv(container.ContainersDir)
	for k, v := range env {
		_ = os.Setenv(k, v)
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)

	return dbconn, env, repoRoot, binPath, tmp
}

func storeFileWithCodecCLIG2(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func restoreMustMatchHashG2(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func TestAdversarialG2DoctorAbortsInjectedProcessingRowsAndPreservesHealthyRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG2Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG2Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g2-anchor.bin", 512*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, codec, anchorPath)

			var danglingLogicalID int64
			if err := dbconn.QueryRow(`
				INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
				VALUES ($1, $2, $3, $4, $5) RETURNING id
			`, "g2_dangling_processing.bin", int64(4096),
				fmt.Sprintf("%064x", 1), filestate.LogicalFileProcessing, int64(0)).Scan(&danglingLogicalID); err != nil {
				t.Fatalf("inject dangling PROCESSING logical_file: %v", err)
			}

			var danglingChunkID int64
			if err := dbconn.QueryRow(`
				INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count)
				VALUES ($1, $2, $3, 0, 0, 0) RETURNING id
			`, "g2-processing-chunk", int64(2048), filestate.ChunkProcessing).Scan(&danglingChunkID); err != nil {
				t.Fatalf("inject dangling PROCESSING chunk: %v", err)
			}

			doctorPayload := testutils.AssertCLIJSONOK(
				t,
				testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json"),
				"doctor",
			)
			doctorData := testutils.JSONMap(t, doctorPayload, "data")
			recoveryData := testutils.JSONMap(t, doctorData, "recovery")

			if got := testutils.JSONInt64(t, recoveryData, "aborted_logical_files"); got < 1 {
				t.Fatalf("expected aborted_logical_files >= 1, got %d recovery=%v", got, recoveryData)
			}
			if got := testutils.JSONInt64(t, recoveryData, "aborted_chunks"); got < 1 {
				t.Fatalf("expected aborted_chunks >= 1, got %d recovery=%v", got, recoveryData)
			}
			if verifyStatus, _ := doctorData["verify_status"].(string); verifyStatus != "ok" {
				t.Fatalf("expected verify_status=ok after doctor recovery, got %q payload=%v", verifyStatus, doctorPayload)
			}

			var logicalStatus string
			if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, danglingLogicalID).Scan(&logicalStatus); err != nil {
				t.Fatalf("query dangling logical_file after doctor: %v", err)
			}
			if logicalStatus != filestate.LogicalFileAborted {
				t.Fatalf("expected dangling logical_file status ABORTED, got %q", logicalStatus)
			}

			var chunkStatus string
			if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, danglingChunkID).Scan(&chunkStatus); err != nil {
				t.Fatalf("query dangling chunk after doctor: %v", err)
			}
			if chunkStatus != filestate.ChunkAborted {
				t.Fatalf("expected dangling chunk status ABORTED, got %q", chunkStatus)
			}

			testutils.AssertNoProcessingRows(t, dbconn)

			anchorOut := filepath.Join(restoreDir, "g2-anchor.restored.bin")
			restoreMustMatchHashG2(t, dbconn, anchorID, anchorOut, anchorHash)
		})
	}
}

func TestAdversarialG2RecoveryQuarantinesMissingContainerRecordAndPreservesHealthyRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG2Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG2Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g2-missing-anchor.bin", 384*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, codec, anchorPath)

			const missingFilename = "g2_missing_container.bin"
			var missingContainerID int64
			if err := dbconn.QueryRow(`
				INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
				VALUES ($1, $2, $3, FALSE, FALSE) RETURNING id
			`, missingFilename, int64(1024), container.GetContainerMaxSize()).Scan(&missingContainerID); err != nil {
				t.Fatalf("insert missing container record: %v", err)
			}

			if _, err := os.Stat(filepath.Join(container.ContainersDir, missingFilename)); !os.IsNotExist(err) {
				t.Fatalf("expected seeded missing container file to be absent before recovery")
			}

			if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
				t.Fatalf("system recovery: %v", err)
			}

			var quarantined bool
			if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = $1`, missingContainerID).Scan(&quarantined); err != nil {
				t.Fatalf("query missing container quarantine after recovery: %v", err)
			}
			if !quarantined {
				t.Fatalf("expected missing container record to be quarantined after recovery")
			}

			testutils.AssertNoProcessingRows(t, dbconn)

			anchorOut := filepath.Join(restoreDir, "g2-missing-anchor.restored.bin")
			restoreMustMatchHashG2(t, dbconn, anchorID, anchorOut, anchorHash)
		})
	}
}

func TestAdversarialG2RepeatedDoctorConvergesOnRecoverableState(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG2Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG2Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g2-converge-anchor.bin", 256*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, codec, anchorPath)

			for round := 0; round < 3; round++ {
				var danglingLogicalID int64
				if err := dbconn.QueryRow(`
					INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
					VALUES ($1, $2, $3, $4, $5) RETURNING id
				`, fmt.Sprintf("g2_round_%02d_processing.bin", round),
					int64(2048+round),
					fmt.Sprintf("%064x", round+11),
					filestate.LogicalFileProcessing, int64(0)).Scan(&danglingLogicalID); err != nil {
					t.Fatalf("round %d inject dangling PROCESSING logical_file: %v", round, err)
				}

				firstDoctor := testutils.AssertCLIJSONOK(
					t,
					testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json"),
					"doctor",
				)
				firstData := testutils.JSONMap(t, firstDoctor, "data")
				firstRecovery := testutils.JSONMap(t, firstData, "recovery")

				if got := testutils.JSONInt64(t, firstRecovery, "aborted_logical_files"); got < 1 {
					t.Fatalf("round %d expected first doctor aborted_logical_files >= 1, got %d recovery=%v", round, got, firstRecovery)
				}
				if verifyStatus, _ := firstData["verify_status"].(string); verifyStatus != "ok" {
					t.Fatalf("round %d expected first doctor verify_status=ok, got %q payload=%v", round, verifyStatus, firstDoctor)
				}

				var logicalStatus string
				if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, danglingLogicalID).Scan(&logicalStatus); err != nil {
					t.Fatalf("round %d query dangling logical_file after doctor: %v", round, err)
				}
				if logicalStatus != filestate.LogicalFileAborted {
					t.Fatalf("round %d expected dangling logical_file ABORTED, got %q", round, logicalStatus)
				}

				secondDoctor := testutils.AssertCLIJSONOK(
					t,
					testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json"),
					"doctor",
				)
				secondData := testutils.JSONMap(t, secondDoctor, "data")
				secondRecovery := testutils.JSONMap(t, secondData, "recovery")

				if got := testutils.JSONInt64(t, secondRecovery, "aborted_logical_files"); got != 0 {
					t.Fatalf("round %d expected converged second doctor aborted_logical_files=0, got %d recovery=%v", round, got, secondRecovery)
				}
				if got := testutils.JSONInt64(t, secondRecovery, "aborted_chunks"); got != 0 {
					t.Fatalf("round %d expected converged second doctor aborted_chunks=0, got %d recovery=%v", round, got, secondRecovery)
				}
				if verifyStatus, _ := secondData["verify_status"].(string); verifyStatus != "ok" {
					t.Fatalf("round %d expected second doctor verify_status=ok, got %q payload=%v", round, verifyStatus, secondDoctor)
				}

				testutils.AssertNoProcessingRows(t, dbconn)

				anchorOut := filepath.Join(restoreDir, fmt.Sprintf("g2-converge-anchor-%02d.restored.bin", round))
				restoreMustMatchHashG2(t, dbconn, anchorID, anchorOut, anchorHash)
			}
		})
	}
}

func TestAdversarialG2VerifyRejectsCompletedChunkMissingBlockRow(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG2Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG2Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g2-missing-block-row.bin", 768*1024)
			fileID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, codec, inPath)

			record := testutils.FetchFirstFileChunkRecord(t, dbconn, fileID)
			if _, err := dbconn.Exec(`DELETE FROM blocks WHERE chunk_id = $1`, record.ChunkID); err != nil {
				t.Fatalf("delete blocks row for completed chunk: %v", err)
			}

			verifyRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
				"verify", "system", "--output", "json")
			if verifyRes.ExitCode == 0 {
				t.Fatalf("verify system should fail for completed chunk missing block row\nstdout:\n%s\nstderr:\n%s", verifyRes.Stdout, verifyRes.Stderr)
			}

			outPath := filepath.Join(restoreDir, "g2-missing-block-row.restored.bin")
			if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err == nil {
				t.Fatalf("restore unexpectedly succeeded for completed chunk missing block row")
			}
		})
	}
}

func TestAdversarialG2GhostByteSealingContainerQuarantinedAndHealthyRestorePreserved(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG2Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG2Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g2-ghost-anchor.bin", 512*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, codec, anchorPath)

			if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
				t.Fatalf("mkdir containers dir: %v", err)
			}

			const ghostFilename = "g2-ghost-sealing.bin"
			ghostContainerPath := filepath.Join(container.ContainersDir, ghostFilename)
			dbCurrentSize := int64(container.ContainerHdrLen + 128)
			ghostBytes := []byte("g2-adversarial-ghost-bytes")
			physicalSize := dbCurrentSize + int64(len(ghostBytes))

			if err := os.WriteFile(ghostContainerPath, make([]byte, physicalSize), 0o644); err != nil {
				t.Fatalf("write ghost-byte container file: %v", err)
			}

			var ghostContainerID int64
			if err := dbconn.QueryRow(`
				INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
				VALUES ($1, $2, $3, FALSE, TRUE, FALSE)
				RETURNING id
			`, ghostFilename, dbCurrentSize, container.GetContainerMaxSize()).Scan(&ghostContainerID); err != nil {
				t.Fatalf("insert ghost-byte sealing container: %v", err)
			}

			if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
				t.Fatalf("system recovery: %v", err)
			}

			var sealed, sealing, quarantine bool
			if err := dbconn.QueryRow(`SELECT sealed, sealing, quarantine FROM container WHERE id = $1`, ghostContainerID).Scan(&sealed, &sealing, &quarantine); err != nil {
				t.Fatalf("query ghost container state after recovery: %v", err)
			}
			if sealed {
				t.Fatalf("expected ghost-byte sealing container to remain unsealed after recovery")
			}
			if sealing {
				t.Fatalf("expected recovery to clear sealing marker on quarantined ghost-byte container")
			}
			if !quarantine {
				t.Fatalf("expected ghost-byte sealing container to be quarantined after recovery")
			}

			testutils.AssertNoProcessingRows(t, dbconn)

			anchorOut := filepath.Join(restoreDir, "g2-ghost-anchor.restored.bin")
			restoreMustMatchHashG2(t, dbconn, anchorID, anchorOut, anchorHash)

			// doctor CLI should remain healthy after quarantine
			doctorPayload := testutils.AssertCLIJSONOK(
				t,
				testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json"),
				"doctor",
			)
			doctorData := testutils.JSONMap(t, doctorPayload, "data")
			if verifyStatus, _ := doctorData["verify_status"].(string); verifyStatus != "ok" {
				t.Fatalf("expected verify_status=ok after ghost-byte recovery, got %q", verifyStatus)
			}
		})
	}
}

var _ = dbschema.PostgresSchema
