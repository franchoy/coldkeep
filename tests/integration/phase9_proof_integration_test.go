package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// TestPhase9PackedFileDeepVerificationBackendProof supplies the frozen
// PostgreSQL integration proof for P3-COR-003. The lower-level verify package
// owns the exhaustive dual-backend placement matrix; this route proves that
// production packed storage and the integration database exercise that code.
func TestPhase9PackedFileDeepVerificationBackendProof(t *testing.T) {
	testgate.RequireDB(t)

	t.Run("packed-companion-mixed-and-corrupt-packed-subset", func(t *testing.T) {
		dbconn, _, fileID := testutils.SetupStoredFileForVerification(t, "phase9-packed-mixed.bin", 4*1024*1024)
		defer dbconn.Close()

		var packedRefs, legacyCompanions int
		if err := dbconn.QueryRow(`
			SELECT COUNT(*), COUNT(b.id)
			FROM file_chunk fc
			JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
			LEFT JOIN blocks b ON b.chunk_id = fc.chunk_id
			WHERE fc.logical_file_id = $1`, fileID).Scan(&packedRefs, &legacyCompanions); err != nil {
			t.Fatalf("count packed and migration-companion placements: %v", err)
		}
		if packedRefs < 2 || legacyCompanions != packedRefs {
			t.Fatalf("fixture packed refs=%d legacy companions=%d, want at least two matching rows", packedRefs, legacyCompanions)
		}
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err != nil {
			t.Fatalf("healthy packed file-deep verification: %v", err)
		}

		var firstChunkID, secondChunkID int64
		rows, err := dbconn.Query(`
			SELECT fc.chunk_id
			FROM file_chunk fc
			JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
			WHERE fc.logical_file_id = $1
			ORDER BY fc.chunk_order
			LIMIT 2`, fileID)
		if err != nil {
			t.Fatalf("query packed recipe: %v", err)
		}
		if !rows.Next() {
			_ = rows.Close()
			t.Fatal("packed fixture did not expose a first ordered chunk")
		}
		if err := rows.Scan(&firstChunkID); err != nil {
			_ = rows.Close()
			t.Fatalf("scan first packed recipe chunk: %v", err)
		}
		if !rows.Next() {
			_ = rows.Close()
			t.Fatal("packed fixture did not expose a second ordered chunk")
		}
		if err := rows.Scan(&secondChunkID); err != nil {
			_ = rows.Close()
			t.Fatalf("scan second packed recipe chunk: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close packed recipe rows: %v", err)
		}

		if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, firstChunkID); err != nil {
			t.Fatalf("convert first chunk to legacy placement: %v", err)
		}
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err != nil {
			t.Fatalf("healthy mixed legacy/packed file-deep verification: %v", err)
		}

		if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("0", 64), secondChunkID); err != nil {
			t.Fatalf("corrupt packed subset chunk hash: %v", err)
		}
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err == nil {
			t.Fatal("mixed recipe with corrupt packed subset passed deep verification")
		}
	})

	t.Run("missing-placement-fails-closed", func(t *testing.T) {
		dbconn, _, fileID := testutils.SetupStoredFileForVerification(t, "phase9-packed-missing.bin", 512*1024)
		defer dbconn.Close()
		var chunkID int64
		if err := dbconn.QueryRow(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order LIMIT 1`, fileID).Scan(&chunkID); err != nil {
			t.Fatalf("query first recipe chunk: %v", err)
		}
		if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkID); err != nil {
			t.Fatalf("delete packed placement: %v", err)
		}
		if _, err := dbconn.Exec(`DELETE FROM blocks WHERE chunk_id = $1`, chunkID); err != nil {
			t.Fatalf("delete legacy companion: %v", err)
		}
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err == nil {
			t.Fatal("missing authoritative placement passed deep verification")
		}
	})

	t.Run("malformed-packed-range-fails-closed", func(t *testing.T) {
		dbconn, _, fileID := testutils.SetupStoredFileForVerification(t, "phase9-packed-range.bin", 512*1024)
		defer dbconn.Close()
		var chunkID int64
		if err := dbconn.QueryRow(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order LIMIT 1`, fileID).Scan(&chunkID); err != nil {
			t.Fatalf("query first recipe chunk: %v", err)
		}
		if _, err := dbconn.Exec(`UPDATE chunk_block_refs SET offset_in_block = offset_in_block + 1 WHERE chunk_id = $1`, chunkID); err != nil {
			t.Fatalf("corrupt packed segment range: %v", err)
		}
		if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "file", int(fileID), verify.VerifyDeep); err == nil {
			t.Fatal("malformed packed range passed deep verification")
		}
	})
}

// TestPhase9SimulateGCPostgresCoordinationIntegration proves that a real CLI
// process cannot enter PostgreSQL-backed simulate-gc application work while a
// cooperative process owns the native repository lease, and can do so after
// the same holder releases it.
func TestPhase9SimulateGCPostgresCoordinationIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := t.TempDir()
	originalContainersDir := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = originalContainersDir })
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("create repository storage: %v", err)
	}
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect fixture database: %v", err)
	}
	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close fixture database before CLI execution: %v", err)
	}

	repositoryPath := container.ContainersDir
	identity, err := coordination.ResolveIdentity(repositoryPath)
	if err != nil {
		t.Fatalf("resolve repository identity: %v", err)
	}
	owner, err := coordination.NewOwner(coordination.OperationStore, identity, "phase9-integration-holder", time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("construct repository owner: %v", err)
	}
	lease, err := coordination.NewCoordinator().Acquire(context.Background(), identity, coordination.Request{
		Operation: coordination.OperationStore,
		Mode:      coordination.ModeExclusive,
		Owner:     owner,
	})
	if err != nil {
		t.Fatalf("acquire holder lease: %v", err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = lease.Release()
		}
	})

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)
	env := testutils.DefaultCLIEnv(repositoryPath)
	busy := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "simulate", "gc", "--output", "json")
	if busy.ExitCode == 0 || !strings.Contains(strings.ToLower(busy.Stderr), "busy") {
		t.Fatalf("contended simulate gc exit=%d stdout=%q stderr=%q", busy.ExitCode, busy.Stdout, busy.Stderr)
	}

	if err := lease.Release(); err != nil {
		t.Fatalf("release holder lease: %v", err)
	}
	released = true
	uncontended := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "simulate", "gc", "--output", "json")
	if uncontended.ExitCode != 0 {
		t.Fatalf("uncontended simulate gc exit=%d stdout=%q stderr=%q", uncontended.ExitCode, uncontended.Stdout, uncontended.Stderr)
	}
	payload, ok := testutils.TryParseLastJSONLine(uncontended.Stdout)
	if !ok {
		t.Fatalf("uncontended simulate gc produced no JSON result: stdout=%q stderr=%q", uncontended.Stdout, uncontended.Stderr)
	}
	if got, _ := payload["type"].(string); got != "simulation" {
		t.Fatalf("uncontended simulate gc type=%q, want simulation: payload=%v", got, payload)
	}
	data := testutils.JSONMap(t, payload, "data")
	gcNode := testutils.JSONMap(t, data, "gc")
	if got, _ := gcNode["kind"].(string); got != "gc" {
		t.Fatalf("uncontended simulate gc kind=%q, want gc: payload=%v", got, payload)
	}
	if exact, _ := gcNode["exact"].(bool); !exact {
		t.Fatalf("uncontended simulate gc exact=%v, want true: payload=%v", gcNode["exact"], payload)
	}
	if mutated, _ := gcNode["mutated"].(bool); mutated {
		t.Fatalf("uncontended simulate gc mutated=%v, want false: payload=%v", gcNode["mutated"], payload)
	}
}
