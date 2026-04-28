package maintenance_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
)

func requireParityDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run DB-backed parity tests")
	}
}

func applyParitySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}
	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
}

func resetParityDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	if _, err := dbconn.Exec(`
		TRUNCATE TABLE
			snapshot_file,
			snapshot,
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func TestSimulateGCMatchesActualGCDeletion(t *testing.T) {
	requireParityDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applyParitySchema(t, dbconn)
	resetParityDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	payload := []byte("gc parity payload")
	filename := "gc-parity.bin"
	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, payload, 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		filename,
		int64(len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		 RETURNING id`,
		"gc-parity-chunk",
		int64(len(payload)),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, 0)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		containerID,
	); err != nil {
		t.Fatalf("insert block row: %v", err)
	}

	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("new observability service: %v", err)
	}

	simulated, err := svc.Simulate(context.Background(), observability.SimulationOptions{Kind: observability.SimulationKindGC})
	if err != nil {
		t.Fatalf("simulate gc: %v", err)
	}
	if simulated == nil || simulated.GC == nil {
		t.Fatal("expected gc simulation result")
	}
	if simulated.GC.Summary.FullyReclaimableContainers != 1 {
		t.Fatalf("expected one fully reclaimable container, got %d", simulated.GC.Summary.FullyReclaimableContainers)
	}
	if len(simulated.GC.Containers) != 1 {
		t.Fatalf("expected one simulated container, got %d", len(simulated.GC.Containers))
	}
	if simulated.GC.Containers[0].Filename != filename {
		t.Fatalf("simulated filename = %q, want %q", simulated.GC.Containers[0].Filename, filename)
	}

	gcResult, err := maintenance.RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("run actual gc: %v", err)
	}
	if gcResult.AffectedContainers != int(simulated.GC.Summary.FullyReclaimableContainers) {
		t.Fatalf("affected containers = %d, want %d", gcResult.AffectedContainers, simulated.GC.Summary.FullyReclaimableContainers)
	}
	if len(gcResult.ContainerFilenames) != 1 || gcResult.ContainerFilenames[0] != filename {
		t.Fatalf("actual gc filenames = %v, want [%s]", gcResult.ContainerFilenames, filename)
	}
	if _, err := os.Stat(containerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected container file to be removed, stat err=%v", err)
	}

	var remainingContainers int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&remainingContainers); err != nil {
		t.Fatalf("count remaining containers: %v", err)
	}
	if remainingContainers != 0 {
		t.Fatalf("expected container metadata to be deleted, got %d rows", remainingContainers)
	}
}
