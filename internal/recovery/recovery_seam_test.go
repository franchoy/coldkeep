package recovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/fsx"
)

// TestRecoverySeamDefaultFSPreservesScanBehavior verifies that the default
// OS-backed filesystem seam produces the expected result for an empty containers dir.
func TestRecoverySeamDefaultFSPreservesScanBehavior(t *testing.T) {
	t.Parallel()
	dbconn := openRecoveryTestDB(t)
	defer func() { _ = dbconn.Close() }()
	dir := t.TempDir()

	stats := &recoveryStats{}
	if err := quarantineOrphanContainersWithFS(dbconn, dir, stats, fsx.Default()); err != nil {
		t.Fatalf("quarantine orphan containers with default fs: %v", err)
	}
	if stats.quarantinedOrphan != 0 {
		t.Fatalf("expected 0 orphans in empty dir, got %d", stats.quarantinedOrphan)
	}
}

// TestRecoverySeamNoopFSMatchesDefaultBehavior verifies that wrapping the
// OS-backed filesystem with NoopFS produces the same orphan-scan result as
// the default write path.
func TestRecoverySeamNoopFSMatchesDefaultBehavior(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	orphanFile := filepath.Join(dir, "orphan.bin")
	if err := os.WriteFile(filepath.Clean(orphanFile), []byte("phase8-recovery-seam-noop"), 0600); err != nil {
		t.Fatalf("write orphan file: %v", err)
	}

	dbDefault := openRecoveryTestDB(t)
	defer func() { _ = dbDefault.Close() }()
	statsDefault := &recoveryStats{}
	if err := quarantineOrphanContainersWithFS(dbDefault, dir, statsDefault, fsx.Default()); err != nil {
		t.Fatalf("quarantine orphan containers with default fs: %v", err)
	}

	dbNoop := openRecoveryTestDB(t)
	defer func() { _ = dbNoop.Close() }()
	statsNoop := &recoveryStats{}
	if err := quarantineOrphanContainersWithFS(dbNoop, dir, statsNoop, fsx.NewNoop(fsx.Default())); err != nil {
		t.Fatalf("quarantine orphan containers with noop fs: %v", err)
	}

	if statsDefault.quarantinedOrphan != statsNoop.quarantinedOrphan {
		t.Fatalf("orphan count mismatch: default=%d noop=%d",
			statsDefault.quarantinedOrphan, statsNoop.quarantinedOrphan)
	}
	if statsDefault.quarantinedOrphan != 1 {
		t.Fatalf("expected 1 orphan, got %d", statsDefault.quarantinedOrphan)
	}
}

// TestRecoveryMissingContainerSeamPreservesStatBehavior verifies that the
// quarantine-missing path correctly skips containers whose files exist on disk,
// confirming that the FS Stat seam is invoked correctly through the default
// and noop wrappers.
func TestRecoveryMissingContainerSeamPreservesStatBehavior(t *testing.T) {
	t.Parallel()
	dbconn := openRecoveryTestDB(t)
	defer func() { _ = dbconn.Close() }()
	dir := t.TempDir()

	// Create a container file on disk so Stat returns non-NotExist.
	containerFile := filepath.Join(dir, "present-seam.bin")
	if err := os.WriteFile(containerFile, []byte("phase8-seam-stat"), 0600); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	// Insert a matching container row.
	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine) VALUES (?, ?, ?, FALSE, FALSE, FALSE)`,
		"present-seam.bin", int64(len("phase8-seam-stat")), 512,
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	stats := &recoveryStats{}
	if err := quarantineMissingContainersWithFS(dbconn, dir, stats, fsx.NewNoop(fsx.Default())); err != nil {
		t.Fatalf("quarantine missing containers with noop fs: %v", err)
	}
	// The file exists on disk — no quarantine should occur.
	if stats.quarantinedMissing != 0 {
		t.Fatalf("expected 0 containers quarantined (file present), got %d", stats.quarantinedMissing)
	}
	if stats.totalContainersChecked != 1 {
		t.Fatalf("expected 1 container checked, got %d", stats.totalContainersChecked)
	}
}
