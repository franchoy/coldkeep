package recovery

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
	_ "github.com/mattn/go-sqlite3"
)

func openRecoveryFaultDB(t *testing.T) *sql.DB {
	t.Helper()
	return openRecoveryTestDB(t)
}

func TestRecoveryFaultFSQuarantineMissingStatFailureFailsClosed(t *testing.T) {
	t.Parallel()

	dbconn := openRecoveryFaultDB(t)
	defer func() { _ = dbconn.Close() }()

	dir := t.TempDir()
	filename := "recovery-missing-stat-fault.bin"
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte("present-on-disk"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine) VALUES (?, ?, ?, FALSE, FALSE, FALSE)`,
		filename, container.ContainerHdrLen, container.ContainerHdrLen+256,
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpStat, Err: faultfs.ErrFaultStat})
	stats := &recoveryStats{}
	err := quarantineMissingContainersWithFS(dbconn, dir, stats, faultfs.New(fsx.Default(), script))
	if !errors.Is(err, faultfs.ErrFaultStat) {
		t.Fatalf("quarantine missing error = %v, want ErrFaultStat", err)
	}
	if stats.totalContainersChecked != 1 {
		t.Fatalf("expected exactly one container checked, got %d", stats.totalContainersChecked)
	}
	if stats.quarantinedMissing != 0 {
		t.Fatalf("expected no quarantined containers, got %d", stats.quarantinedMissing)
	}
	var quarantine int
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE filename = ?`, filename).Scan(&quarantine); err != nil {
		t.Fatalf("query container row: %v", err)
	}
	if quarantine != 0 {
		t.Fatalf("expected row to remain unquarantined, got %d", quarantine)
	}
}

func TestRecoveryFaultFSQuarantineOrphanReadDirFailureFailsClosed(t *testing.T) {
	t.Parallel()

	dbconn := openRecoveryFaultDB(t)
	defer func() { _ = dbconn.Close() }()

	dir := t.TempDir()
	readDirErr := errors.New("read dir fault")
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpReadDir, Err: readDirErr})
	stats := &recoveryStats{}
	err := quarantineOrphanContainersWithFS(dbconn, dir, stats, faultfs.New(fsx.Default(), script))
	if !errors.Is(err, readDirErr) {
		t.Fatalf("quarantine orphan error = %v, want read dir fault", err)
	}
	if stats.quarantinedOrphan != 0 {
		t.Fatalf("expected no orphan containers quarantined, got %d", stats.quarantinedOrphan)
	}
}

func TestRecoveryFaultFSQuarantineCorruptActiveTailStatFailureFailsClosed(t *testing.T) {
	t.Parallel()

	dbconn := openRecoveryFaultDB(t)
	defer func() { _ = dbconn.Close() }()

	dir := t.TempDir()
	filename := "recovery-active-tail-stat-fault.bin"
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte("active-container-bytes"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine) VALUES (?, ?, ?, FALSE, FALSE, FALSE)`,
		filename, container.ContainerHdrLen, container.ContainerHdrLen+256,
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpStat, Err: faultfs.ErrFaultStat})
	stats := &recoveryStats{}
	err := quarantineCorruptActiveContainerTailsWithFS(dbconn, dir, stats, faultfs.New(fsx.Default(), script))
	if !errors.Is(err, faultfs.ErrFaultStat) {
		t.Fatalf("quarantine active tail error = %v, want ErrFaultStat", err)
	}
	if stats.quarantinedCorruptTail != 0 {
		t.Fatalf("expected no quarantined corrupt tails, got %d", stats.quarantinedCorruptTail)
	}
	var quarantine int
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE filename = ?`, filename).Scan(&quarantine); err != nil {
		t.Fatalf("query container row: %v", err)
	}
	if quarantine != 0 {
		t.Fatalf("expected row to remain unquarantined, got %d", quarantine)
	}
}
