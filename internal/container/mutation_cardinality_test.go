package container

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
)

func TestRequiredContainerMutationsFailClosedOnMissingRows(t *testing.T) {
	dbconn := setupContainerOpsTestDB(t)
	defer func() { _ = dbconn.Close() }()

	t.Run("update-size", func(t *testing.T) {
		err := UpdateContainerSize(dbconn, 404, ContainerHdrLen)
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})

	t.Run("simulated-seal", func(t *testing.T) {
		err := NewSimulatedWriter(ContainerHdrLen+128).SealContainer(dbconn, 404, "", "")
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})
}

func TestSealContainerFailsClosedWhenUpdateMatchesZero(t *testing.T) {
	dbconn := setupContainerOpsTestDB(t)
	defer func() { _ = dbconn.Close() }()

	dir := t.TempDir()
	filename := "phase17-seal.bin"
	maxSize := int64(ContainerHdrLen + 128)
	createTestContainerFile(t, filepath.Join(dir, filename), maxSize)

	result, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		 VALUES (?, ?, ?, FALSE, TRUE, FALSE)`,
		filename,
		int64(ContainerHdrLen),
		maxSize,
	)
	if err != nil {
		t.Fatalf("insert container row: %v", err)
	}
	containerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("container id: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TRIGGER phase17_ignore_container_seal
		BEFORE UPDATE OF sealed ON container
		BEGIN
			SELECT RAISE(IGNORE);
		END
	`); err != nil {
		t.Fatalf("create ignored-seal trigger: %v", err)
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin seal transaction: %v", err)
	}
	err = SealContainerInDir(tx, containerID, filename, dir)
	if !errors.Is(err, db.ErrMutationCardinality) {
		_ = tx.Rollback()
		t.Fatalf("error=%v, want ErrMutationCardinality", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback seal transaction: %v", err)
	}

	var sealed, sealing bool
	if err := dbconn.QueryRow(`SELECT sealed, sealing FROM container WHERE id = ?`, containerID).Scan(&sealed, &sealing); err != nil {
		t.Fatalf("read container state: %v", err)
	}
	if sealed || !sealing {
		t.Fatalf("unexpected state after rollback: sealed=%t sealing=%t", sealed, sealing)
	}
}

func TestLocalWriterRotationFailsClosedWhenSealingMarkerMatchesZero(t *testing.T) {
	const containerID = int64(1)
	const maxSize = int64(ContainerHdrLen + 12)
	dbconn := openContainerTestDB(t, containerID, ContainerHdrLen+10, maxSize)
	if _, err := dbconn.Exec(`
		CREATE TRIGGER phase17_ignore_rotation_sealing
		BEFORE UPDATE OF sealing ON container
		WHEN OLD.id = 1
		BEGIN
			SELECT RAISE(IGNORE);
		END
	`); err != nil {
		t.Fatalf("create ignored-rotation trigger: %v", err)
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin rotation transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	handle := &fakeContainer{size: ContainerHdrLen + 10}
	w := NewLocalWriterWithDirAndDB(t.TempDir(), maxSize, dbconn)
	w.hasActive = true
	w.activeID = containerID
	w.activeFile = "c.bin"
	w.activeHandle = handle
	w.activeSize = ContainerHdrLen + 10

	_, err = w.AppendPayload(tx, []byte("abc"))
	if !errors.Is(err, db.ErrMutationCardinality) {
		t.Fatalf("error=%v, want ErrMutationCardinality", err)
	}
	if got := handle.Size(); got != ContainerHdrLen+10 {
		t.Fatalf("rotation wrote bytes after missed sealing marker: size=%d", got)
	}
}
