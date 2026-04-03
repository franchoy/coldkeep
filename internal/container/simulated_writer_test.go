package container

import (
	"database/sql"
	"strings"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func TestSimulatedWriterAppendPayloadFailsWhenPayloadIsEmpty(t *testing.T) {
	w := NewSimulatedWriter(ContainerHdrLen + 128)

	_, err := w.AppendPayload(nil, nil)
	if err == nil || !strings.Contains(err.Error(), "payload is empty") {
		t.Fatalf("expected empty-payload error contract, got: %v", err)
	}
}

func TestSimulatedWriterAppendPayloadFailsWhenPayloadIsTooLarge(t *testing.T) {
	// max payload = maxSize - header = 4 bytes
	w := NewSimulatedWriter(ContainerHdrLen + 4)
	payload := []byte("12345")

	_, err := w.AppendPayload(nil, payload)
	if err == nil || !strings.Contains(err.Error(), "payload too large") {
		t.Fatalf("expected payload-too-large error contract, got: %v", err)
	}
}

func TestSimulatedWriterAppendPayloadWrapsEnsureActiveFailure(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	// Intentionally do not run migrations so INSERT INTO container fails.
	w := NewSimulatedWriter(ContainerHdrLen + 64)
	_, err = w.AppendPayload(dbconn, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "create simulated container row") {
		t.Fatalf("expected wrapped ensureActive contract, got: %v", err)
	}
}

func TestSimulatedWriterAppendPayloadCreatesAndRotatesContainer(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Max payload per container is 3 bytes.
	w := NewSimulatedWriter(ContainerHdrLen + 3)

	first, err := w.AppendPayload(dbconn, []byte("abc"))
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if first.ContainerID <= 0 {
		t.Fatalf("expected positive container id, got %d", first.ContainerID)
	}
	if first.Offset != ContainerHdrLen {
		t.Fatalf("unexpected first offset: got %d want %d", first.Offset, ContainerHdrLen)
	}
	if first.StoredSize != 3 {
		t.Fatalf("unexpected first stored size: got %d want 3", first.StoredSize)
	}
	if first.NewContainerSize != ContainerHdrLen+3 {
		t.Fatalf("unexpected first new size: got %d want %d", first.NewContainerSize, ContainerHdrLen+3)
	}
	if first.Rotated {
		t.Fatalf("expected first append not to rotate")
	}
	if !first.Full {
		t.Fatalf("expected first append to mark container as full")
	}

	second, err := w.AppendPayload(dbconn, []byte("z"))
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if !second.Rotated {
		t.Fatalf("expected second append to rotate container")
	}
	if second.PreviousID != first.ContainerID {
		t.Fatalf("unexpected previous id: got %d want %d", second.PreviousID, first.ContainerID)
	}
	if second.PreviousSize != first.NewContainerSize {
		t.Fatalf("unexpected previous size: got %d want %d", second.PreviousSize, first.NewContainerSize)
	}
	if second.ContainerID == first.ContainerID {
		t.Fatalf("expected rotation to create a new container id")
	}
	if second.Offset != ContainerHdrLen {
		t.Fatalf("unexpected second offset: got %d want %d", second.Offset, ContainerHdrLen)
	}

	var containerCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containerCount); err != nil {
		t.Fatalf("count containers: %v", err)
	}
	if containerCount != 2 {
		t.Fatalf("expected two container rows after rotation, got %d", containerCount)
	}
}
