package container

import (
	"database/sql"
	"strings"
	"testing"

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
