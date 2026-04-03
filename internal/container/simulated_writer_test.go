package container

import (
	"strings"
	"testing"
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
