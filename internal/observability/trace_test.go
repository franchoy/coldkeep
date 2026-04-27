package observability

import (
	"testing"
)

type testTraceSink struct {
	events []TraceEvent
}

func (s *testTraceSink) Event(event TraceEvent) {
	s.events = append(s.events, event)
}

func TestEmitTraceSanitizesSensitiveMetadataAndMessage(t *testing.T) {
	sink := &testTraceSink{}

	emitTrace(TraceOptions{Enabled: true, Sink: sink}, TraceEvent{
		Step:    "test.trace",
		Message: "reading from /home/user/private.txt",
		Metadata: map[string]any{
			"source_path":  "/workspaces/coldkeep/samples/hello.txt",
			"db_url":       "postgres://user:pass@localhost:5432/coldkeep",
			"env_value":    "DB_PASSWORD=supersecret",
			"key_material": "-----BEGIN PRIVATE KEY-----",
			"count":        int64(7),
		},
	})

	if len(sink.events) != 1 {
		t.Fatalf("expected one event, got %d", len(sink.events))
	}
	got := sink.events[0]

	if got.Message != "[redacted_path]" {
		t.Fatalf("expected redacted path in message, got %q", got.Message)
	}
	if got.Metadata["source_path"] != "[redacted]" {
		t.Fatalf("expected source_path redacted, got %v", got.Metadata["source_path"])
	}
	if got.Metadata["db_url"] != "[redacted]" {
		t.Fatalf("expected db_url redacted, got %v", got.Metadata["db_url"])
	}
	if got.Metadata["env_value"] != "[redacted]" {
		t.Fatalf("expected env_value redacted, got %v", got.Metadata["env_value"])
	}
	if got.Metadata["key_material"] != "[redacted]" {
		t.Fatalf("expected key_material redacted, got %v", got.Metadata["key_material"])
	}
	if got.Metadata["count"] != int64(7) {
		t.Fatalf("expected non-sensitive count to pass through, got %v", got.Metadata["count"])
	}
}

func TestEmitTraceDisabledSkipsEvent(t *testing.T) {
	sink := &testTraceSink{}
	emitTrace(TraceOptions{Enabled: false, Sink: sink}, TraceEvent{Step: "ignored", Message: "ignored"})
	if len(sink.events) != 0 {
		t.Fatalf("expected no events when disabled, got %d", len(sink.events))
	}
}

func TestTraceDisabledProducesNoEvents(t *testing.T) {
	TestEmitTraceDisabledSkipsEvent(t)
}
