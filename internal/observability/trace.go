package observability

import (
	"encoding/json"
	"fmt"
	"io"
)

type TraceSink interface {
	Event(event TraceEvent)
}

type TraceEvent struct {
	Step     string         `json:"step"`
	Entity   string         `json:"entity,omitempty"`
	EntityID string         `json:"entity_id,omitempty"`
	Message  string         `json:"message"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type NoopTraceSink struct{}

func (NoopTraceSink) Event(event TraceEvent) {}

type HumanTraceSink struct {
	W io.Writer
}

func (s HumanTraceSink) Event(e TraceEvent) {
	fmt.Fprintf(s.W, "TRACE %s", e.Step)

	if e.Entity != "" {
		fmt.Fprintf(s.W, " %s=%s", e.Entity, e.EntityID)
	}

	if e.Message != "" {
		fmt.Fprintf(s.W, " %s", e.Message)
	}

	fmt.Fprintln(s.W)
}

type JSONTraceSink struct {
	W   io.Writer
	enc *json.Encoder
}

func NewJSONTraceSink(w io.Writer) *JSONTraceSink {
	return &JSONTraceSink{
		W:   w,
		enc: json.NewEncoder(w),
	}
}

func (s *JSONTraceSink) Event(e TraceEvent) {
	_ = s.enc.Encode(e)
}

func emitTrace(trace TraceOptions, event TraceEvent) {
	if !trace.Enabled {
		return
	}
	if trace.Sink == nil {
		return
	}
	trace.Sink.Event(event)
}
