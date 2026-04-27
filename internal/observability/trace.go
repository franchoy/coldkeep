package observability

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
