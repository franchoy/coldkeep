package graph

type TraceFunc func(TraceEvent)

type TraversalOptions struct {
	Trace TraceFunc
}

type TraceEvent struct {
	Step     string
	Node     NodeID
	Message  string
	Metadata map[string]any
}

func emitTrace(trace TraceFunc, event TraceEvent) {
	if trace == nil {
		return
	}
	trace(event)
}
