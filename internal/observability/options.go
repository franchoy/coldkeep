package observability

type StatsOptions struct {
	IncludeContainers bool
}

const (
	DefaultInspectLimit = 100
	MaxInspectLimit     = 1000
)

type InspectOptions struct {
	Deep      bool
	Relations bool
	Reverse   bool
	Limit     int
}

func normalizeInspectOptions(opts InspectOptions) InspectOptions {
	if opts.Deep {
		// Deep traversal implies forward relation expansion.
		opts.Relations = true
	}

	if opts.Limit <= 0 {
		opts.Limit = DefaultInspectLimit
	}
	if opts.Limit > MaxInspectLimit {
		opts.Limit = MaxInspectLimit
	}

	return opts
}

type SimulationOptions struct {
	Kind string
}

type InspectTarget struct {
	EntityType EntityType
	EntityID   string
}

type SimulationTarget struct {
	Kind     string
	Metadata map[string]any
}
