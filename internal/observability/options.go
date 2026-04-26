package observability

type StatsOptions struct {
	IncludeContainers bool
}

type InspectOptions struct {
	Deep      bool
	Relations bool
	Reverse   bool
	Limit     int
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
