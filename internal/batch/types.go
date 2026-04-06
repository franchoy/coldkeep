package batch

// OperationType identifies the command-level operation executed in a batch.
type OperationType string

const (
	OperationRestore OperationType = "restore"
	OperationRemove  OperationType = "remove"
)

// TargetSource identifies where a target originated.
type TargetSource string

const (
	TargetFromArgs    TargetSource = "args"
	TargetFromInput   TargetSource = "input"
	TargetFromPattern TargetSource = "pattern"
)

// RawTarget is a target as entered by the user, before normalization.
type RawTarget struct {
	Value  string
	Source TargetSource
}

// ResolvedTarget is a normalized, deduplicated target ready for planning.
type ResolvedTarget struct {
	Name   string
	Source TargetSource
}

// PlanItem is a single planned batch operation.
type PlanItem struct {
	Op      OperationType
	Target  ResolvedTarget
	Skipped bool
	Reason  string
}

// ItemResultStatus is the per-item execution outcome.
type ItemResultStatus string

const (
	ResultSuccess ItemResultStatus = "success"
	ResultFailed  ItemResultStatus = "failed"
	ResultSkipped ItemResultStatus = "skipped"
)

// ItemResult describes the outcome of one target.
type ItemResult struct {
	Op      OperationType    `json:"op"`
	Target  string           `json:"target"`
	Status  ItemResultStatus `json:"status"`
	Message string           `json:"message"`
}

// Summary aggregates all item outcomes.
type Summary struct {
	Total   int `json:"total"`
	Success int `json:"success"`
	Failed  int `json:"failed"`
	Skipped int `json:"skipped"`
}

// Report contains per-item outcomes and an aggregate summary.
type Report struct {
	Summary Summary      `json:"summary"`
	Results []ItemResult `json:"results"`
}

// Plan is a sequence of planned operations.
type Plan struct {
	Items []PlanItem
}
