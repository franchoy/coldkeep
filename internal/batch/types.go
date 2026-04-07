package batch

// OperationType identifies the command-level operation executed in a batch.
type OperationType string

const (
	OperationRestore OperationType = "restore"
	OperationRemove  OperationType = "remove"
)

// RawTarget is a target as entered by the user, before normalization.
type RawTarget struct {
	Value  string
	Source string
}

// ResolvedTarget is a validated target ready for planning.
//
// Deprecated: transitional v1.1 type kept for legacy plan-based helpers
// (ResolveTargets/DeduplicateTargets/BuildPlan/ExecutePlan). New CLI paths use
// RawTarget -> PrepareTargets -> ExecutePrepared.
type ResolvedTarget struct {
	ID int64
}

// PlanItem is a single planned batch operation.
//
// Deprecated: transitional v1.1 type used only by legacy plan-based helpers.
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
	ResultPlanned ItemResultStatus = "planned"
)

// ItemResult describes the outcome of one target.
type ItemResult struct {
	ID           int64            `json:"id"`
	RawValue     string           `json:"raw_value,omitempty"`
	Status       ItemResultStatus `json:"status"`
	Message      string           `json:"message,omitempty"`
	OutputPath   string           `json:"output_path,omitempty"`
	OriginalName string           `json:"original_name,omitempty"`
}

// Summary aggregates all item outcomes.
type Summary struct {
	Total   int `json:"total"`
	Planned int `json:"planned"`
	Success int `json:"success"`
	Failed  int `json:"failed"`
	Skipped int `json:"skipped"`
}

// Report contains per-item outcomes and an aggregate summary.
type Report struct {
	Operation OperationType `json:"operation"`
	DryRun    bool          `json:"dry_run"`
	Summary   Summary       `json:"summary"`
	Results   []ItemResult  `json:"results"`
}

// Plan is a sequence of planned operations.
//
// Deprecated: transitional v1.1 type used only by legacy plan-based helpers.
type Plan struct {
	Items []PlanItem
}
