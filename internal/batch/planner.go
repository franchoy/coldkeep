package batch

// BuildPlan creates a deterministic execution plan from resolved targets.
//
// Deprecated: transitional v1.1 helper from the legacy plan-based path.
// Prefer PrepareTargets + ExecutePrepared for new code.
func BuildPlan(op OperationType, targets []ResolvedTarget) Plan {
	items := make([]PlanItem, 0, len(targets))
	for _, target := range targets {
		items = append(items, PlanItem{
			Op:     op,
			Target: target,
		})
	}
	return Plan{Items: items}
}
