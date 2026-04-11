package batch

// ExecuteOptions controls execution semantics.
//
// Deprecated: transitional v1.1 type for ExecutePlan.
type ExecuteOptions struct {
	DryRun   bool
	FailFast bool
}

// ExecutePrepared runs prepared targets in input order.
func ExecutePrepared(op OperationType, dryRun bool, failFast bool, targets []PreparedTarget, execFunc func(id int64) ItemResult) Report {
	results := make([]ItemResult, 0, len(targets))

	for _, target := range targets {
		if !target.Executable {
			results = append(results, target.Result)
			continue
		}

		item := execFunc(target.ID)
		results = append(results, item)
		if failFast && item.Status == ResultFailed {
			break
		}
	}

	report := NewReport(op, dryRun, results)
	report.ExecutionMode = ExecutionModeContinueOnError
	if failFast {
		report.ExecutionMode = ExecutionModeFailFast
	}
	return report
}

// ExecutePlan runs a plan using a per-item execution callback.
//
// Deprecated: transitional v1.1 helper from the legacy plan-based path.
// Prefer ExecutePrepared for new code.
func ExecutePlan(plan Plan, opts ExecuteOptions, execFunc func(id int64) (string, error)) Report {
	results := make([]ItemResult, 0, len(plan.Items))

	for _, item := range plan.Items {
		if item.Skipped {
			results = append(results, ItemResult{
				ID:      item.Target.ID,
				Status:  ResultSkipped,
				Message: item.Reason,
			})
			continue
		}

		if opts.DryRun {
			results = append(results, ItemResult{
				ID:      item.Target.ID,
				Status:  ResultPlanned,
				Message: "planned",
			})
			continue
		}

		message, err := execFunc(item.Target.ID)
		if err != nil {
			results = append(results, ItemResult{
				ID:      item.Target.ID,
				Status:  ResultFailed,
				Message: err.Error(),
			})
			if opts.FailFast {
				break
			}
			continue
		}

		if message == "" {
			message = "completed"
		}
		results = append(results, ItemResult{
			ID:      item.Target.ID,
			Status:  ResultSuccess,
			Message: message,
		})
	}

	report := NewReport(planOperation(plan), opts.DryRun, results)
	report.ExecutionMode = ExecutionModeContinueOnError
	if opts.FailFast {
		report.ExecutionMode = ExecutionModeFailFast
	}
	return report
}

func planOperation(plan Plan) OperationType {
	if len(plan.Items) == 0 {
		return ""
	}
	return plan.Items[0].Op
}
