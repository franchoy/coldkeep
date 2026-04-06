package batch

// ExecuteOptions controls execution semantics.
type ExecuteOptions struct {
	DryRun   bool
	FailFast bool
}

// ExecutePlan runs a plan using a per-item execution callback.
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

	return NewReport(planOperation(plan), opts.DryRun, results)
}

func planOperation(plan Plan) OperationType {
	if len(plan.Items) == 0 {
		return ""
	}
	return plan.Items[0].Op
}
