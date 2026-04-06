package batch

// ExecuteOptions controls execution semantics.
type ExecuteOptions struct {
	DryRun   bool
	FailFast bool
}

// Executor runs plan items and returns a report.
type Executor interface {
	Execute(plan Plan, opts ExecuteOptions) Report
}

// ExecuteItemFunc runs a single plan item and returns a human message.
type ExecuteItemFunc func(item PlanItem) (string, error)

// RealExecutor performs real mutations for each plan item.
type RealExecutor struct {
	Run ExecuteItemFunc
}

// Execute runs the plan with best-effort semantics unless fail-fast is requested.
func (e RealExecutor) Execute(plan Plan, opts ExecuteOptions) Report {
	results := make([]ItemResult, 0, len(plan.Items))

	for _, item := range plan.Items {
		if item.Skipped {
			results = append(results, ItemResult{
				Op:      item.Op,
				Target:  item.Target.Name,
				Status:  ResultSkipped,
				Message: item.Reason,
			})
			continue
		}

		message, err := e.Run(item)
		if err != nil {
			results = append(results, ItemResult{
				Op:      item.Op,
				Target:  item.Target.Name,
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
			Op:      item.Op,
			Target:  item.Target.Name,
			Status:  ResultSuccess,
			Message: message,
		})
	}

	return NewReport(results)
}

// DryRunExecutor reports what would run without mutation.
type DryRunExecutor struct{}

// Execute produces a success result for each non-skipped planned item.
func (e DryRunExecutor) Execute(plan Plan, opts ExecuteOptions) Report {
	results := make([]ItemResult, 0, len(plan.Items))
	for _, item := range plan.Items {
		if item.Skipped {
			results = append(results, ItemResult{
				Op:      item.Op,
				Target:  item.Target.Name,
				Status:  ResultSkipped,
				Message: item.Reason,
			})
			continue
		}

		results = append(results, ItemResult{
			Op:      item.Op,
			Target:  item.Target.Name,
			Status:  ResultSuccess,
			Message: "planned (dry-run)",
		})
	}
	return NewReport(results)
}
